use crate::{get_pool, models::Task, DBType, Error, Job};
use log::{error, info, warn};
use sqlx::{Any, AnyPool, Connection};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::timeout;

type OnStoppingFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync>;

#[derive(Clone)]
pub struct Worker {
    db_type: DBType,
    pool: AnyPool,
    queue: String,
    retry_after: i64,
    worker_count: u32,
    on_stopping: Option<OnStoppingFn>,
}

impl Worker {
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    async fn run(&self) -> Result<(), Error> {
        let mut pool = self.pool.acquire().await?;
        let mut conn = pool.begin().await?;
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unknown)?
            .as_secs() as i64;

        let task = sqlx::query_as::<Any, Task>(&format!(
            r#"
            SELECT
                id,
                uuid,
                payload,
                attempts
            FROM
                jobs
            WHERE
                queue = {}
                AND ((reserved_at IS NULL
                    AND available_at <= {})
                    OR (reserved_at <= {}))
            ORDER BY
                id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED"#,
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$1"
            },
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$2"
            },
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$3"
            },
        ))
        .bind(&self.queue)
        .bind(unix_timestamp)
        .bind(unix_timestamp - self.retry_after)
        .fetch_optional(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        let task = match task {
            Some(task) => task,
            None => {
                return {
                    drop(conn);
                    // conn.commit().await?;
                    pool.close().await?;
                    Ok(())
                };
            }
        };

        sqlx::query(&format!(
            r#"
            UPDATE jobs
            SET reserved_at = {},  attempts = {}
            WHERE id = {}
            "#,
            unix_timestamp,
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$1"
            },
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$2"
            },
        ))
        .bind(task.attempts + 1)
        .bind(task.id)
        .execute(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        let job: Box<dyn Job> =
            serde_json::from_value(task.payload.0).map_err(Error::SerdeError)?;

        info!("Job {}#{} started", job.typetag_name(), task.id);

        let result = std::panic::catch_unwind(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    timeout(Duration::from_secs(job.timeout() as u64), job.handle())
                        .await
                        .map_err(|_| Error::JobTimeout)
                })
            })
        });

        let result = match result {
            Ok(result) => result,
            Err(_) => Err(Error::JobPanic),
        };

        sqlx::query(&format!(
            "DELETE FROM jobs WHERE id = {}",
            if self.db_type == DBType::Mysql {
                "?"
            } else {
                "$1"
            }
        ))
        .bind(task.id)
        .execute(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        match result {
            Ok(Ok(_)) => {
                info!("Job {}#{} finished", job.typetag_name(), task.id);
            }
            Ok(Err(err)) | Err(err) => {
                let error_message = err.to_string();
                let _ = job.failed(err).await;

                let tries = job.tries();
                let attempts = task.attempts;

                if (attempts + 1) < tries {
                    let time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| Error::Unknown)?
                        .as_secs();

                    let backoff = job.backoff((attempts + 1) as u32) as i64;

                    error!(
                        "Job {}#{} failed, will be retried in {} seconds",
                        job.typetag_name(),
                        task.id,
                        backoff
                    );

                    sqlx::query(&format!(
                        r#"
                        INSERT INTO jobs (uuid, queue, payload, attempts, available_at, created_at)
                        VALUES ({}, {},{},{},{},{})
                        "#,
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$1"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$2"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$3"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$4"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$5"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$6"
                        },
                    ))
                    .bind(&task.uuid)
                    .bind(&self.queue)
                    .bind(serde_json::to_string(&job).map_err(Error::SerdeError)?)
                    .bind(attempts + 1)
                    .bind(time as i64 + backoff)
                    .execute(&mut *conn)
                    .await
                    .map_err(Error::DatabaseError)?;
                } else {
                    sqlx::query(&format!(
                        r#"
                        INSERT INTO failed_jobs (uuid, queue, payload, exception)
                        VALUES ({},{},{},{})
                        "#,
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$1"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$2"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$3"
                        },
                        if self.db_type == DBType::Mysql {
                            "?"
                        } else {
                            "$4"
                        },
                    ))
                    .bind(&task.uuid)
                    .bind(&self.queue)
                    .bind(serde_json::to_string(&job).map_err(Error::SerdeError)?)
                    .bind(error_message)
                    .execute(&mut *conn)
                    .await
                    .map_err(Error::DatabaseError)?;

                    error!("Job {}#{} failed", job.typetag_name(), task.id);
                }
            }
        }

        conn.commit().await?;
        pool.close().await?;

        Ok(())
    }

    pub async fn start(&self) -> Result<(), Error> {
        info!("Processing jobs from the [{}] queue.", self.queue);

        let mut handles = vec![];

        let token = tokio_util::sync::CancellationToken::new();

        for _ in 0..self.worker_count {
            let worker = self.clone();
            let cloned_token = token.clone();

            let handle = tokio::spawn(async move {
                let mut running = true;

                while running {
                    worker.run().await.unwrap();

                    if cloned_token.is_cancelled() {
                        running = false;
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });

            handles.push(handle);
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, shutting down");

                token.cancel();
            }
            _ = block_on_handles(&handles) => {}
        }

        while !handles.iter().all(|handle| handle.is_finished()) {
            tokio::time::sleep(Duration::from_millis(300)).await;

            info!("Waiting for workers to finish");
        }

        if token.is_cancelled() {
            info!("All workers finished after Ctrl-C");
        } else {
            warn!("All workers finished, probably a crash");
        }

        if let Some(callback) = &self.on_stopping {
            info!("Running on_stopping callback");

            let fut = callback();

            let _ = fut.await;
        }

        Ok(())
    }
}

async fn block_on_handles(handles: &[tokio::task::JoinHandle<()>]) {
    loop {
        if handles.iter().all(|handle| handle.is_finished()) {
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Default, Clone)]
pub struct WorkerBuilder {
    pub max_connections: u32,
    pub min_connections: u32,
    pub worker_count: u32,
    pub retry_after: i64,
    pub queue: String,
    pub on_stopping: Option<OnStoppingFn>,
}

impl WorkerBuilder {
    pub fn new() -> Self {
        Self {
            queue: "default".to_string(),
            max_connections: 10,
            min_connections: 0,
            retry_after: 300,
            worker_count: 1,
            on_stopping: None,
        }
    }

    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = min_connections;
        self
    }

    pub fn worker_count(mut self, worker_count: u32) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn retry_after(mut self, retry_after: i64) -> Self {
        self.retry_after = retry_after;
        self
    }

    pub fn queue(mut self, queue: &str) -> Self {
        self.queue = queue.to_string();
        self
    }

    pub fn on_stopping<F, Fut>(mut self, callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    {
        let callback_arc = Arc::new(move || -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
            Box::pin(callback())
        });
        self.on_stopping = Some(callback_arc);
        self
    }

    pub async fn connect(self, database_url: &str) -> Result<Worker, Error> {
        let (pool, db_type) = get_pool(
            database_url,
            crate::PoolOptions {
                max_connections: self.max_connections,
                min_connections: self.min_connections,
            },
        )
        .await?;

        let worker = Worker {
            db_type,
            pool,
            queue: self.queue,
            retry_after: self.retry_after,
            worker_count: self.worker_count,
            on_stopping: self.on_stopping,
        };

        Ok(worker)
    }
}
