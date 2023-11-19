use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Error, Job};
use log::{error, info};
use sqlx::any::AnyTypeInfo;
use sqlx::decode::Decode;
use sqlx::postgres::any::{AnyTypeInfoKind, AnyValueKind};
use sqlx::{any::AnyPoolOptions, database::HasValueRef, error::BoxDynError, Any, AnyPool};
use sqlx::{Connection, Type, ValueRef};

#[derive(Debug)]
struct JsonValue(serde_json::Value);

#[derive(Debug, sqlx::FromRow)]
struct Task {
    id: i64,
    payload: JsonValue,
    attempts: i16,
    // available_at: i64,
    // created_at: i64,
}

impl Type<Any> for JsonValue {
    fn type_info() -> AnyTypeInfo {
        AnyTypeInfo {
            kind: AnyTypeInfoKind::Blob,
        }
    }
}

impl<'r> Decode<'r, Any> for JsonValue {
    fn decode(value: <Any as HasValueRef<'r>>::ValueRef) -> Result<Self, BoxDynError> {
        let v = ValueRef::to_owned(&value);

        match v.kind {
            AnyValueKind::Blob(s) => Ok(JsonValue(serde_json::from_slice(&s)?)),
            _ => Err("invalid type".into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Worker {
    pool: AnyPool,
    queue: String,
    retry_after: i64,
    worker_count: u32,
}

impl Worker {
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    async fn run(&self) -> Result<(), Error> {
        let mut pool = self.pool.acquire().await?;
        let mut conn = pool.begin().await?;

        let task = sqlx::query_as::<_, Task>(
            r#"
            SELECT
                *
            FROM
                jobs
            WHERE
                queue = ?
                AND ((reserved_at IS NULL
                    AND available_at <= UNIX_TIMESTAMP())
                    OR (reserved_at <= UNIX_TIMESTAMP() - ?))
            ORDER BY
                id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED"#,
        )
        .bind(&self.queue)
        .bind(self.retry_after)
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

        sqlx::query(
            r#"
            UPDATE jobs
            SET reserved_at = UNIX_TIMESTAMP(),  attempts = ?
            WHERE id = ?
            "#,
        )
        .bind(task.attempts + 1)
        .bind(task.id)
        .execute(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        let job: Box<dyn Job> =
            serde_json::from_value(task.payload.0).map_err(Error::SerdeError)?;

        info!("Job {}#{} started", job.typetag_name(), task.id);

        let result = job.handle().await;

        sqlx::query(
            r#"
            DELETE FROM jobs
            WHERE id = ?
            "#,
        )
        .bind(task.id)
        .execute(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        match result {
            Ok(_) => {
                info!("Job {}#{} finished", job.typetag_name(), task.id);
            }
            Err(_e) => {
                let tries = job.max_retries();
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

                    sqlx::query(
                        r#"
                        INSERT INTO jobs (queue, payload, attempts, available_at, created_at)
                        VALUES (?,?,?,?,?)
                        "#,
                    )
                    .bind(&self.queue)
                    .bind(serde_json::to_string(&job).map_err(Error::SerdeError)?)
                    .bind(attempts + 1)
                    .bind(time as i64 + backoff)
                    .bind(time as i64)
                    .execute(&mut *conn)
                    .await
                    .map_err(Error::DatabaseError)?;
                } else {
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

        for _ in 0..self.worker_count {
            let worker = self.clone();

            tokio::spawn(async move {
                loop {
                    worker.run().await.unwrap();

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct WorkerBuilder {
    pub max_connections: u32,
    pub min_connections: u32,
    pub worker_count: u32,
    pub retry_after: i64,
    pub queue: String,
}

impl WorkerBuilder {
    pub fn new() -> Self {
        Self {
            queue: "default".to_string(),
            max_connections: 10,
            min_connections: 0,
            retry_after: 300,
            worker_count: 1,
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

    pub async fn connect(self, database_url: &str) -> Result<Worker, Error> {
        sqlx::any::install_default_drivers();

        let pool = AnyPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .connect(database_url)
            .await
            .map_err(Error::DatabaseError)?;

        let mut conn = pool.acquire().await?;

        sqlx::query(include_str!("./migrations/mysql.sql"))
            .execute(&mut *conn)
            .await
            .map_err(Error::DatabaseError)?;

        conn.close().await?;

        let worker = Worker {
            pool,
            queue: self.queue,
            retry_after: self.retry_after,
            worker_count: self.worker_count,
        };

        Ok(worker)
    }
}
