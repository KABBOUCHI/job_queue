use crate::{get_pool, DBType, Error, Job};
use sqlx::AnyPool;
use uuid::Uuid;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct DispatchOptions {
    pub queue: Option<String>,
    pub delay: Option<Duration>,
}

#[derive(Debug)]
pub struct Client {
    pool: AnyPool,
    db_type: DBType,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub async fn dispatch(&self, job: impl Job) -> Result<(), Error> {
        let queue = job.queue();

        self.dispatch_on_queue(job, &queue).await
    }

    pub async fn dispatch_on_queue(&self, job: impl Job, queue: &str) -> Result<(), Error> {
        let options = DispatchOptions {
            queue: Some(queue.to_string()),
            ..Default::default()
        };

        self.custom_dispatch(job, options).await
    }

    pub async fn custom_dispatch(
        &self,
        job: impl Job,
        options: DispatchOptions,
    ) -> Result<(), Error> {
        let mut conn = self.pool.clone().acquire().await?;
        let payload = serde_json::to_string(&job as &dyn Job).map_err(Error::SerdeError)?;
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unknown)?
            .as_secs();

        let job_id = Uuid::new_v4().to_string();

        sqlx::query(&format!(
            "INSERT INTO jobs (uuid, queue, payload, attempts, available_at, created_at) VALUES {}",
            match self.db_type {
                DBType::Mysql => "(?, ?, ?, ?, ?, ?)",
                DBType::Postgres => "($1, $2, $3, $4, $5, $6)",
            }
        ))
        .bind(job_id)
        .bind(options.queue.unwrap_or_else(|| job.queue()))
        .bind(payload)
        .bind(0)
        .bind(
            (time
                + options
                    .delay
                    .unwrap_or_else(|| Duration::from_secs(0))
                    .as_secs()) as i64,
        )
        .bind(time as i64)
        .execute(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        conn.close().await?;

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ClientBuilder {
    max_connections: u32,
    min_connections: u32,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            max_connections: 10,
            min_connections: 0,
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

    pub async fn connect(self, database_url: &str) -> Result<Client, Error> {
        let (pool, db_type) = get_pool(
            database_url,
            crate::PoolOptions {
                max_connections: self.max_connections,
                min_connections: self.min_connections,
            },
        )
        .await?;

        let client = Client { db_type, pool };

        Ok(client)
    }
}
