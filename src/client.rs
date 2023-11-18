use crate::{Error, Job};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Client {
    pool: AnyPool,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub async fn dispatch(&self, job: impl Job) -> Result<(), Error> {
        let mut conn = self.pool.clone().acquire().await?;
        let payload = serde_json::to_string(&job as &dyn Job).map_err(Error::SerdeError)?;
        let queue = job.queue();
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unknown)?
            .as_secs();

        sqlx::query(
            "INSERT INTO jobs (queue, payload, attempts, available_at, created_at) VALUES (?,?,?,?,?)",
        )
        .bind(queue)
        .bind(payload)
        .bind(0)
        .bind(time as i64)
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
        sqlx::any::install_default_drivers();

        let pool = AnyPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .connect(database_url)
            .await
            .map_err(Error::DatabaseError)?;

        sqlx::query(include_str!("./migrations/mysql.sql"))
            .execute(&pool)
            .await
            .map_err(Error::DatabaseError)?;

        Ok(Client { pool })
    }
}
