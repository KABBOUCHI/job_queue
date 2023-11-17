use crate::Error;
use async_trait::async_trait;
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use uuid::Uuid;

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait Job: Send + Sync {
    async fn handle(&self) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct JobQueue {
    pool: AnyPool,
}

impl JobQueue {
    pub fn new(pool: AnyPool) -> JobQueue {
        JobQueue { pool }
    }

    pub async fn dispatch(&self, job: impl Job) -> Result<(), Error> {
        let value = serde_json::to_string(&job as &dyn Job).map_err(Error::SerdeError)?;
        let param = "?"; // TODO: handle pgsql

        sqlx::query(&format!("INSERT INTO jobs (id, payload) VALUES ({param}, {param})"))
            .bind(Uuid::new_v4().to_string())
            .bind(value)
            .execute(&self.pool)
            .await
            .map_err(Error::DatabaseError)?;

        Ok(())
    }
}

pub async fn create_queue(database_url: &str) -> Result<JobQueue, Error> {
    sqlx::any::install_default_drivers();

    let pool = AnyPoolOptions::new()
        .connect(database_url)
        .await
        .map_err(Error::DatabaseError)?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id varchar(255) PRIMARY KEY,
            payload TEXT NOT NULL,
            status varchar(255) NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(&pool)
    .await
    .map_err(Error::DatabaseError)?;

    Ok(JobQueue::new(pool))
}
