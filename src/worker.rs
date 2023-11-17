use crate::{Error, Job};
use sqlx::any::AnyTypeInfo;
use sqlx::decode::Decode;
use sqlx::postgres::any::{AnyTypeInfoKind, AnyValueKind};
use sqlx::{any::AnyPoolOptions, database::HasValueRef, error::BoxDynError, Any, AnyPool};
use sqlx::{Type, ValueRef};

#[derive(Debug)]
struct JsonValue(serde_json::Value);

#[derive(Debug, sqlx::FromRow)]
struct Task {
    id: String,
    payload: JsonValue,
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
}

impl Worker {
    pub fn new(pool: AnyPool) -> Worker {
        Worker { pool }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let mut conn = self.pool.clone().acquire().await?;

        let task = sqlx::query_as::<_, Task>(
            r#"
            SELECT id, payload, status
            FROM jobs
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1 FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(Error::DatabaseError)?;

        let task = match task {
            Some(task) => task,
            None => {
                return {
                    conn.close().await?;
                    Ok(())
                }
            }
        };

        let job: Box<dyn Job> =
            serde_json::from_value(task.payload.0).map_err(Error::SerdeError)?;

        let result = job.handle().await;

        match result {
            Ok(_) => {
                sqlx::query(
                    r#"
                    UPDATE jobs
                    SET status = 'completed'
                    WHERE id = ?
                    "#,
                )
                .bind(task.id)
                .execute(&mut *conn)
                .await
                .map_err(Error::DatabaseError)?;
            }
            Err(_e) => {
                sqlx::query(
                    r#"
                    UPDATE jobs
                    SET status = 'failed'
                    WHERE id = ?
                    "#,
                )
                .bind(task.id)
                .execute(&mut *conn)
                .await
                .map_err(Error::DatabaseError)?;
            }
        }

        conn.close().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct WorkerOptions {
    pub max_connection: u32,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        WorkerOptions { max_connection: 5 }
    }
}

pub async fn create_worker(database_url: &str, options: WorkerOptions) -> Result<Worker, Error> {
    sqlx::any::install_default_drivers();

    let pool = AnyPoolOptions::new()
        .max_connections(options.max_connection)
        .connect(database_url)
        .await
        .map_err(Error::DatabaseError)?;

    let mut conn = pool.acquire().await?;

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
    .execute(&mut *conn)
    .await
    .map_err(Error::DatabaseError)?;

    conn.close().await?;

    let worker = Worker::new(pool.clone());

    Ok(worker)
}
