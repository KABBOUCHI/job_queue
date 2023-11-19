use crate::{DBType, Error, Job};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use std::time::{SystemTime, UNIX_EPOCH};

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
        let mut conn = self.pool.clone().acquire().await?;
        let payload = serde_json::to_string(&job as &dyn Job).map_err(Error::SerdeError)?;
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unknown)?
            .as_secs();

        sqlx::query(&format!(
            "INSERT INTO jobs (queue, payload, attempts, available_at, created_at) VALUES {}",
            match self.db_type {
                DBType::Mysql => "(?, ?, ?, ?, ?)",
                DBType::Postgres => "($1, $2, $3, $4, $5)",
            }
        ))
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

        let db_type = if database_url.starts_with("mysql") {
            DBType::Mysql
        } else if database_url.starts_with("postgres") {
            DBType::Postgres
        } else {
            return Err(Error::UnsupportedDatabaseUrl);
        };

        if database_url.starts_with("mysql") {
            sqlx::query(
                r" CREATE TABLE IF NOT EXISTS `jobs` (
                `id` bigint unsigned NOT NULL AUTO_INCREMENT,
                `queue` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
                `attempts` int unsigned NOT NULL,
                `reserved_at` int unsigned DEFAULT NULL,
                `available_at` int unsigned NOT NULL,
                `created_at` int unsigned NOT NULL,
                PRIMARY KEY (`id`),
                KEY `jobs_queue_index` (`queue`)
              ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            )
            .execute(&pool)
            .await
            .map_err(Error::DatabaseError)?;
        } else if database_url.starts_with("postgres") {
            let mut transaction = pool.begin().await?;

            sqlx::query("CREATE SEQUENCE IF NOT EXISTS jobs_id_seq")
                .execute(&mut *transaction)
                .await
                .map_err(Error::DatabaseError)?;
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS public.jobs (
                    id int8 NOT NULL DEFAULT nextval('jobs_id_seq'::regclass),
                    queue varchar NOT NULL,
                    payload text NOT NULL,
                    attempts int2 NOT NULL,
                    reserved_at int4,
                    available_at int4 NOT NULL,
                    created_at int4 NOT NULL,
                    PRIMARY KEY (id)
                )
                "#,
            )
            .execute(&mut *transaction)
            .await
            .map_err(Error::DatabaseError)?;

            transaction.commit().await.map_err(Error::DatabaseError)?;
        } else {
            return Err(Error::UnsupportedDatabaseUrl);
        }

        Ok(Client { pool, db_type })
    }
}
