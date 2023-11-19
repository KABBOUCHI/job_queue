mod client;
mod errors;
mod job;
mod worker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DBType {
    Postgres,
    Mysql,
}

pub use client::{Client, ClientBuilder, DispatchOptions};
pub use errors::Error;
pub use job::Job;
pub use worker::{Worker, WorkerBuilder};

pub(crate) struct PoolOptions {
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
}

pub(crate) async fn get_pool(
    database_url: &str,
    options: PoolOptions,
) -> Result<(sqlx::Pool<sqlx::Any>, DBType), Error> {
    sqlx::any::install_default_drivers();

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(options.max_connections)
        .min_connections(options.min_connections)
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

    Ok((pool, db_type))
}
