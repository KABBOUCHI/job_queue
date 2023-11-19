use crate::{DBType, Error};

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
                `uuid` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                `queue` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
                `attempts` int unsigned NOT NULL,
                `reserved_at` int unsigned DEFAULT NULL,
                `available_at` int unsigned NOT NULL,
                `created_at` int unsigned NOT NULL,
                PRIMARY KEY (`id`),
                KEY `jobs_queue_index` (`queue`)
              ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
        )
        .execute(&pool)
        .await
        .map_err(Error::DatabaseError)?;

        sqlx::query(
            r" CREATE TABLE IF NOT EXISTS `failed_jobs` (
            `id` bigint unsigned NOT NULL AUTO_INCREMENT,
            `uuid` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
            `queue` text COLLATE utf8mb4_unicode_ci NOT NULL,
            `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
            `exception` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
            `failed_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `failed_jobs_uuid_unique` (`uuid`)
          ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
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
                    uuid char(36) NOT NULL,
                    queue char(36) NOT NULL,
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

        sqlx::query(" CREATE SEQUENCE IF NOT EXISTS failed_jobs_id_seq")
            .execute(&mut *transaction)
            .await
            .map_err(Error::DatabaseError)?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS public.failed_jobs (
                    "id" int8 NOT NULL DEFAULT nextval('failed_jobs_id_seq'::regclass),
                    "uuid" char(36) NOT NULL,
                    "queue" char(36) NOT NULL,
                    "payload" text NOT NULL,
                    "exception" text NOT NULL,
                    "failed_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY ("id")
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
