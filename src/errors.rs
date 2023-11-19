use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error")]
    DatabaseError(#[from] sqlx::Error),
    #[error("usupported database url")]
    UnsupportedDatabaseUrl,
    #[error("serializing error")]
    SerdeError(#[from] serde_json::Error),
    #[error("job error: {0}")]
    Message(String),
    #[error("unknown data store error")]
    Unknown,
}
