mod client;
mod errors;
mod job;
mod worker;
mod pool;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DBType {
    Postgres,
    Mysql,
}

pub use client::{Client, ClientBuilder, DispatchOptions};
pub use errors::Error;
pub use job::Job;
pub use worker::{Worker, WorkerBuilder};
pub(crate) use pool::{PoolOptions, get_pool};

#[doc(hidden)]
pub extern crate serde;