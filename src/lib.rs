mod client;
mod errors;
mod job;
mod worker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DBType {
    Postgres,
    Mysql,
}

pub use client::{Client, ClientBuilder};
pub use errors::Error;
pub use job::Job;
pub use worker::{Worker, WorkerBuilder};
