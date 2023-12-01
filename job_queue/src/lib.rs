#![doc = include_str!("../../README.md")]

mod client;
mod errors;
mod job;
pub(crate) mod models;
mod pool;
mod worker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DBType {
    Postgres,
    Mysql,
}

pub use client::{Client, ClientBuilder, DispatchOptions};
pub use errors::Error;
pub use job::Job;
pub(crate) use pool::{get_pool, PoolOptions};
pub use worker::{Worker, WorkerBuilder};

#[doc(hidden)]
pub extern crate serde;

#[doc(hidden)]
pub extern crate typetag;

#[doc(hidden)]
pub extern crate async_trait;
