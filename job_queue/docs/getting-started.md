# Introduction

A simple, efficient Rust library for handling asynchronous job processing and task queuing.

## Create a job

```rust
use job_queue::{Error, Job, typetag, async_trait, serde};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(crate = "job_queue::serde")]
pub struct HelloJob {
    pub message: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Job for HelloJob {
    async fn handle(&self) -> Result<(), Error> {
        println!("{}", self.message);
        Ok(())
    }
}
```

## Create a client and dispatch a job

```rust

use job_queue::{Error, Job, Queue};

let queue = Client::builder()
    .connect("mysql://root:@localhost/job_queue") // or postgres://root:@localhost/job_queue
    .await?;

queue
    .dispatch(&HelloJob {
        message: "Hello, world!".to_string(),
    })
    .await?;
```

## Create a worker

```rust
use job_queue::{Error, Job, Worker};
use std::time::Duration;

let worker = Worker::builder()
        .max_connections(10)
        .worker_count(10)
        .connect("mysql://root:@localhost/job_queue") // or postgres://root:@localhost/job_queue
        .await?;

worker.start().await?; // blocks forever, or until all workers are stopped (crash or ctrl-c)
```