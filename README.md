# job_queue

> [!IMPORTANT]
> This lib is unfinished and heavily work in progress.

## Setup

```bash
cargo add job_queue
```

## Usage

### Create a job

```rust
use job_queue::{Error, Job, typetag, async_trait};
use job_queue::serde::{Deserialize, Serialize};

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

### Create a queue and dispatch a job

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

### Create a worker

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

TODO:

- [ ] emit events, failing, stopping, before and after processing a job
