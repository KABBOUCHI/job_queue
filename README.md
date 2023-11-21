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
use job_queue::{Error, Job};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
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
    .dispatch(HelloJob {
        message: "Hello, world!".to_string(),
    })
    .await?;
```

### Create a worker

```rust
let worker = Worker::builder()
        .max_connections(10)
        .worker_count(10)
        .connect("mysql://root:@localhost/job_queue") // or postgres://root:@localhost/job_queue
        .await?;

worker.start().await?;

loop {
    tokio::time::sleep(Duration::from_millis(100)).await;
}
```

TODO:
- [ ] emit events, failing, stopping, before and after processing a job