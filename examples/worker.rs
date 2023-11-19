use std::time::Duration;

use job_queue::{Error, Job, Worker};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct HelloJob {
    pub message: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Job for HelloJob {
    async fn handle(&self) -> Result<(), Error> {
        tokio::time::sleep(Duration::from_secs(10)).await;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let worker_count = 10;

    let worker = Worker::builder()
        .max_connections(worker_count * 2)
        .worker_count(worker_count)
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    worker.start().await?;

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
