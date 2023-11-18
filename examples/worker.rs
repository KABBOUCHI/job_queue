use job_queue::{Error, Job, Worker};
use std::time::Duration;

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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let worker_count = 10;

    let worker = Worker::builder()
        .max_connections(worker_count * 2)
        .connect("mysql://root:@localhost/job_queue")
        .await?;

    for _ in 0..worker_count {
        let worker = worker.clone();

        tokio::spawn(async move {
            loop {
                worker.run().await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
