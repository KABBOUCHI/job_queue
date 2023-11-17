use std::time::Duration;

use job_queue::{create_worker, Error, Job, WorkerOptions};

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

    let worker = create_worker(
        "mysql://root:@localhost/job_queue",
        WorkerOptions {
            max_connection: worker_count * 2,
        },
    )
    .await?;

    for _ in 0..worker_count {
        let worker = worker.clone();

        tokio::spawn(async move {
            loop {
                worker.run().await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }

    loop {
        tokio::time::sleep(Duration::from_millis(10)).await; 
    }
}
