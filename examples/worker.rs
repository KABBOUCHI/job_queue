use job_queue::{Error, Job, Worker};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct HelloJob {
    pub message: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Job for HelloJob {
    async fn handle(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let worker_count = 10;

    let worker = Worker::builder()
        .max_connections(worker_count * 2)
        .worker_count(worker_count)
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    worker.start().await?;

    Ok(())
}
