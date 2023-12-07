extern crate job_queue_examples; // workaround to avoid "unused import" warning, needed to include the jobs in the binary.

use job_queue::{Error, Worker};

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let worker_count = 10;

    let worker = Worker::builder()
        .max_connections(worker_count * 2)
        .worker_count(worker_count)
        .on_stopping(|| async move {
            println!("Worker is stopping");

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        })
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    worker.start().await?;

    Ok(())
}
