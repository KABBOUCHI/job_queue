use job_queue::{Client, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let queue = Client::builder()
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    queue.retry_all_failed_jobs().await?;

    Ok(())
}
