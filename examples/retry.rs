use job_queue::{Client, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let queue = Client::builder()
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    queue
        .retry_job_id("5f54f970-87ba-4aea-a0e1-1d0f6a80e17a")
        .await?;

    Ok(())
}
