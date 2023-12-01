use std::time::Duration;
use job_queue::{Client, DispatchOptions, Error};
use job_queue_examples::HelloJob;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let queue = Client::builder()
        .connect("postgres://kabbouchi:@localhost/job_queue")
        .await?;

    loop {
        println!("Dispatching job...");
        queue
            .custom_dispatch(
                &HelloJob {
                    message: "Hello, world!".to_string(),
                },
                &DispatchOptions {
                    queue: Some("default".to_string()),
                    delay: Some(Duration::from_secs(1)),
                },
            )
            .await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Ok(())
}
