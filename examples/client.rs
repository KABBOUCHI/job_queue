use job_queue::{Client, DispatchOptions, Error, Job};
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
                    delay: Some(Duration::from_secs(5)),
                },
            )
            .await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }

    // Ok(())
}
