use job_queue::{Client, Error, Job};

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
        .connect("mysql://root:@localhost/job_queue")
        .await?;

    // loop {
        println!("Dispatching job...");
        queue
            .dispatch(HelloJob {
                message: "Hello, world!".to_string(),
            })
            .await?;

        // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    // }

    Ok(())
}
