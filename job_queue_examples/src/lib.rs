use job_queue::{async_trait, serde, typetag, Error, Job};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(crate = "job_queue::serde")]
pub struct HelloJob {
    pub message: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Job for HelloJob {
    async fn handle(&self) -> Result<(), Error> {
        println!("{}", self.message);

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Ok(())
    }
}
