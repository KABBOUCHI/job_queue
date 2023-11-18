use crate::Error;
use async_trait::async_trait;

const COMMON_QUEUE: &str = "default";
pub const MAX_RETRIES: i16 = 1;

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait Job: Send + Sync {
    async fn handle(&self) -> Result<(), Error>;

    fn queue(&self) -> String {
        COMMON_QUEUE.to_string()
    }

    fn max_retries(&self) -> i16 {
        MAX_RETRIES
    }

    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
