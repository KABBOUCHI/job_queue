use crate::Error;
use async_trait::async_trait;

const COMMON_QUEUE: &str = "default";
const TRIES: i16 = 1;
const TIMEOUT: i16 = 300;

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait Job: Send + Sync {
    async fn handle(&self) -> Result<(), Error>;

    /// The name of the queue the job should be dispatched to.
    fn queue(&self) -> String {
        COMMON_QUEUE.to_string()
    }

    /// The number of times the job may be attempted.
    fn tries(&self) -> i16 {
        TRIES
    }

    /// The number of seconds the job can run before timing out.
    fn timeout(&self) -> i16 {
        TIMEOUT
    }

    /// Calculate the number of seconds to wait before retrying the job.
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }

    /// Handle a job failure.
    async fn failed(&self, _err: Error) -> Result<(), Error> {
        Ok(())
    }
}
