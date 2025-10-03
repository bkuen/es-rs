use std::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait Transaction {
    type Error: Error;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}