use async_trait::async_trait;

#[async_trait]
pub trait Transaction {
    type Error;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}