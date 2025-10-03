use async_trait::async_trait;
use sqlx::{Pool, Postgres};
use tokio::sync::RwLock;
use crate::error::EsError;
use crate::store::transaction::Transaction;

pub struct PgTransaction {
    pub(crate) inner: RwLock<sqlx::Transaction<'static, Postgres>>,
}

impl PgTransaction {
    pub async fn new(pool: &Pool<Postgres>) -> Result<Self, EsError> {
        Ok(Self {
            inner: RwLock::new(pool.begin().await?),
        })
    }

    pub fn inner_mut(&mut self) -> &mut sqlx::Transaction<'static, Postgres> {
        self.inner.get_mut()
    }
}

#[async_trait]
impl Transaction for PgTransaction
{
    type Error = EsError;

    async fn commit(self) -> Result<(), EsError> {
        self.inner.into_inner().commit()
            .await
            .map_err(EsError::from)?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), EsError> {
        self.inner.into_inner().rollback()
            .await
            .map_err(EsError::from)?;
        Ok(())
    }
}