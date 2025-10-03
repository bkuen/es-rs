use crate::error::EsError;
use crate::store::transaction::Transaction;
use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use sqlx::{Database, Describe, Either, Error, Execute, Executor, Pool, Postgres};
use std::fmt::Debug;
use std::ops::DerefMut;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct PgTransaction {
    pub(crate) inner: RwLock<sqlx::Transaction<'static, Postgres>>,
}

impl PgTransaction {
    pub async fn new(pool: &Pool<Postgres>) -> Result<Self, EsError> {
        Ok(Self {
            inner: RwLock::new(pool.begin().await?),
        })
    }
}

impl<'c> Executor<'c> for &'c PgTransaction {
    type Database = Postgres;

    fn execute<'e, 'q: 'e, E>(self, query: E) -> futures_core::future::BoxFuture<'e, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.execute(query).await
        })
    }

    fn execute_many<'e, 'q: 'e, E>(self, query: E) -> futures_core::stream::BoxStream<'e, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        let inner = &self.inner;
        Box::pin(stream! {
            let mut guard = inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            let mut stream = tx.execute_many(query);
            while let Some(result) = stream.next().await {
                yield result;
            }
        })
    }

    fn fetch<'e, 'q: 'e, E>(self, query: E) -> futures_core::stream::BoxStream<'e, Result<<Self::Database as Database>::Row, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        let inner = &self.inner;
        Box::pin(stream! {
            let mut guard = inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            let mut stream = tx.fetch(query);
            while let Some(result) = stream.next().await {
                yield result;
            }
        })
    }

    fn fetch_many<'e, 'q: 'e, E>(self, query: E) -> futures_core::stream::BoxStream<'e, Result<Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        let inner = &self.inner;
        Box::pin(stream! {
            let mut guard = inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            let mut stream = tx.fetch_many(query);
            while let Some(result) = stream.next().await {
                yield result;
            }
        })
    }

    fn fetch_all<'e, 'q: 'e, E>(self, query: E) -> futures_core::future::BoxFuture<'e, Result<Vec<<Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.fetch_all(query).await
        })
    }

    fn fetch_one<'e, 'q: 'e, E>(self, query: E) -> futures_core::future::BoxFuture<'e, Result<<Self::Database as Database>::Row, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.fetch_one(query).await
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(self, query: E) -> futures_core::future::BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.fetch_optional(query).await
        })
    }

    fn prepare<'e, 'q: 'e>(self, query: &'q str) -> futures_core::future::BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, Error>>
    where
        'c: 'e
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.prepare(query).await
        })
    }

    fn prepare_with<'e, 'q: 'e>(self, sql: &'q str, parameters: &'e [<Self::Database as Database>::TypeInfo]) -> futures_core::future::BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, Error>>
    where
        'c: 'e
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.prepare_with(sql, parameters).await
        })
    }

    fn describe<'e, 'q: 'e>(self, sql: &'q str) -> futures_core::future::BoxFuture<'e, Result<Describe<Self::Database>, Error>>
    where
        'c: 'e
    {
        Box::pin(async move {
            // Use write() to get a &mut to the transaction
            let mut guard = self.inner.write().await;
            let tx: &mut sqlx::Transaction<'_, Postgres> = guard.deref_mut();
            tx.describe(sql).await
        })
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