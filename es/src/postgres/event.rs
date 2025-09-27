use std::ops::DerefMut;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::debug;
use num_traits::cast::FromPrimitive;
use sqlx::{FromRow, Pool, Postgres, types};
use uuid::Uuid;
use crate::error::EsError;
use crate::postgres::transaction::PgTransaction;
use crate::store::{AggregateEvent, AggregateStore, EventSourcedAggregate};
use crate::store::aggregate::{Aggregate, AggregateVersion};
use crate::store::event::{DomainEvent, EventApplier};
use crate::store::snapshot::{Snapshot, SnapshotApplier};
use crate::store::transaction::Transaction;

#[derive(Clone)]
pub struct EventStore {
    snapshot_threshold: usize,
    events_table_name: String,
    snapshots_table_name: String,
    pool: Pool<Postgres>,
}

impl EventStore {
    pub fn new(
        events_table_name: &'static str,
        snapshots_table_name: &'static str,
        snapshot_threshold: usize,
        pool: &Pool<Postgres>
    ) -> Self {
        Self {
            events_table_name: events_table_name.to_string(),
            snapshots_table_name: snapshots_table_name.to_string(),
            snapshot_threshold,
            pool: pool.clone(),
        }
    }

    pub async fn save_and_commit<'a, A>(
        &'a self,
        aggregate: &mut EventSourcedAggregate<A>,
    ) -> Result<Vec<AggregateEvent<A::Event>>, EsError>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + for<'b> From<&'b A> + 'static
    {
        let transaction = PgTransaction::new(&self.pool).await?;
        let events = self.save(aggregate, Some(&transaction)).await?;
        transaction.commit().await?;

        Ok(events)
    }

    async fn load_snapshot<A>(&self, aggregate: &mut EventSourcedAggregate<A>) -> Result<Option<PgSnapshot<A::Snapshot>>, EsError>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        let result = sqlx::query_as::<_, PgSnapshot<A::Snapshot>>(
            format!("SELECT stream_version, snapshot_name, snapshot_data FROM {} WHERE stream_id = $1 AND stream_name = $2", &self.events_table_name).as_str()
        )
            .bind(aggregate.aggregate_id())
            .bind(aggregate.aggregate_name().to_string())
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(snapshot) => {
                Ok(Some(snapshot))
            }
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(e) => Err(e.into())
        }
    }

    /// Save a snapshot for an aggregate
    async fn save_snapshot<A>(&self, aggregate: &EventSourcedAggregate<A>) -> Result<(), EsError>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + for<'a> From<&'a A> + 'static,
    {
        let aggregate_version = i64::from_u64(aggregate.aggregate_version())
            .ok_or(EsError::InvalidVersion)?;

        let snapshot_data = A::Snapshot::from(aggregate);
        let snapshot_name = snapshot_data.snapshot_name();

        sqlx::query(format!("INSERT INTO {} (stream_id, stream_name, stream_version, snapshot_name, snapshot_data) VALUES ($1, $2, $3, $4, $5)", &self.snapshots_table_name).as_str())
            .bind(aggregate.aggregate_id())
            .bind(aggregate.aggregate_name().to_string())
            .bind(aggregate_version)
            .bind(snapshot_name)
            .bind(types::Json(snapshot_data))
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl AggregateStore for EventStore {
    type Error = EsError;
    type Transaction = PgTransaction;

    async fn load<A>(&self, id: Uuid) -> Result<EventSourcedAggregate<A>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        let mut aggregate: EventSourcedAggregate<A> = self.load_empty(id).await?;

        // Try to load the latest snapshot
        if let Ok(Some(snapshot)) = self.load_snapshot::<A>(&mut aggregate).await {
            aggregate.apply_snapshot(snapshot.snapshot_data);
            aggregate.set_version(snapshot.stream_version);
        }

        let version = aggregate.aggregate_version();
        self.load_events_after(&mut aggregate, version).await?;

        Ok(aggregate)
    }

    async fn load_events_after<A>(&self, aggregate: &mut EventSourcedAggregate<A>, version: u64) -> Result<(), Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        let aggregate_version = i64::from_u64(version)
            .ok_or(EsError::InvalidVersion)?;

        let rows: Vec<PgAggregateEvent<A::Event>> = sqlx::query_as(format!("SELECT stream_version, event_id, event_name, event_data, occurred_at FROM {} WHERE stream_id = $1 AND stream_name = $2 AND stream_version > $3 ORDER BY stream_version ASC", &self.events_table_name).as_str())
            .bind(aggregate.aggregate_id())
            .bind(aggregate.aggregate_name().to_string())
            .bind(aggregate_version)
            .fetch_all(&self.pool)
            .await
            .map_err(EsError::from)?;

        for aggregate_event in rows {
            aggregate.apply_event(&aggregate_event.event_data);
            aggregate.set_version(aggregate_event.stream_version);
        }

        Ok(())
    }

    async fn save<A>(&self, aggregate: &mut EventSourcedAggregate<A>, transaction: Option<&Self::Transaction>) -> Result<Vec<AggregateEvent<A::Event>>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + for<'a> From<&'a A> + 'static
    {
        if aggregate.pending_version() == aggregate.version() {
            return Ok(Vec::new());
        }

        if let Some(transaction) = transaction {
            debug!("use outer transaction");

            let mut tx = transaction.inner.write().await;
            let conn = tx.deref_mut().deref_mut();

            for event in aggregate.pending_events() {
                let aggregate_version = i64::from_u64(event.aggregate_version())
                    .ok_or(EsError::InvalidVersion)?;

                sqlx::query(format!("INSERT INTO {} (stream_id, stream_name, stream_version, event_id, event_name, event_data, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7)", &self.events_table_name).as_str())
                    .bind(aggregate.aggregate_id())
                    .bind(aggregate.aggregate_name().to_string())
                    .bind(aggregate_version)
                    .bind(event.event_id())
                    .bind(event.event_name())
                    .bind(types::Json(event.payload()))
                    .bind(event.occurred_at())
                    .execute(&mut *conn)
                    .await
                    .map_err(EsError::from)?;
            }

            // We have to check whether we should take a snapshot before commiting the events
            let should_snapshot = aggregate.should_snapshot(self.snapshot_threshold);

            // After saving, we must commit the events to avoid saving them twice
            let events = aggregate.commit_events();

            // Check if we should create a new snapshot
            if should_snapshot {
                self.save_snapshot::<A>(aggregate).await?;
            }

            Ok(events)
        } else {
            self.save_and_commit(aggregate).await
        }
    }
}

#[derive(FromRow)]
struct PgAggregateEvent<T: DomainEvent> {
    #[sqlx(try_from = "i64")]
    stream_version: u64,
    event_id: Uuid,
    event_name: String,
    #[sqlx(json)]
    event_data: T,
    occurred_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct PgSnapshot<S: Snapshot> {
    #[sqlx(try_from = "i64")]
    stream_version: u64,
    snapshot_name: String,
    #[sqlx(json)]
    snapshot_data: S,
}