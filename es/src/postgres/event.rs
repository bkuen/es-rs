use async_trait::async_trait;
use uuid::Uuid;
use crate::error::EsError;
use crate::store::{AggregateStore, EventSourcedAggregate};
use crate::store::aggregate::Aggregate;
use crate::store::event::EventApplier;
use crate::store::snapshot::SnapshotApplier;

#[derive(Clone, Default)]
pub struct DummyEventStore {}

impl DummyEventStore {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl AggregateStore for DummyEventStore {
    type Error = EsError;
    type Transaction = ();

    async fn load<A>(&self, id: Uuid) -> Result<EventSourcedAggregate<A>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        todo!()
    }

    async fn load_events_after<A>(&self, aggregate: &mut EventSourcedAggregate<A>, version: u64) -> Result<(), Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        todo!()
    }

    async fn save<A>(&self, aggregate: &mut EventSourcedAggregate<A>, transaction: Option<&Self::Transaction>) -> Result<(), Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + for<'a> From<&'a A> + 'static
    {
        Ok(())
    }
}