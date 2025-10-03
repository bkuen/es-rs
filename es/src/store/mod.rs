use std::ops::{Deref, DerefMut};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::store::aggregate::{Aggregate, AggregateVersion};
use crate::store::event::{DomainEvent, EventApplier};
use crate::store::snapshot::SnapshotApplier;

pub mod aggregate;
pub mod event;
pub mod view;
pub mod snapshot;
pub mod transaction;

#[derive(Clone, Debug)]
pub struct AggregateEvent<T: DomainEvent> {
    pub(crate) event_id: Uuid,
    pub(crate) event_name: String,
    pub(crate) event_data: T,
    pub(crate) aggregate_version: u64,
    pub(crate) occurred_at: DateTime<Utc>,
}

impl<T: DomainEvent> Deref for AggregateEvent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
       &self.event_data
    }
}

impl <T: DomainEvent> AggregateEvent<T> {
    pub(crate) fn new(event_data: T, aggregate_version: u64) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            event_name: event_data.event_name(),
            event_data,
            aggregate_version,
            occurred_at: Utc::now(),
        }
    }

    pub fn aggregate_version(&self) -> u64 {
        self.aggregate_version
    }
}

impl<T: DomainEvent> AggregateEvent<T> {
    pub fn event_id(&self) -> Uuid {
        self.event_id
    }

    pub fn event_name(&self) -> &String {
        &self.event_name
    }

    pub fn payload(&self) -> &T {
        &self.event_data
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}

pub struct EventSourcedAggregate<A: Aggregate + EventApplier> {
    aggregate: A,
    version: u64,
    events: Vec<AggregateEvent<A::Event>>,
}

impl<A: Aggregate + EventApplier> Default for EventSourcedAggregate<A> {
    fn default() -> Self {
        Self::new(Uuid::now_v7())
    }
}

impl<A: Aggregate + EventApplier> EventSourcedAggregate<A> {

    pub fn new(id: Uuid) -> Self {
        Self {
            aggregate: A::new(id),
            version: 0,
            events: Vec::new(),
        }
    }

    pub fn inner_ref(&self) -> &A {
        &self.aggregate
    }

    pub fn into_inner(self) -> A {
        self.aggregate
    }

    pub fn add_event(&mut self, event: A::Event) {
        self.events.push(AggregateEvent::new(event, self.pending_version()+1));
    }

    /// Apply all pending events to the aggregate
    ///
    /// This will increment the version of the aggregate.
    /// It is important to call this method after saving the events to the store because it will
    /// clear the pending events.
    pub(crate) fn commit_events(&mut self) -> Vec<AggregateEvent<A::Event>> {
        self.version += self.events.len() as u64;
        for event in self.events.iter() {
            self.aggregate.apply_event(&event.event_data);
        }

        self.events.drain(0..).collect()
    }

    pub fn pending_events(&self) -> &[AggregateEvent<A::Event>] {
        &self.events
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn pending_version(&self) -> u64 {
        self.version + self.events.len() as u64
    }

    pub(crate) fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub(crate) fn should_snapshot(&self, snapshot_threshold: usize) -> bool {
        if snapshot_threshold == 0 {
            return false; // treat 0 as "never snapshot"
        }

        let t = snapshot_threshold as u64;
        let before = self.version / t;           // bucket index before committing
        let after  = self.pending_version() / t; // bucket index after committing pending events
        after > before
    }

}

impl<A: Aggregate + EventApplier> Deref for EventSourcedAggregate<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.aggregate
    }
}

impl<A: Aggregate + EventApplier> DerefMut for EventSourcedAggregate<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.aggregate
    }
}

impl<A: Aggregate + EventApplier> AggregateVersion for EventSourcedAggregate<A> {
    fn aggregate_version(&self) -> u64 {
        self.version
    }

    fn set_aggregate_version(&mut self, version: u64) {
        self.version = version;
    }
}

impl<A: Aggregate + SnapshotApplier + EventApplier> SnapshotApplier for EventSourcedAggregate<A> {
    type Snapshot = A::Snapshot;

    fn apply_snapshot(&mut self, snapshot: A::Snapshot) {
        self.aggregate.apply_snapshot(snapshot);
    }
}

#[async_trait]
pub trait AggregateStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Transaction: Send + Sync + 'static;

    async fn load_empty<A>(&self, id: Uuid) -> Result<EventSourcedAggregate<A>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static
    {
        Ok(EventSourcedAggregate::new(id))
    }

    async fn load<A>(&self, id: Uuid) -> Result<EventSourcedAggregate<A>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static;

    async fn load_events_after<A>(&self, aggregate: &mut EventSourcedAggregate<A>, version: u64) -> Result<(), Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + 'static;

    async fn save<A>(&self, aggregate: &mut EventSourcedAggregate<A>, transaction: Option<&Self::Transaction>) -> Result<Vec<AggregateEvent<A::Event>>, Self::Error>
    where
        A: Aggregate + EventApplier + SnapshotApplier,
        A::Event: Unpin + 'static,
        A::Snapshot: Unpin + for<'a> From<&'a A> + 'static;
}

