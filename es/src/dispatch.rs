use std::sync::Arc;
use crate::store::aggregate::Aggregate;
use crate::store::{AggregateStore, EventSourcedAggregate};
use crate::store::event::{EventApplier};
use crate::store::snapshot::SnapshotApplier;
use crate::store::view::{HandleEvents, ViewStore};

pub struct Dispatcher<S, W>
    where S: AggregateStore + Send + Sync,
          W: Send + Sync
{
    aggregate_store: Arc<S>,
    view_store: Arc<ViewStore<W>>,
}

impl<S, W> Dispatcher<S, W>
    where S: AggregateStore + Send + Sync,
          W: Send + Sync
{
    pub fn new(aggregate_store: Arc<S>, view_store: Arc<ViewStore<W>>) -> Self {
        Self {
            aggregate_store,
            view_store,
        }
    }

    pub async fn dispatch<A>(&self, aggregate: &mut EventSourcedAggregate<A>, transaction: Option<&S::Transaction>) -> Result<(), S::Error>
        where A: Aggregate + EventApplier + SnapshotApplier,
              A::Event: Unpin + 'static,
              A::Snapshot: Unpin + for<'a> From<&'a A> + 'static,
              W: HandleEvents<A::Event>
    {
        let events = self.aggregate_store.save(aggregate, transaction).await?;

        self.view_store.update_views::<A::Event>(aggregate.aggregate_id(), &events).await;

        Ok(())
    }
}