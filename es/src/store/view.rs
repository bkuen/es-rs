use std::error::Error;
use std::sync::{Arc};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::store::{AggregateEvent};
use crate::store::event::{DomainEvent, EventApplier};

#[async_trait]
pub trait View {
    type Aggregate: EventApplier;
    type Error: Error;

    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<<Self::Aggregate as EventApplier>::Event>]) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait HandleEvents<E: DomainEvent> {
    type Error: Error;

    async fn handle(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<E>]) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug)]
pub struct ViewStore<W>
{
    views: Arc<Mutex<Vec<W>>>,
}

impl<W> ViewStore<W>
{
    pub fn new() -> Self {
        ViewStore {
            views: Default::default(),
        }
    }

    pub async fn register_view<V>(&self, view: V)
    where
        V: View + Send + Sync + Into<W> + 'static,
    {
        let mut views = self.views.lock().await;
        views.push(view.into());
    }

    pub async fn update_views<E, Err>(&self, aggregate_id: Uuid, events: &[AggregateEvent<E>]) -> Result<(), Err>
    where
        E: DomainEvent + Send + Sync + 'static,
        W: HandleEvents<E> + Send + Sync,
        Err: Error + From<<W as HandleEvents<E>>::Error>
    {
        let mut views = self.views.lock().await;
        for view in views.iter_mut() {
            view.handle(aggregate_id, events).await?;
        }

        Ok(())
    }
}

impl<W> Default for ViewStore<W> {
    fn default() -> Self {
        Self {
            views: Default::default(),
        }
    }
}