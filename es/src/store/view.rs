use std::sync::{Arc};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::store::{AggregateEvent};
use crate::store::event::{DomainEvent, EventApplier};

#[async_trait]
pub trait View<A: EventApplier> {
    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<A::Event>]);
}

#[async_trait]
pub trait HandleEvents<E: DomainEvent> {
    async fn handle(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<E>]);
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

    pub async fn register_view<V, A>(&self, view: V)
    where
        V: View<A> + Send + Sync + Into<W> + 'static,
        A: EventApplier + Send + Sync + 'static,
    {
        let mut views = self.views.lock().await;
        views.push(view.into());
    }

    pub async fn update_views<E>(&self, aggregate_id: Uuid, events: &[AggregateEvent<E>])
    where
        E: DomainEvent + Send + Sync + 'static,
        W: HandleEvents<E> + Send + Sync,
    {
        let mut views = self.views.lock().await;
        for view in views.iter_mut() {
            view.handle(aggregate_id, events).await;
        }
    }
}