use std::fmt::Debug;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait DomainEvent:
    Serialize + DeserializeOwned + Clone + PartialEq + Debug + Send + Sync
{
    fn event_name(&self) -> String;
}

pub trait EventApplier {
    type Event: DomainEvent;

    fn apply_event(&mut self, event: &Self::Event);
}