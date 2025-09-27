use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

pub trait Aggregate:
    Serialize + DeserializeOwned + Sync + Send
{
    const NAME: &'static str;

    fn new(id: Uuid) -> Self;

    fn aggregate_name(&self) -> &'static str {
        Self::NAME
    }

    fn aggregate_id(&self) -> Uuid;
}

pub trait AggregateVersion {
    fn aggregate_version(&self) -> u64;

    fn set_aggregate_version(&mut self, version: u64);
}