use std::fmt::Debug;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait Snapshot:
    Serialize + DeserializeOwned + Clone + PartialEq + Debug + Send + Sync
{
    fn snapshot_name(&self) -> String;
}

pub trait SnapshotApplier {
    type Snapshot: Snapshot;

    fn apply_snapshot(&mut self, snapshot: Self::Snapshot);
}