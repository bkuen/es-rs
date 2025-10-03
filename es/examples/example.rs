use es::postgres::event::{EventStore};
use es::store::aggregate::Aggregate;
use es::store::event::{DomainEvent, EventApplier};
use es::store::snapshot::{Snapshot, SnapshotApplier};
use es::store::view::{HandleEvents, View, ViewStore};
use es::store::{AggregateEvent};
use es::store::EventSourcedAggregate;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
use es::dispatch::Dispatcher;
use es::error::EsError;
use es::postgres::transaction::PgTransaction;
use es::store::transaction::Transaction;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PersonAggregate {
    id: Uuid,
    name: String,
    age: u8,
}

impl Aggregate for PersonAggregate {
    const NAME: &'static str = "Person";

    fn new(id: Uuid) -> Self {
        Self {
            id,
            name: "Default Name".to_string(),
            age: 18,
        }
    }

    fn aggregate_id(&self) -> Uuid {
        self.id
    }
}

impl EventApplier for PersonAggregate {
    type Event = PersonEvent;

    fn apply_event(&mut self, event: &Self::Event) {
        match event {
            PersonEvent::Rename { name } => {
                self.name = name.clone();
                println!("Person renamed to: {}", self.name);
            }
            PersonEvent::Aging { age } => {
                self.age = *age;
                println!("Person aged to: {}", self.age);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PersonSnapshot {
    name: String,
    age: u8,
}

impl Snapshot for PersonSnapshot {
    fn snapshot_name(&self) -> String {
        "PersonSnapshot".to_string()
    }
}

impl From<&PersonAggregate> for PersonSnapshot {
    fn from(aggregate: &PersonAggregate) -> Self {
        Self {
            name: aggregate.name.clone(),
            age: aggregate.age,
        }
    }
}

impl SnapshotApplier for PersonAggregate {
    type Snapshot = PersonSnapshot;

    fn apply_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.name = snapshot.name;
        self.age = snapshot.age;

        println!("Person snapshot applied: name={}, age={}", self.name, self.age);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PersonEvent {
    Rename {
        name: String,
    },
    Aging{
        age: u8,
    },

}

impl DomainEvent for PersonEvent {
    fn event_name(&self) -> String {
        match self {
            PersonEvent::Rename { .. } => "PersonRenamed".to_string(),
            PersonEvent::Aging { .. } => "PersonAged".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OrganizationEvent {
    Create {
        name: String,
    },
}

impl DomainEvent for OrganizationEvent {
    fn event_name(&self) -> String {
        match self {
            OrganizationEvent::Create { .. } => "OrganizationCreated".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EventWrapper {
    Person(PersonEvent),
    Organization(OrganizationEvent),
}

impl From<PersonEvent> for EventWrapper {
    fn from(event: PersonEvent) -> Self {
        Self::Person(event)
    }
}

impl From<OrganizationEvent> for EventWrapper {
    fn from(event: OrganizationEvent) -> Self {
        Self::Organization(event)
    }
}

pub struct PersonView {}

#[async_trait::async_trait]
impl View for PersonView {
    type Aggregate = PersonAggregate;
    type Transaction = PgTransaction;
    type Error = EsError;

    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<PersonEvent>], tx: Option<&Self::Transaction>) -> Result<(), Self::Error> {
        for event in events {
            match &**event {
                PersonEvent::Rename { name } => {
                    println!("Updating view for person {}: renamed to {}", aggregate_id, name);
                }
                PersonEvent::Aging { age } => {
                    println!("Updating view for person {}: aged to {}", aggregate_id, age);
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrganizationAggregate {
    id: Uuid,
    name: String,
}

impl Aggregate for OrganizationAggregate {
    const NAME: &'static str = "Organization";

    fn new(id: Uuid) -> Self {
        Self {
            id,
            name: "Default Name".to_string(),
        }
    }

    fn aggregate_id(&self) -> Uuid {
        self.id
    }
}

impl EventApplier for OrganizationAggregate {
    type Event = OrganizationEvent;

    fn apply_event(&mut self, event: &Self::Event) {
        match event {
            OrganizationEvent::Create { name } => {
                self.name = name.clone();
                println!("Organization created with name: {}", self.name);
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrganizationSnapshot {
    name: String,
}

impl Snapshot for OrganizationSnapshot {
    fn snapshot_name(&self) -> String {
        "OrganizationSnapshot".to_string()
    }
}

impl SnapshotApplier for OrganizationAggregate {
    type Snapshot = OrganizationSnapshot;

    fn apply_snapshot(&mut self, snapshot: Self::Snapshot) {
        self.name = snapshot.name;

        println!("Organization snapshot applied: name={}", self.name);
    }
}

impl From<&OrganizationAggregate> for OrganizationSnapshot {
    fn from(aggregate: &OrganizationAggregate) -> Self {
        Self {
            name: aggregate.name.clone(),
        }
    }
}

pub struct OrganizationView {}

#[async_trait::async_trait]
impl View for OrganizationView {
    type Aggregate = OrganizationAggregate;
    type Transaction = PgTransaction;
    type Error = EsError;

    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<OrganizationEvent>], tx: Option<&Self::Transaction>) -> Result<(), Self::Error> {
        for event in events {
            match &**event {
                OrganizationEvent::Create { name } => {
                    println!("Updating view for organization {}: created with name {}", aggregate_id, name);
                }
            }
        }

        Ok(())
    }
}

#[es_macros::view_wrapper(
    views(
        PersonView(PersonEvent),
        OrganizationView(OrganizationEvent),
    )
)]
pub struct MyViewWrapper;

#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user:pass@localhost:5432/es").await.unwrap();

    let tx = PgTransaction::new(&pool).await.unwrap();

    let mut person_aggregate = EventSourcedAggregate::<PersonAggregate>::new(Uuid::now_v7());

    let person_event = PersonEvent::Rename{
        name: "Updated Name".to_string(),
    };

    let person_event2 = PersonEvent::Rename{
        name: "Updated Name 2".to_string(),
    };

    let person_event3 = PersonEvent::Rename{
        name: "Updated Name 3".to_string(),
    };

    let person_event4 = PersonEvent::Rename{
        name: "Updated Name 4".to_string(),
    };

    let person_event5 = PersonEvent::Rename{
        name: "Updated Name 5".to_string(),
    };


    let mut organization_aggregate = EventSourcedAggregate::<OrganizationAggregate>::new(Uuid::now_v7());

    let organization_event = OrganizationEvent::Create {
        name: "Organization Name".to_string(),
    };

    let event_store_config = es::postgres::event::Config::default()
        .with_events_table_name("events")
        .with_snapshots_table_name("snapshots");
    let event_store = Arc::new(EventStore::new(event_store_config, &pool));

    let person_view = PersonView {};
    let organization_view = OrganizationView {};

    person_aggregate.add_event(person_event);
    person_aggregate.add_event(person_event2);
    person_aggregate.add_event(person_event3);
    person_aggregate.add_event(person_event4);
    person_aggregate.add_event(person_event5);
    organization_aggregate.add_event(organization_event);

    let view_store = Arc::new(ViewStore::<MyViewWrapper>::new());
    view_store.register_view(person_view).await;
    view_store.register_view(organization_view).await;

    let dispatcher = Dispatcher::new(event_store.clone(), view_store.clone());
    dispatcher.dispatch(&mut person_aggregate, Some(&tx)).await.unwrap();
    dispatcher.dispatch(&mut organization_aggregate, Some(&tx)).await.unwrap();

    let dispatcher2 = dispatcher.clone();

    tx.commit().await.expect("failed to commit")
}