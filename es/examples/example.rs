use es::postgres::event::DummyEventStore;
use es::store::aggregate::Aggregate;
use es::store::event::{DomainEvent, EventApplier};
use es::store::snapshot::{Snapshot, SnapshotApplier};
use es::store::view::{HandleEvents, View, ViewStore, ViewUpdater};
use es::store::{AggregateEvent, AggregateStore};
use es::store::EventSourcedAggregate;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
use es::dispatch::Dispatcher;
use es::postgres::transaction::PgTransaction;

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
            id: Uuid::now_v7(),
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
impl View<PersonAggregate> for PersonView {
    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<PersonEvent>]) {
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
            id: Uuid::now_v7(),
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

pub struct OrganizationView {}

#[async_trait::async_trait]
impl View<OrganizationAggregate> for OrganizationView {
    async fn update(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<OrganizationEvent>]) {
        for event in events {
            match &**event {
                OrganizationEvent::Create { name } => {
                    println!("Updating view for organization {}: created with name {}", aggregate_id, name);
                }
            }
        }
    }
}

pub enum MyViewWrapper {
    PersonView(PersonView),
    OrganizationView(OrganizationView),
}

#[async_trait::async_trait]
impl HandleEvents<PersonEvent> for MyViewWrapper {
    async fn handle(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<PersonEvent>]) {
        if let MyViewWrapper::PersonView(view) = self {
            view.update(aggregate_id, events).await;
        }
    }
}

#[async_trait::async_trait]
impl HandleEvents<OrganizationEvent> for MyViewWrapper {
    async fn handle(&mut self, aggregate_id: Uuid, events: &[AggregateEvent<OrganizationEvent>]) {
        if let MyViewWrapper::OrganizationView(view) = self {
            view.update(aggregate_id, events).await;
        }
    }
}

impl From<PersonView> for MyViewWrapper {
    fn from(view: PersonView) -> Self {
        MyViewWrapper::PersonView(view)
    }
}

impl From<OrganizationView> for MyViewWrapper {
    fn from(view: OrganizationView) -> Self {
        MyViewWrapper::OrganizationView(view)
    }
}

// #[es_macros::view_store(
//     views(
//         PersonView,
//         OrganizationView,
//     )
// )]
// pub struct BetterViewStore;

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

    let mut organization_aggregate = EventSourcedAggregate::<OrganizationAggregate>::new(Uuid::now_v7());

    let organization_event = OrganizationEvent::Create {
        name: "Organization Name".to_string(),
    };

    let event_store = Arc::new(DummyEventStore::new());

    let person_view = PersonView {};
    let organization_view = OrganizationView {};
    // let mut store = ViewStore::<MyViewWrapper, _>::new(event_store);
    // store.register_view(view).await;

    person_aggregate.apply_event(&person_event);
    organization_aggregate.apply_event(&organization_event);
    // store.update_views(aggregate.id, &[event.clone()]).await;


    let view_store = Arc::new(ViewStore::<MyViewWrapper>::new());
    view_store.register_view(person_view).await;
    view_store.register_view(organization_view).await;

    let dispatcher = Dispatcher::new(event_store.clone(), view_store.clone());
    dispatcher.dispatch(&mut person_aggregate, Some(&())).await.unwrap();
}