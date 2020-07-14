#[macro_use]
extern crate log;

use bson::{from_bson, to_bson, Bson};
use chrono::prelude::*;
use riker::actors::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;
use uuid::Uuid;

//trait EventStorage {}

pub trait Aggregate: Message + Serialize {
    type Update: Message + Serialize;
    fn id(&self) -> EntityId;
}

#[derive(Default)]
pub struct Store<T> {
    events: Vec<StoreEvent>,
    _e: PhantomData<T>,
}

//--- not needed after riker bug fix ---
#[derive(Debug, Clone)]
pub enum StoreMsg<T: Aggregate> {
    Event(Event<T>),
    Query(EntityId),
}
impl<T: Aggregate> From<Event<T>> for StoreMsg<T> {
    fn from(e: Event<T>) -> Self {
        StoreMsg::Event(e)
    }
}
impl<T: Aggregate> From<EntityId> for StoreMsg<T> {
    fn from(q: EntityId) -> Self {
        StoreMsg::Query(q)
    }
}
//---------------------------------------

impl<T: Aggregate> Actor for Store<T> {
    type Msg = StoreMsg<T>;
    fn recv(&mut self, cx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            StoreMsg::Event(msg) => self.receive(cx, msg, sender),
            StoreMsg::Query(msg) => self.receive(cx, msg, sender),
        };
    }
}

impl<T: Aggregate> Receive<EntityId> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, cx: &Context<Self::Msg>, id: EntityId, sender: Sender) {
        debug!("reading events of {}", id);
        for e in self.events.iter().take(1) {
            //.filter(|e| e.id == id) {
            let event: StoreEvent = e.clone();
            sender
                .as_ref()
                .unwrap()
                .try_tell(event, Some(cx.myself().into()))
                .unwrap();
            //.expect("send event back");
        }
    }
}

impl<T: Aggregate> Receive<Event<T>> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, _cx: &Context<Self::Msg>, msg: Event<T>, _sender: Sender) {
        debug!("storing event {}", msg.id());
        self.events.push(msg.into());
        debug!("stored event {:?}", self.events);
    }
}

/// Events as they are stored in an EventStore
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreEvent {
    pub id: EntityId,
    pub action: Action,
    pub when: DateTime<Utc>,
    pub who: Option<String>,
    pub data: Bson,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    Create,
    Update,
}

impl<T: Aggregate> From<Event<T>> for StoreEvent {
    fn from(event: Event<T>) -> Self {
        match event {
            Event::Create(entity, author) => StoreEvent {
                id: entity.id(),
                action: Action::Create,
                data: to_bson(&entity).unwrap(),
                when: Utc::now(),
                who: Some(author),
            },
            Event::Update(id, msg, author) => StoreEvent {
                id,
                action: Action::Update,
                data: to_bson(&msg).unwrap(),
                when: Utc::now(),
                who: Some(author),
            },
        }
    }
}

/// Uniquely idenfies an entity
#[derive(Serialize, Deserialize, Clone, Debug, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityId(Uuid);
impl EntityId {
    pub fn new() -> Self {
        Default::default()
    }
}
impl From<&str> for EntityId {
    fn from(id: &str) -> Self {
        EntityId(Uuid::new_v5(&Uuid::NAMESPACE_URL, id.as_bytes()))
    }
}
impl From<Uuid> for EntityId {
    fn from(uuid: Uuid) -> Self {
        EntityId(uuid)
    }
}
impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Default for EntityId {
    fn default() -> Self {
        Uuid::new_v4().into()
    }
}

#[derive(Debug, Clone)]
pub enum Query {
    One(EntityId),
}

#[derive(Clone, Debug)]
pub enum Event<T: Aggregate> {
    Create(T, Author),
    Update(EntityId, T::Update, Author),
}
impl<T: Aggregate> Event<T> {
    pub fn id(&self) -> EntityId {
        match self {
            Event::Create(e, _) => e.id(),
            Event::Update(id, _, _) => *id,
        }
    }
}

type Author = String;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{executor::block_on, future::FutureExt, select};
    use futures_timer::Delay;
    use riker_patterns::ask::ask;
    use std::time::Duration;

    #[tokio::test]
    async fn event_store() {
        #[derive(Serialize, Clone, Debug, Default)]
        struct Data;
        #[derive(Serialize, Clone, Debug)]
        struct Update;
        impl Aggregate for Data {
            type Update = Update;
            fn id(&self) -> EntityId {
                "123".into()
            }
        }

        type StoreRef = ActorRef<<Store<Data> as Actor>::Msg>;

        #[actor(StoreEvent, Query)]
        #[derive(Default)]
        struct TestEntity {
            store: Option<StoreRef>,
        }
        impl ActorFactoryArgs<StoreRef> for TestEntity {
            fn create_args(store: StoreRef) -> Self {
                TestEntity { store: Some(store) }
            }
        }
        impl Actor for TestEntity {
            type Msg = TestEntityMsg;
            fn recv(&mut self, cx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
                self.receive(cx, msg, sender);
            }
        }

        impl Receive<StoreEvent> for TestEntity {
            type Msg = TestEntityMsg;
            fn receive(&mut self, _cx: &Context<Self::Msg>, msg: StoreEvent, _sender: Sender) {
                debug!("got msg {:?}", msg);
            }
        }
        impl Receive<Query> for TestEntity {
            type Msg = TestEntityMsg;
            fn receive(&mut self, cx: &Context<Self::Msg>, q: Query, sender: Sender) {
                debug!("got Query {:?}", q);
                match q {
                    Query::One(id) => self
                        .store
                        .as_ref()
                        .unwrap()
                        .tell(id, Some(cx.myself().into())),
                };
                cx.run(async move {
                    debug!("waiiiiiiiiiiiittttttt");
                    let answer: Option<Data> = loop {
                        select! {
                            _ = Delay::new(Duration::from_millis(200)).fuse() => {
                                break None;
                            }
                        }
                    };
                    debug!("timeeeeeeeeeeeeeeeeeeee");
                    sender
                        .as_ref()
                        .unwrap()
                        .try_tell(answer, None)
                        .expect("receive query");
                })
                .unwrap();
            }
        }

        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<Data>>("store").unwrap();
        store.tell(Event::Create(Data, "yo".into()), None);
        store.tell(Event::Update("123".into(), Update, "tu".into()), None);

        let entity = sys
            .actor_of_args::<TestEntity, _>("test-entity", store)
            .unwrap();
        let data: Option<Data> = ask(&sys, &entity, Query::One("123".into())).await;
        assert!(data.is_some());
    }
}
