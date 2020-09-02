use crate::{EntityId, Event, EventBus, Model};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::future::ok;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use riker::actors::*;
use std::fmt;
use std::ops::{Deref, RangeTo};
use std::sync::Arc;
use thiserror::Error;

pub use in_memory::MemStore;

mod in_memory;

pub type DynCommitStore<M> = Arc<Box<dyn CommitStore<M>>>;
type CommitResult<T> = Result<T, CommitError>;

/// A wrapper for a stored entity that applies changes until the specified moment in time.
struct TimeTraveler<'a, M: Model> {
    model: M,
    changes: BoxStream<'a, CommitResult<&'a Commit<M>>>,
}

impl<'a, M: Model> TimeTraveler<'a, M> {
    async fn to_present(self) -> CommitResult<M> {
        self.travel_to(Utc::now()).await
    }

    async fn travel_to(self, until: DateTime<Utc>) -> CommitResult<M> {
        let model = self
            .changes
            .try_fold(self.model, |mut m, c| {
                let change = c.change().unwrap();
                m.apply_change(change);
                ok(m)
            })
            .await?;
        Ok(model)
    }
}

impl<M: Model> fmt::Debug for TimeTraveler<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TimeTraveler({:?})", self.model)
    }
}

#[async_trait]
pub trait CommitStore<M: Model>: fmt::Debug + Send + Sync + 'static {
    fn keys(&self) -> BoxStream<CommitResult<EntityId>>;

    fn commit_list(&self, id: EntityId) -> BoxStream<CommitResult<&Commit<M>>>;

    async fn commit(&self, c: Commit<M>) -> CommitResult<()>;

    fn list(&self) -> BoxStream<CommitResult<TimeTraveler<'_, M>>> {
        self.keys().and_then(|id| self.get(id)).boxed()
    }

    async fn get(&self, id: EntityId) -> CommitResult<TimeTraveler<'_, M>> {
        let mut changes = self.commit_list(id);
        let model = changes
            .try_next()
            .await?
            .ok_or(CommitError::NotFound)?
            .entity()
            // first change has to be the entity
            .unwrap();
        Ok(TimeTraveler { changes, model })
    }

    async fn snapshot(&self, id: EntityId, time: DateTime<Utc>) -> CommitResult<M> {
        self.get(id).await?.travel_to(time).await
    }
}

#[derive(Error, Debug)]
enum CommitError {
    #[error("Cant change non existing entity")]
    CantChange,
    #[error("Didn't find commit for entity")]
    NotFound,
}

/// An actor that handles the persistance of events of entities
/// using "commits" to track who made a change and why.  
#[derive(Debug)]
pub struct Store<M: Model> {
    bus: Option<EventBus<M>>,
    backend: DynCommitStore<M>,
}

pub type StoreRef<A> = ActorRef<StoreMsg<A>>;

impl<M: Model> Actor for Store<M> {
    type Msg = StoreMsg<M>;
    fn recv(&mut self, cx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            StoreMsg::Commit(msg) => self.receive(cx, msg, sender),
            StoreMsg::Subscribe(msg) => self.receive(cx, msg, sender),
            StoreMsg::Snapshot(msg) => self.receive(cx, msg, sender),
            StoreMsg::SnapshotList(msg) => self.receive(cx, msg, sender),
        };
    }
}

impl<M: Model> ActorFactoryArgs<DynCommitStore<M>> for Store<M> {
    fn create_args(backend: DynCommitStore<M>) -> Self {
        Store { backend, bus: None }
    }
}

impl<M: Model> Receive<Commit<M>> for Store<M> {
    type Msg = StoreMsg<M>;
    fn receive(&mut self, cx: &Context<Self::Msg>, c: Commit<M>, _sender: Sender) {
        trace!("storing {:?}", c);
        let store = self.backend.clone();
        cx.run(async move {
            let id = c.entity_id();
            store.commit(c).await;
            //if self.bus.is_some() {
            //    self.bus.as_ref().unwrap().tell(
            //        Publish {
            //            topic: cx.myself.name().into(),
            //            msg: c.event,
            //        },
            //        None,
            //    );
            //}
            debug!("saved commit for {}", id);
        });
    }
}

impl<M: Model> Receive<(EntityId, DateTime<Utc>)> for Store<M> {
    type Msg = StoreMsg<M>;

    fn receive(
        &mut self,
        cx: &Context<Self::Msg>,
        (id, until): (EntityId, DateTime<Utc>),
        sender: Sender,
    ) {
        let store = self.backend.clone();
        cx.run(async move {
            if let Ok(snapshot) = store.snapshot(id, until).await {
                debug!("loaded snapshot for {}", id);
                sender
                    .unwrap()
                    .try_tell(snapshot, None)
                    .expect("can receive snapshot");
            } else {
                debug!("Couldn't load entity {}", id);
            }
        });
    }
}

impl<M: Model> Receive<RangeTo<DateTime<Utc>>> for Store<M> {
    type Msg = StoreMsg<M>;

    fn receive(&mut self, cx: &Context<Self::Msg>, range: RangeTo<DateTime<Utc>>, sender: Sender) {
        let backend = self.backend.clone();
        cx.run(async move {
            let entities = backend
                .clone()
                .list()
                .and_then(|entity| entity.travel_to(range.end))
                .try_collect::<Vec<M>>()
                .await
                .expect("list entities");
            sender
                .unwrap()
                .try_tell(entities, None)
                .expect("can receive snapshot list");
            debug!("loaded list of snapshots until {}", range.end);
        });
    }
}

impl<M: Model> Receive<EntityId> for Store<M> {
    type Msg = StoreMsg<M>;

    fn receive(&mut self, _cx: &Context<Self::Msg>, _id: EntityId, _sender: Sender) {
        todo!();
    }
}

#[derive(Debug, Clone)]
pub enum StoreMsg<T: Model> {
    Commit(Commit<T>),
    Snapshot((EntityId, DateTime<Utc>)),
    SnapshotList(RangeTo<DateTime<Utc>>),
    Subscribe(EntityId),
}
impl<T: Model> From<Event<T>> for StoreMsg<T> {
    fn from(msg: Event<T>) -> Self {
        StoreMsg::Commit(msg.into())
    }
}
impl<T: Model> From<RangeTo<DateTime<Utc>>> for StoreMsg<T> {
    fn from(range: RangeTo<DateTime<Utc>>) -> Self {
        StoreMsg::SnapshotList(range)
    }
}
impl<T: Model> From<Commit<T>> for StoreMsg<T> {
    fn from(msg: Commit<T>) -> Self {
        StoreMsg::Commit(msg)
    }
}
impl<T: Model> From<EntityId> for StoreMsg<T> {
    fn from(id: EntityId) -> Self {
        StoreMsg::Subscribe(id)
    }
}
impl<T: Model> From<(EntityId, DateTime<Utc>)> for StoreMsg<T> {
    fn from(snap: (EntityId, DateTime<Utc>)) -> Self {
        StoreMsg::Snapshot(snap)
    }
}

type Author = Option<String>;
type Reason = Option<String>;

/// Commit represents a unique inmutable change to the system made by someone at a specific time
#[derive(Debug, Clone)]
pub struct Commit<T: Model> {
    event: Event<T>,
    when: DateTime<Utc>,
    who: Author,
    why: Reason,
}
impl<T: Model> Commit<T> {
    pub fn new(event: Event<T>, who: Author, why: Reason) -> Self {
        Commit {
            event,
            when: Utc::now(),
            who,
            why,
        }
    }
}

impl<T: Model> Deref for Commit<T> {
    type Target = Event<T>;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<T: Model> From<Event<T>> for Commit<T> {
    fn from(e: Event<T>) -> Self {
        Commit::new(e, None, None)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::executor::block_on;
    use riker_patterns::ask::ask;

    #[derive(Default, Clone, Debug)]
    pub struct TestCount {
        id: EntityId,
        pub count: i16,
    }
    impl TestCount {
        pub fn new(c: i16) -> Self {
            Self {
                count: c,
                id: EntityId::new(),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub enum Op {
        Add(i16),
        Sub(i16),
    }
    impl Model for TestCount {
        type Change = Op;
        fn id(&self) -> EntityId {
            self.id
        }
        fn apply_change(&mut self, change: &Self::Change) {
            match change {
                Op::Add(n) => self.count += n,
                Op::Sub(n) => self.count -= n,
            };
        }
    }

    #[test]
    fn load_snapshot() {
        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<TestCount>>("test-counts").unwrap();

        let test = TestCount::default();
        let id = test.id();
        store.tell(Event::Create(test), None);
        store.tell(Event::Change(id, Op::Add(15)), None);
        store.tell(Event::Change(id, Op::Add(5)), None);
        store.tell(Event::Change(id, Op::Sub(9)), None);
        store.tell(Event::Change(id, Op::Add(31)), None);

        let result: Option<TestCount> = block_on(ask(&sys, &store, (id, Utc::now())));
        assert_eq!(result.unwrap().count, 42);
    }

    #[test]
    fn non_existing_entity() {
        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<TestCount>>("test-counts").unwrap();

        let result: Option<TestCount> = block_on(ask(&sys, &store, ("123".into(), Utc::now())));
        assert!(result.is_none());
    }

    #[test]
    fn load_list_of_snapshots() {
        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<TestCount>>("test-counts").unwrap();

        let some_counter = TestCount {
            id: "123".into(),
            count: 42,
        };
        store.tell(Event::Create(some_counter), None);
        store.tell(Event::Create(TestCount::default()), None);
        store.tell(Event::Create(TestCount::default()), None);
        store.tell(Event::Change("123".into(), Op::Add(8)), None);

        let result: Vec<TestCount> = block_on(ask(&sys, &store, ..Utc::now()));
        assert_eq!(result.len(), 3);
        let some_counter_snapshot = result.iter().find(|s| s.id() == "123".into()).unwrap();
        assert_eq!(some_counter_snapshot.count, 50);
    }

    #[test]
    fn broadcast_event() {
        let sys = ActorSystem::new().unwrap();
        let bus: EventBus<_> = channel("bus", &sys).unwrap();
        let store_name = "test-counts";
        let store = sys
            .actor_of_args::<Store<TestCount>, _>(store_name, bus.clone())
            .unwrap();

        #[derive(Clone, Debug)]
        pub struct Get;
        #[derive(Clone, Debug)]
        enum TestSubMsg {
            Event(Event<TestCount>),
            Get,
        }
        impl From<Event<TestCount>> for TestSubMsg {
            fn from(event: Event<TestCount>) -> Self {
                TestSubMsg::Event(event)
            }
        }

        #[derive(Default)]
        struct TestSub(Option<Event<TestCount>>);
        impl Actor for TestSub {
            type Msg = TestSubMsg;
            fn recv(&mut self, _cx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
                match msg {
                    TestSubMsg::Get => {
                        sender.unwrap().try_tell(self.0.clone(), None).unwrap();
                    }
                    TestSubMsg::Event(e) => {
                        self.0 = Some(e);
                    }
                }
            }
        }

        let sub = sys.actor_of::<TestSub>("subscriber").unwrap();
        bus.tell(
            Subscribe {
                topic: store_name.into(),
                actor: Box::new(sub.clone()),
            },
            None,
        );

        store.tell(Event::Create(TestCount::default()), None);

        let result: Option<Event<TestCount>> = block_on(ask(&sys, &sub, TestSubMsg::Get));

        assert!(result.is_some());
    }
}
