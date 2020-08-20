use crate::{EntityId, Event, EventBus, Model};
use chrono::prelude::*;
use riker::actors::*;
use std::collections::HashMap;
use std::ops::RangeTo;

/// An actor that handles the persistance of events of entities
/// using "commits" to track who made a change and why.  
#[derive(Debug)]
pub struct Store<T: Model> {
    entities: HashMap<EntityId, (Commit<T>, Vec<Commit<T>>)>,
    bus: Option<EventBus<T>>,
}
impl<T: Model> Default for Store<T> {
    fn default() -> Self {
        Self {
            bus: None,
            entities: HashMap::new(),
        }
    }
}

type Author = Option<String>;
type Reason = Option<String>;

/// Commits represent changes to the system about to be persisted
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

    pub fn entity_id(&self) -> EntityId {
        self.event.entity_id()
    }
}
impl<T: Model> From<Event<T>> for Commit<T> {
    fn from(e: Event<T>) -> Self {
        Commit::new(e, None, None)
    }
}

impl<T: Model> Store<T> {
    fn make_snapshot(&self, id: EntityId, _until: DateTime<Utc>) -> Option<T> {
        let (commit, updates) = self.entities.get(&id)?;
        let entity = match commit.event.clone() {
            Event::Create(e) => e,
            Event::Change(_, _) => unreachable!(),
        };
        let snapshot = updates.iter().fold(entity, |mut e, up| {
            let update = match up.event.clone() {
                Event::Create(_) => unreachable!(),
                Event::Change(_, u) => u,
            };
            T::apply_change(&mut e, update);
            e
        });
        Some(snapshot)
    }
}

pub type StoreRef<A> = ActorRef<StoreMsg<A>>;

impl<T: Model> Actor for Store<T> {
    type Msg = StoreMsg<T>;
    fn recv(&mut self, cx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            StoreMsg::Commit(msg) => self.receive(cx, msg, sender),
            StoreMsg::Subscribe(msg) => self.receive(cx, msg, sender),
            StoreMsg::Snapshot(msg) => self.receive(cx, msg, sender),
            StoreMsg::SnapshotList(msg) => self.receive(cx, msg, sender),
        };
    }
}

impl<T: Model> ActorFactoryArgs<EventBus<T>> for Store<T> {
    fn create_args(bus: EventBus<T>) -> Self {
        Store {
            bus: Some(bus),
            ..Default::default()
        }
    }
}

impl<T: Model> Receive<Commit<T>> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, cx: &Context<Self::Msg>, c: Commit<T>, _sender: Sender) {
        trace!("storing {:?}", c);
        let id = c.event.entity_id();
        let commit = c.clone();
        match c.event {
            Event::Create(_) => {
                self.entities.insert(id, (commit, vec![]));
            }
            Event::Change(_, _) => {
                let (_, updates) = self.entities.get_mut(&id).expect("entity exists");
                updates.push(commit);
            }
        }
        if self.bus.is_some() {
            self.bus.as_ref().unwrap().tell(
                Publish {
                    topic: cx.myself.name().into(),
                    msg: c.event,
                },
                None,
            );
        }
        debug!("saved commit for {}", id);
    }
}

impl<T: Model> Receive<(EntityId, DateTime<Utc>)> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(
        &mut self,
        _cx: &Context<Self::Msg>,
        (id, until): (EntityId, DateTime<Utc>),
        sender: Sender,
    ) {
        let snapshot = self.make_snapshot(id, until);
        if snapshot.is_some() {
            debug!("loaded snapshot for {}", id);
        } else {
            debug!("entity {} not in store", id);
        }
        sender
            .unwrap()
            .try_tell(snapshot, None)
            .expect("can receive snapshot");
    }
}

impl<T: Model> Receive<RangeTo<DateTime<Utc>>> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(
        &mut self,
        _ctx: &Context<Self::Msg>,
        range: RangeTo<DateTime<Utc>>,
        sender: Sender,
    ) {
        let snaps = self
            .entities
            .keys()
            .map(|id| self.make_snapshot(*id, range.end).unwrap())
            .collect::<Vec<_>>();
        trace!("{:?}", snaps);
        debug!("loaded list of snapshots until {}", range.end);
        sender
            .unwrap()
            .try_tell(snaps, None)
            .expect("can receive snapshot list");
    }
}

impl<T: Model> Receive<EntityId> for Store<T> {
    type Msg = StoreMsg<T>;
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
        fn apply_change(&mut self, change: Self::Change) {
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
