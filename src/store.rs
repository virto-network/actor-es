use crate::{Aggregate, EntityId, Event};
use chrono::prelude::*;
use riker::actors::*;
use std::collections::HashMap;
use std::ops::Range;

/// An actor that handles the persistance of events of entities
/// using "commits" to track who made a change and why.  
#[derive(Default, Debug)]
pub struct Store<T: Aggregate> {
    entities: HashMap<EntityId, (Commit<T>, Vec<Commit<T>>)>,
}

type Author = Option<String>;
type Reason = Option<String>;

/// Commits represent changes to the system about to be persisted
#[derive(Debug, Clone)]
pub struct Commit<T: Aggregate> {
    event: Event<T>,
    when: DateTime<Utc>,
    who: Author,
    why: Option<String>,
}
impl<T: Aggregate> Commit<T> {
    pub fn new(event: Event<T>, who: Author, why: Reason) -> Self {
        Commit {
            event,
            when: Utc::now(),
            who,
            why,
        }
    }
}
impl<T: Aggregate> From<Event<T>> for Commit<T> {
    fn from(e: Event<T>) -> Self {
        Commit::new(e, None, None)
    }
}

#[derive(Debug, Clone)]
pub enum StoreMsg<T: Aggregate> {
    Commit(Commit<T>),
    Snapshot((EntityId, DateTime<Utc>)),
    SnapshotList(Range<DateTime<Utc>>),
    Subscribe(EntityId),
}
impl<T: Aggregate> From<Event<T>> for StoreMsg<T> {
    fn from(msg: Event<T>) -> Self {
        StoreMsg::Commit(msg.into())
    }
}
impl<T: Aggregate> From<Range<DateTime<Utc>>> for StoreMsg<T> {
    fn from(range: Range<DateTime<Utc>>) -> Self {
        StoreMsg::SnapshotList(range)
    }
}
impl<T: Aggregate> From<Commit<T>> for StoreMsg<T> {
    fn from(msg: Commit<T>) -> Self {
        StoreMsg::Commit(msg)
    }
}
impl<T: Aggregate> From<EntityId> for StoreMsg<T> {
    fn from(id: EntityId) -> Self {
        StoreMsg::Subscribe(id)
    }
}
impl<T: Aggregate> From<(EntityId, DateTime<Utc>)> for StoreMsg<T> {
    fn from(snap: (EntityId, DateTime<Utc>)) -> Self {
        StoreMsg::Snapshot(snap)
    }
}

impl<T: Aggregate> Actor for Store<T> {
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

impl<T: Aggregate> Receive<Commit<T>> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, _cx: &Context<Self::Msg>, c: Commit<T>, _sender: Sender) {
        trace!("storing {:?}", c);
        let id = c.event.entity_id();
        match c.event {
            Event::Create(_) => {
                self.entities.insert(id, (c, vec![]));
            }
            Event::Update(_, _) => {
                let (_, updates) = self.entities.get_mut(&id).expect("entity exists");
                updates.push(c);
            }
        }
        debug!("saved commit for {}", id);
    }
}

impl<T: Aggregate> Receive<(EntityId, DateTime<Utc>)> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(
        &mut self,
        _cx: &Context<Self::Msg>,
        (id, _until): (EntityId, DateTime<Utc>),
        sender: Sender,
    ) {
        let (commit, updates) = self.entities.get(&id).unwrap();
        let entity = match commit.event.clone() {
            Event::Create(e) => e,
            Event::Update(_, _) => unreachable!(),
        };
        let snapshot = updates.iter().fold(entity, |mut e, up| {
            let update = match up.event.clone() {
                Event::Create(_) => unreachable!(),
                Event::Update(_, u) => u,
            };
            T::apply_update(&mut e, update);
            e
        });
        debug!("loaded snapshot for {}", id);
        sender
            .unwrap()
            .try_tell(snapshot, None)
            .expect("can receive snapshot");
    }
}

impl<T: Aggregate> Receive<Range<DateTime<Utc>>> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: Range<DateTime<Utc>>, sender: Sender) {
        todo!()
    }
}

impl<T: Aggregate> Receive<EntityId> for Store<T> {
    type Msg = StoreMsg<T>;
    fn receive(&mut self, _cx: &Context<Self::Msg>, _id: EntityId, _sender: Sender) {
        todo!();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use riker_patterns::ask::ask;

    #[derive(Default, Clone, Debug)]
    pub(crate) struct TestCount {
        id: EntityId,
        pub count: i16,
    }
    #[derive(Clone, Debug)]
    pub(crate) enum Op {
        Add(i16),
        Sub(i16),
    }
    impl Aggregate for TestCount {
        type Update = Op;
        fn id(&self) -> EntityId {
            self.id
        }
        fn apply_update(&mut self, update: Self::Update) {
            match update {
                Op::Add(n) => self.count += n,
                Op::Sub(n) => self.count -= n,
            };
        }
    }
    impl Actor for TestCount {
        type Msg = Op;
        fn recv(&mut self, _cx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
            self.apply_update(msg);
        }
    }

    #[tokio::test]
    async fn load_snapshot() {
        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<TestCount>>("test-counts").unwrap();

        let test = TestCount::default();
        let id = test.id();
        store.tell(Event::Create(test), None);
        store.tell(Event::Update(id, Op::Add(15)), None);
        store.tell(Event::Update(id, Op::Add(5)), None);
        store.tell(Event::Update(id, Op::Sub(9)), None);
        store.tell(Event::Update(id, Op::Add(31)), None);

        let result: TestCount = ask(&sys, &store, (id, Utc::now())).await;
        assert_eq!(result.count, 42);
    }

    #[tokio::test]
    async fn load_list_of_snapshots() {
        let sys = ActorSystem::new().unwrap();
        let store = sys.actor_of::<Store<TestCount>>("test-counts").unwrap();

        let before = Utc::now();
        let some_counter = TestCount {
            id: "123".into(),
            count: 42,
        };
        store.tell(Event::Create(some_counter), None);
        store.tell(Event::Create(TestCount::default()), None);
        store.tell(Event::Create(TestCount::default()), None);
        store.tell(Event::Update("123".into(), Op::Add(8)), None);

        let result: Vec<TestCount> = ask(&sys, &store, before..Utc::now()).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].count, 50);
    }
}
