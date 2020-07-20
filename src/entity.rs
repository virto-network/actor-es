use crate::{EntityId, Store, StoreMsg};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::lock::Mutex;
use riker::actors::*;
use std::fmt;
use std::sync::Arc;

/// An Aggregate is the projected data of a series of events of an entity,
/// given an initial state update events are applied to it until it reaches the desired state.
pub trait Aggregate: Message + Default {
    type Update: Message;
    fn id(&self) -> EntityId;
    fn apply_update(&mut self, update: Self::Update);
}

pub type StoreRef<A> = ActorRef<StoreMsg<A>>;
pub trait Sys: TmpActorRefFactory + Run + Send + Sync {}
impl<T: TmpActorRefFactory + Run + Send + Sync> Sys for T {}

/// Implement this trait to allow your entity handle external commands
#[async_trait]
pub trait ES: Default + fmt::Debug + Send + Sync + 'static {
    type Agg: Aggregate;
    type Cmd: Message;
    type Event: Message;

    async fn handle_command<Ctx: Sys>(&self, cmd: Self::Cmd, ctx: Ctx, store: StoreRef<Self::Agg>);

    async fn handle_event<Ctx: Sys>(
        &self,
        _event: Self::Event,
        _ctx: Ctx,
        _store: StoreRef<Self::Agg>,
    ) {
    }
}

/// Entity is an actor that dispatches commands and manages aggregates that are being queried
pub struct Entity<E: ES> {
    store: Option<StoreRef<E::Agg>>,
    es: Arc<Mutex<E>>,
}

impl<E: ES> Default for Entity<E> {
    fn default() -> Self {
        Entity {
            store: None,
            es: Arc::new(Mutex::new(E::default())),
        }
    }
}

impl<E: ES> Actor for Entity<E> {
    type Msg = CQRS<E::Cmd, E::Event>;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.store = Some(ctx.actor_of::<Store<E::Agg>>(ctx.myself().name()).unwrap());
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            CQRS::Query(q) => self.receive(ctx, q, sender),
            CQRS::Cmd(cmd) => {
                let sys = ctx.system.clone();
                let store = self.store.as_ref().unwrap().clone();
                let es = self.es.clone();
                ctx.system.exec.spawn_ok(async move {
                    debug!("processing command {:?}", cmd.clone());
                    es.lock().await.handle_command(cmd, sys, store).await;
                });
            }
            CQRS::Event(event) => {
                let sys = ctx.system.clone();
                let store = self.store.as_ref().unwrap().clone();
                let es = self.es.clone();
                ctx.system.exec.spawn_ok(async move {
                    debug!("processing event {:?}", event.clone());
                    es.lock().await.handle_event(event, sys, store).await;
                });
            }
        };
    }
}

impl<E: ES> Receive<Query> for Entity<E> {
    type Msg = CQRS<E::Cmd, E::Event>;
    fn receive(&mut self, _ctx: &Context<Self::Msg>, q: Query, sender: Sender) {
        match q {
            Query::One(id) => self.store.as_ref().unwrap().tell((id, Utc::now()), sender),
            Query::All => self.store.as_ref().unwrap().tell(..Utc::now(), sender),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CQRS<C, E> {
    Cmd(C),
    Event(E),
    Query(Query),
}
impl<C, E> From<Query> for CQRS<C, E> {
    fn from(q: Query) -> Self {
        CQRS::Query(q)
    }
}

#[derive(Clone, Debug)]
pub enum Query {
    All,
    One(EntityId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::{Op, TestCount};
    use crate::Event;
    use futures::executor::block_on;
    use riker_patterns::ask::ask;
    use std::time::Duration;

    #[derive(Default, Debug)]
    struct Test;
    #[async_trait]
    impl ES for Test {
        type Agg = TestCount;
        type Cmd = TestCmd;
        type Event = ();

        async fn handle_command<Ctx: Sys>(
            &self,
            cmd: Self::Cmd,
            cx: Ctx,
            store: StoreRef<Self::Agg>,
        ) {
            match cmd {
                TestCmd::Create42 => store.tell(Event::Create(TestCount::new(42)), None),
                TestCmd::Create99 => store.tell(Event::Create(TestCount::new(99)), None),
                TestCmd::Double(id) => {
                    let res: TestCount = ask(&cx, &store, (id, Utc::now())).await;
                    store.tell(Event::Update(res.id(), Op::Add(res.count)), None)
                }
            }
        }
    }
    #[derive(Clone, Debug)]
    enum TestCmd {
        Create42,
        Create99,
        Double(EntityId),
    }

    #[test]
    fn command_n_query() {
        let sys = ActorSystem::new().unwrap();
        let entity = sys.actor_of::<Entity<Test>>("counts").unwrap();

        entity.tell(CQRS::Cmd(TestCmd::Create42), None);
        entity.tell(CQRS::Cmd(TestCmd::Create99), None);

        std::thread::sleep(Duration::from_millis(20));
        let counts: Vec<TestCount> = block_on(ask(&sys, &entity, Query::All));

        assert_eq!(counts.len(), 2);
        let count42 = counts.iter().find(|c| c.count == 42);
        let count99 = counts.iter().find(|c| c.count == 99);
        assert!(count42.is_some());
        assert!(count99.is_some());

        let id = count42.unwrap().id();
        entity.tell(CQRS::Cmd(TestCmd::Double(id)), None);
        std::thread::sleep(Duration::from_millis(20));
        let result: TestCount = block_on(ask(&sys, &entity, Query::One(id)));
        assert_eq!(result.count, 84);
    }
}
