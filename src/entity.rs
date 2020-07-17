use crate::{EntityId, Store, StoreMsg};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::executor::block_on;
use riker::actors::*;
use std::marker::PhantomData;

/// An Aggregate is the projected data of a series of events of an entity,
/// given an initial state update events are applied to it until it reaches the desired state.
pub trait Aggregate: Actor + Message + Default {
    type Update: Message;
    fn id(&self) -> EntityId;
    fn apply_update(&mut self, update: Self::Update);
}

type StoreRef<A> = ActorRef<StoreMsg<A>>;
pub trait Sys: TmpActorRefFactory + Run + Send + Sync {}
impl<T: TmpActorRefFactory + Run + Send + Sync> Sys for T {}

/// Implement this trait to allow your entity handle external commands
#[async_trait]
pub trait Command: Message {
    type Agg: Aggregate;
    async fn handle<Ctx: Sys>(self, cx: Ctx, store: StoreRef<Self::Agg>);
}

/// For entities that don't require handling commands
#[derive(Clone, Debug)]
pub struct NoCmd<A: Aggregate>(PhantomData<A>);
#[async_trait]
impl<A: Aggregate> Command for NoCmd<A> {
    type Agg = A;

    async fn handle<Ctx: Sys>(self, _cx: Ctx, _store: StoreRef<Self::Agg>) {}
}

/// Entities
pub struct Entity<C: Command> {
    store: Option<StoreRef<C::Agg>>,
}

impl<C: Command> Default for Entity<C> {
    fn default() -> Self {
        Entity { store: None }
    }
}

impl<C: Command> Actor for Entity<C> {
    type Msg = CQRS<C>;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.store = Some(ctx.actor_of::<Store<C::Agg>>(ctx.myself().name()).unwrap());
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            CQRS::Query(q) => self.receive(ctx, q, sender),
            CQRS::Cmd(cmd) => {
                let sys = ctx.system.clone();
                let store = self.store.as_ref().unwrap().clone();
                // TODO hot to run?
                let _ = ctx
                    .run(async move {
                        println!("processing command {:?}", cmd.clone());
                        cmd.handle(sys, store).await;
                    })
                    .unwrap();
            }
        };
    }
}

impl<C: Command> Receive<Query> for Entity<C> {
    type Msg = CQRS<C>;
    fn receive(&mut self, _ctx: &Context<Self::Msg>, q: Query, sender: Sender) {
        match q {
            Query::One(id) => self.store.as_ref().unwrap().tell((id, Utc::now()), sender),
            Query::All => self.store.as_ref().unwrap().tell(..Utc::now(), sender),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CQRS<C> {
    Query(Query),
    Cmd(C),
}
impl<C: Command> From<Query> for CQRS<C> {
    fn from(q: Query) -> Self {
        CQRS::Query(q)
    }
}
impl<C: Command> From<C> for CQRS<C> {
    fn from(c: C) -> Self {
        CQRS::Cmd(c)
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
    use riker_patterns::ask::ask;

    #[derive(Clone, Debug)]
    enum TestCmd {
        Create42,
        Create99,
        Double(EntityId),
    }
    #[async_trait]
    impl Command for TestCmd {
        type Agg = TestCount;

        async fn handle<Ctx: Sys>(self, cx: Ctx, store: StoreRef<Self::Agg>) {
            match self {
                TestCmd::Create42 => store.tell(Event::Create(TestCount::new(42)), None),
                TestCmd::Create99 => store.tell(Event::Create(TestCount::new(99)), None),
                TestCmd::Double(id) => {
                    let res: TestCount = ask(&cx, &store, (id, Utc::now())).await;
                    store.tell(Event::Update(res.id(), Op::Add(res.count)), None)
                }
            }
        }
    }

    #[tokio::test]
    async fn command_n_query() {
        let sys = ActorSystem::new().unwrap();
        let entity = sys.actor_of::<Entity<TestCmd>>("counts").unwrap();

        entity.tell(TestCmd::Create42, None);
        entity.tell(TestCmd::Create99, None);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let counts: Vec<TestCount> = ask(&sys, &entity, Query::All).await;
        println!("{:?}", counts);
        assert_eq!(counts[0].count, 42);
        assert_eq!(counts[1].count, 99);

        let id = counts[0].id();
        entity.tell(TestCmd::Double(id), None);
        let result: TestCount = ask(&sys, &entity, Query::One(id)).await;
        assert_eq!(result.count, 84);
    }
}
