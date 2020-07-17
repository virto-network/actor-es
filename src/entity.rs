use crate::{EntityId, Store, StoreMsg};
use async_trait::async_trait;
use chrono::prelude::*;
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

/// Implement this trait to allow your entity handle external commands
#[async_trait]
pub trait Command: Message {
    type Agg: Aggregate;
    async fn handle(self, cx: &Context<CQRS<Self>>, store: &StoreRef<Self::Agg>);
}

/// For entities that don't require handling commands
#[derive(Clone, Debug)]
pub struct NoCmd<A: Aggregate>(PhantomData<A>);
#[async_trait]
impl<A: Aggregate> Command for NoCmd<A> {
    type Agg = A;
    async fn handle(self, _: &Context<CQRS<Self>>, _: &StoreRef<Self::Agg>) {}
}

/// Entities
struct Entity<C: Command> {
    store: Option<StoreRef<C::Agg>>,
    //_cmd: PhantomData<C>,
}

impl<C: Command> Default for Entity<C> {
    fn default() -> Self {
        Entity {
            store: None,
            //_cmd: PhantomData,
        }
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
            CQRS::Cmd(c) => {
                ctx.run(async move {
                    c.handle(ctx, self.store.as_ref().unwrap()).await;
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
            Query::One(id) => self.store.as_ref().unwrap().tell(id, sender),
            Query::All => self
                .store
                .as_ref()
                .unwrap()
                .tell(Utc.timestamp(0, 0)..Utc::now(), sender),
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
        async fn handle(self, cx: &Context<CQRS<Self>>, store: &StoreRef<Self::Agg>) {
            match self {
                TestCmd::Create42 => store.tell(
                    Event::Create(TestCount {
                        count: 42,
                        ..Default::default()
                    }),
                    None,
                ),
                TestCmd::Create99 => store.tell(
                    Event::Create(TestCount {
                        count: 99,
                        ..Default::default()
                    }),
                    None,
                ),
                TestCmd::Double(id) => {
                    let TestCount { count, id } = ask(&cx.system, &store, id).await;
                    store.tell(Event::Update(id, Op::Add(count)), None)
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

        let counts: Vec<TestCount> = ask(&sys, &entity, Query::All).await;
        assert_eq!(counts[0].count, 42);
        assert_eq!(counts[1].count, 99);

        let id = counts[0].id();
        entity.tell(TestCmd::Double(id), None);
        let result: TestCount = ask(&sys, &entity, Query::One(id)).await;
        assert_eq!(result.count, 84);
    }
}
