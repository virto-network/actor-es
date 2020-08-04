use crate::{Commit, EntityId, Store, StoreMsg};
use async_trait::async_trait;
use chrono::prelude::*;
use futures::lock::Mutex;
use riker::actors::*;
use std::fmt;
use std::sync::Arc;

/// An Aggregate is the projected data of a series of events of an entity,
/// given an initial state update events are applied to it until it reaches the desired state.
pub trait Aggregate: Message {
    type Update: Message;
    fn id(&self) -> EntityId;
    fn apply_update(&mut self, update: Self::Update);
}

pub type StoreRef<A> = ActorRef<StoreMsg<A>>;
pub trait Sys: TmpActorRefFactory + Run + Send + Sync {}
impl<T: TmpActorRefFactory + Run + Send + Sync> Sys for T {}

/// Implement this trait to allow your entity handle external commands
#[async_trait]
pub trait ES: fmt::Debug + Send + Sync + 'static {
    type Args: ActorArgs;
    type Agg: Aggregate;
    type Cmd: Message;
    type Error: fmt::Debug;

    fn new(cx: &Context<CQRS<Self::Cmd>>, args: Self::Args) -> Self;

    async fn handle_command(&mut self, _cmd: Self::Cmd) -> Result<Commit<Self::Agg>, Self::Error>;
}

/// Entity is an actor that dispatches commands and manages aggregates that are being queried
pub struct Entity<E: ES> {
    store: Option<StoreRef<E::Agg>>,
    args: E::Args,
    es: Option<Arc<Mutex<E>>>,
}

impl<E, Args> ActorFactoryArgs<Args> for Entity<E>
where
    Args: ActorArgs,
    E: ES<Args = Args>,
{
    fn create_args(args: Args) -> Self {
        Entity {
            store: None,
            es: None,
            args,
        }
    }
}

impl<E: ES> Actor for Entity<E> {
    type Msg = CQRS<E::Cmd>;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        self.es = Some(Arc::new(Mutex::new(E::new(ctx, self.args.clone()))));
        self.store = Some(ctx.actor_of::<Store<E::Agg>>(ctx.myself().name()).unwrap());
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            CQRS::Query(q) => self.receive(ctx, q, sender),
            CQRS::Cmd(cmd) => {
                let store = self.store.as_ref().unwrap().clone();
                let es = self.es.clone();
                ctx.system.exec.spawn_ok(async move {
                    let cmd_dbg = format!("{:?}", cmd);
                    debug!("processing command {}", cmd_dbg);
                    let commit = es
                        .unwrap()
                        .lock()
                        .await
                        .handle_command(cmd)
                        .await
                        .expect("Failed handling command");
                    let entity_id = commit.entity_id();
                    store.tell(commit, None);

                    if let Some(sender) = sender {
                        let _ = sender
                            .try_tell(entity_id, None)
                            .map_err(|_| warn!("Couldn't signal completion of {}", cmd_dbg));
                    }
                });
            }
        };
    }
}

impl<E: ES> Receive<Query> for Entity<E> {
    type Msg = CQRS<E::Cmd>;
    fn receive(&mut self, _ctx: &Context<Self::Msg>, q: Query, sender: Sender) {
        match q {
            Query::One(id) => self.store.as_ref().unwrap().tell((id, Utc::now()), sender),
            Query::All => self.store.as_ref().unwrap().tell(..Utc::now(), sender),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CQRS<C> {
    Cmd(C),
    Query(Query),
}
impl<C> From<Query> for CQRS<C> {
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

    #[derive(Debug)]
    struct Test {
        sys: ActorSystem,
        entity: ActorRef<CQRS<<Self as ES>::Cmd>>,
        _foo: String,
    }
    #[async_trait]
    impl ES for Test {
        type Args = (u8, String);
        type Agg = TestCount;
        type Cmd = TestCmd;
        type Error = String;

        fn new(cx: &Context<CQRS<Self::Cmd>>, (num, txt): Self::Args) -> Self {
            Test {
                _foo: format!("{}{}", num, txt),
                sys: cx.system.clone(),
                entity: cx.myself(),
            }
        }

        async fn handle_command(
            &mut self,
            cmd: Self::Cmd,
        ) -> Result<Commit<Self::Agg>, Self::Error> {
            let event = match cmd {
                TestCmd::Create42 => Event::Create(TestCount::new(42)),
                TestCmd::Create99 => Event::Create(TestCount::new(99)),
                TestCmd::Double(id) => {
                    let res: Option<TestCount> = ask(&self.sys, &self.entity, Query::One(id)).await;
                    let res = res.ok_or("Not found")?;
                    Event::Update(res.id(), Op::Add(res.count))
                }
            };
            Ok(event.into())
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
        let entity = sys
            .actor_of_args::<Entity<Test>, _>("counts", (42, "42".into()))
            .unwrap();

        let _: EntityId = block_on(ask(&sys, &entity, CQRS::Cmd(TestCmd::Create42)));
        let _: EntityId = block_on(ask(&sys, &entity, CQRS::Cmd(TestCmd::Create99)));
        let counts: Vec<TestCount> = block_on(ask(&sys, &entity, Query::All));

        assert_eq!(counts.len(), 2);
        let count42 = counts.iter().find(|c| c.count == 42);
        let count99 = counts.iter().find(|c| c.count == 99);
        assert!(count42.is_some());
        assert!(count99.is_some());

        let id = count42.unwrap().id();
        let _: EntityId = block_on(ask(&sys, &entity, CQRS::Cmd(TestCmd::Double(id))));
        let result: Option<TestCount> = block_on(ask(&sys, &entity, Query::One(id)));
        assert_eq!(result.unwrap().count, 84);
    }
}
