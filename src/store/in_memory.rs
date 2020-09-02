use super::{Commit, CommitError, CommitResult, CommitStore, DynCommitStore, Event};
use crate::{EntityId, Model};
use async_trait::async_trait;
use futures::lock::Mutex;
use futures::stream::{self, BoxStream, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemStore<M: Model>(Mutex<HashMap<EntityId, (Commit<M>, Vec<Commit<M>>)>>);

impl<M: Model> MemStore<M> {
    pub fn new() -> DynCommitStore<M> {
        Arc::new(Box::new(MemStore(Mutex::new(HashMap::new()))))
    }
}

#[async_trait]
impl<M: Model> CommitStore<M> for MemStore<M> {
    fn keys(&self) -> BoxStream<CommitResult<EntityId>> {
        stream::once(async {
            let keys = self
                .0
                .lock()
                .await
                .keys()
                .map(|k| Ok(*k))
                .collect::<Vec<_>>();
            stream::iter(keys)
        })
        .flatten()
        .boxed()
    }

    fn commit_list(&self, id: EntityId) -> BoxStream<CommitResult<&Commit<M>>> {
        stream::once(async {
            let (initial_commit, changes) =
                self.0.lock().await.get(&id).ok_or(CommitError::NotFound)?;
            let changes = stream::iter(changes);
            Ok(initial_commit)
        })
        .boxed()
    }

    async fn commit(&self, c: Commit<M>) -> Result<(), CommitError> {
        let id = c.event.entity_id();
        let mut entities = self.0.lock().await;
        match c.event {
            Event::Create(_) => {
                entities.insert(id, (c, vec![]));
            }
            Event::Change(_, _) => {
                let (_, updates) = entities.get_mut(&id).ok_or(CommitError::CantChange)?;
                updates.push(c);
            }
        }
        Ok(())
    }
}
