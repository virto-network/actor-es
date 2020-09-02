use super::{Commit, CommitError, CommitResult, CommitStore, Event};
use crate::{EntityId, Model};
use async_trait::async_trait;
use futures::lock::Mutex;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemStore<M: Model>(Arc<Mutex<HashMap<EntityId, (Commit<M>, Vec<Commit<M>>)>>>);

impl<M: Model> MemStore<M> {
    pub fn new() -> Self {
        MemStore(Arc::new(Mutex::new(HashMap::new())))
    }
}

#[async_trait]
impl<M: Model> CommitStore<M> for MemStore<M> {
    fn keys(&self) -> BoxStream<CommitResult<EntityId>> {
        let map = self.0.clone();
        stream::once(async move {
            let keys = map.lock().await.keys().map(|k| Ok(*k)).collect::<Vec<_>>();
            stream::iter(keys)
        })
        .flatten()
        .boxed()
    }

    fn change_list(&self, id: EntityId) -> BoxStream<CommitResult<Commit<M>>> {
        let map = self.0.clone();
        stream::once(async move {
            let map = map.lock().await;
            let (initial_commit, changes) = map.get(&id).ok_or(CommitError::NotFound)?;
            let changes = changes.to_owned();
            let commits =
                iter::once(Ok(initial_commit.clone())).chain(changes.into_iter().map(Result::Ok));
            Ok(stream::iter(commits))
        })
        .try_flatten()
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

impl<M: Model> Clone for MemStore<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
