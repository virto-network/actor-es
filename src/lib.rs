#[macro_use]
extern crate log;

use riker::actors::ChannelRef;
use std::fmt;
use uuid::Uuid;

pub use entity::{Entity, Model, Query, Result, CQRS, ES};
pub use entity_manager::{EntityName, Manager};
pub use store::{Commit, Store, StoreMsg, StoreRef};

pub type EventBus<T> = ChannelRef<Event<T>>;

mod entity;
mod entity_manager;
mod store;

/// Events are changes to the system generated by entities after processing
/// other events or external commands
#[derive(Clone, Debug)]
pub enum Event<T: Model> {
    Create(T),
    Change(EntityId, T::Change),
}
impl<T: Model> Event<T> {
    pub fn entity_id(&self) -> EntityId {
        match self {
            Event::Create(e) => e.id(),
            Event::Change(id, _) => *id,
        }
    }
}
impl<T: Model> From<(EntityId, T::Change)> for Event<T> {
    fn from((id, data): (EntityId, T::Change)) -> Self {
        Event::Change(id, data)
    }
}
impl<T: Model> From<T> for Event<T> {
    fn from(data: T) -> Self {
        Event::Create(data)
    }
}

/// Uniquely idenfies an entity
#[derive(Clone, Debug, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityId(Uuid);
impl EntityId {
    pub fn new() -> Self {
        Default::default()
    }
}
impl From<String> for EntityId {
    fn from(id: String) -> Self {
        id.as_str().into()
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
