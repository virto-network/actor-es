use crate::EntityId;
use riker::actors::{Actor, Message};

pub trait Aggregate: Actor + Message + Default {
    type Update: Message;
    fn id(&self) -> EntityId;
    fn apply_update(&mut self, update: Self::Update);
}
