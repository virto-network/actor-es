use crate::{Aggregate, EntityId};
use bson::{to_bson, Bson};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

/// Events as they are stored in the DB
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreEvent {
    pub id: EntityId,
    pub action: Action,
    pub when: DateTime<Utc>,
    pub who: Option<String>,
    pub data: Bson,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    Create,
    Update,
}

impl<T: Aggregate> From<Event<T>> for StoreEvent {
    fn from(event: Event<T>) -> Self {
        match event {
            Event::Create(entity, author) => StoreEvent {
                id: entity.id(),
                action: Action::Create,
                data: to_bson(&entity).unwrap(),
                when: Utc::now(),
                who: Some(author),
            },
            Event::Update(id, msg, author) => StoreEvent {
                id,
                action: Action::Update,
                data: to_bson(&msg).unwrap(),
                when: Utc::now(),
                who: Some(author),
            },
        }
    }
}
