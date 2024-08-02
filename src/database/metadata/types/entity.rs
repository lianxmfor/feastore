use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ApplyGroup, Group};

#[derive(sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub description: String,

    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    #[sqlx(skip)]
    pub groups: Option<Vec<Group>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag = "kind", rename = "Entity")]
pub struct ApplyEntity {
    pub name: String,
    pub description: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<ApplyGroup>>,
}

impl ApplyEntity {
    pub fn from(entity: Entity) -> Self {
        let groups = entity.groups.map(|groups| {
            groups
                .into_iter()
                .map(|group| ApplyGroup::from(group, false))
                .collect()
        });

        Self {
            name: entity.name,
            description: entity.description,
            groups,
        }
    }

    pub fn take_groups(&mut self) -> Option<Vec<ApplyGroup>> {
        match self.groups.take() {
            Some(groups) => Some(
                groups
                    .into_iter()
                    .enumerate()
                    .map(|(_, mut g)| {
                        g.entity_name = Some(self.name.clone());
                        g
                    })
                    .collect(),
            ),
            None => None,
        }
    }
}
