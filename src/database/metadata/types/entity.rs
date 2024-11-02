use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::database::metadata::types::RichGroup;

#[derive(sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub description: String,

    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag = "kind", rename = "Entity")]
pub struct RichEntity {
    pub name: String,
    pub description: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<RichGroup>>,
}

impl RichEntity {
    pub fn from(entity: Entity, groups: Option<Vec<RichGroup>>) -> Self {
        Self {
            name: entity.name,
            description: entity.description,
            groups: Self::remove_reluctant_fields(groups),
        }
    }

    pub fn take_groups(&mut self) -> Option<Vec<RichGroup>> {
        if let Some(mut groups) = self.groups.take() {
            groups.iter_mut().for_each(|g| {
                g.kind = Some("Group".to_string());
                g.entity_name = Some(self.name.clone())
            });
            Some(groups)
        } else {
            None
        }
    }

    fn remove_reluctant_fields(groups: Option<Vec<RichGroup>>) -> Option<Vec<RichGroup>> {
        let mut groups = groups;
        if let Some(mut groups) = groups.take() {
            groups.iter_mut().for_each(|g| {
                g.kind = None;
                g.entity_name = None;
            });
            Some(groups)
        } else {
            None
        }
    }
}
