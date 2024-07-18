use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Default, Clone)]
pub struct Group {
    pub id: i64,
    pub name: String,
    pub category: GroupCategory,

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub entity_id: i64,
}

pub struct CreateGroupOpt {
    pub entity_id: i64,
    pub name: String,
    pub category: GroupCategory,
    pub description: String,
}

impl From<Group> for CreateGroupOpt {
    fn from(group: Group) -> Self {
        Self {
            entity_id: group.entity_id,
            name: group.name,
            category: group.category,
            description: group.description,
        }
    }
}

#[derive(sqlx::Type, Default, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupCategory {
    #[default]
    Batch,
    Stream,
}
