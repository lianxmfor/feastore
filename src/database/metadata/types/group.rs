use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::{Entity, RichFeature};

#[derive(sqlx::FromRow, Default, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: i64,
    pub name: String,
    #[serde(rename(serialize = "entity", deserialize = "entity"))]
    pub entity_name: String,
    pub category: Category,

    #[serde(rename(serialize = "snapshot-interval", deserialize = "snapshot-interval"))]
    pub snapshot_interval: Option<i32>, // FIXME: use chrono::Duration repleace i32

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub entity_id: i64,
}

#[derive(sqlx::FromRow, Default, Clone, Serialize, Deserialize)]
pub struct Group2 {
    pub id: i64,
    pub name: String,
    pub entity_id: i64,
    pub category: Category,
    pub snapshot_interval: Option<i32>, // FIXME: use chrono::Duration repleace i32
    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    #[sqlx(skip)]
    pub entity: Option<Entity>,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default)]
pub struct RichGroup {
    // just for printing, the value is always None or Some("Group")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,

    #[serde(
        rename(serialize = "entity", deserialize = "entity"),
        skip_serializing_if = "Option::is_none"
    )]
    pub entity_name: Option<String>,

    pub category: Category,

    #[serde(
        skip_serializing_if = "Option::is_none",
        rename(serialize = "snapshot-interval", deserialize = "snapshot-interval"),
        default
    )]
    pub snapshot_interval: Option<i32>, // FIXME: use chrono::Duration repleace i32

    pub description: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<RichFeature>>,
}

impl RichGroup {
    pub fn from(group: Group, features: Option<Vec<RichFeature>>) -> Self {
        Self {
            kind: Some("Group".to_string()),
            name: group.name,
            entity_name: Some(group.entity_name),
            category: group.category,
            snapshot_interval: group.snapshot_interval,
            description: group.description,
            features: Self::remove_reluctant_fields(features),
        }
    }

    fn remove_reluctant_fields(features: Option<Vec<RichFeature>>) -> Option<Vec<RichFeature>> {
        let mut features = features;
        if let Some(mut features) = features.take() {
            features.iter_mut().for_each(|f| {
                f.group_name = None;
                f.kind = None;
            });
            Some(features)
        } else {
            None
        }
    }

    pub fn take_features(&mut self) -> Option<Vec<RichFeature>> {
        if let Some(mut features) = self.features.take() {
            features.iter_mut().for_each(|f| {
                f.kind = Some("Feature".to_string());
                f.group_name = Some(self.name.to_string());
            });
            Some(features)
        } else {
            None
        }
    }
}

pub struct CreateGroupOpt {
    pub entity_id: i64,
    pub name: String,
    pub category: Category,
    pub snapshot_interval: Option<i32>,
    pub description: String,
}

impl From<Group> for CreateGroupOpt {
    fn from(group: Group) -> Self {
        Self {
            entity_id: group.entity_id,
            name: group.name,
            category: group.category,
            snapshot_interval: group.snapshot_interval,
            description: group.description,
        }
    }
}

#[derive(sqlx::Type, Default, PartialEq, Debug, Clone, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    #[default]
    Batch,
    Stream,
}
