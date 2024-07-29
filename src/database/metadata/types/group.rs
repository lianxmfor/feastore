use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationSeconds;

use std::time::Duration;

use super::ApplyFeature;
use super::Feature;

#[derive(sqlx::FromRow, Default, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: i64,
    pub name: String,
    pub category: GroupCategory,
    pub snapshot_interval: Option<i32>, // FIXME: use chrono::Duration repleace i32

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub entity_id: i64,
    pub entity_name: String,

    #[sqlx(skip)]
    pub features: Option<Vec<Feature>>,
}

#[serde_as]
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "Group")]
pub struct ApplyGroup {
    pub name: String,
    #[serde(rename(serialize = "entity-name", deserialize = "entity-name"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_name: Option<String>,
    pub category: GroupCategory,
    #[serde(rename(serialize = "snapshot-interval", deserialize = "snapshot-interval"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    // TODO: instead serde_as with github.com/jsofhgujoifdjisiojpihhvjpkojjkp'jkbjjj'jojbkhvkbjljbjkbkj;kbjljlkj;kbjb nkkjbhvkhihugkvhhiugvgguhcgvgvhhgihhuhjihjlan-airoldie/humantime-serde
    #[serde_as(as = "Option<DurationSeconds>")]
    pub snapshot_interval: Option<Duration>,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<ApplyFeature>>,
}

impl ApplyGroup {
    pub fn from(group: Group, need_group_name: bool) -> Self {
        let features = group.features.map(|features| {
            features
                .into_iter()
                .map(|f| ApplyFeature::from(f, false))
                .collect()
        });

        Self {
            name: group.name,
            entity_name: if need_group_name {
                Some(group.entity_name)
            } else {
                None
            },
            category: group.category,
            snapshot_interval: group
                .snapshot_interval
                .map(|i| Duration::from_secs(i as u64)),
            description: group.description,
            features,
        }
    }

    pub fn take_features(&mut self) -> Option<Vec<ApplyFeature>> {
        match self.features.take() {
            Some(features) => Some(
                features
                    .into_iter()
                    .enumerate()
                    .map(|(_, mut f)| {
                        f.group_name = Some(self.name.clone());
                        f
                    })
                    .collect(),
            ),
            None => None,
        }
    }
}

pub struct CreateGroupOpt {
    pub entity_id: i64,
    pub name: String,
    pub category: GroupCategory,
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
pub enum GroupCategory {
    #[default]
    Batch,
    Stream,
}
