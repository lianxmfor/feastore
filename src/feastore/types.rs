#![allow(dead_code)]

use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use std::time::Duration;

use crate::database::metadata::types::Category;

pub struct ApplyOpt<R: std::io::Read> {
    pub r: R,
}

#[derive(Debug, PartialEq)]
pub struct ApplyStage {
    pub new_entities: Vec<Entity>,
    pub new_groups: Vec<Group>,
    pub new_features: Vec<Feature>,
}

impl ApplyStage {
    pub fn new() -> Self {
        Self {
            new_entities: Vec::new(),
            new_groups: Vec::new(),
            new_features: Vec::new(),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Feature {
    pub kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "group-name", deserialize = "group-name"))]
    pub group_name: Option<String>,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: String,
    pub description: String,
}

#[serde_as]
#[derive(Deserialize, Debug, PartialEq)]
pub struct Group {
    pub kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "entity-name", deserialize = "entity-name"))]
    pub entity_name: Option<String>,
    pub category: Category,
    #[serde(rename(serialize = "snapshot-interval", deserialize = "snapshot-interval"))]
    #[serde_as(as = "Option<DurationSeconds>")]
    pub snapshot_interval: Option<Duration>,
    pub description: String,

    pub features: Option<Vec<Feature>>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Entity {
    pub kind: Option<String>,
    pub name: String,
    pub description: String,

    pub groups: Option<Vec<Group>>,
}
