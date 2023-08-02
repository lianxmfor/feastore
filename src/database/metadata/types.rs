#![allow(dead_code)]
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub description: String,

    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,
}

#[derive(sqlx::Type, Default, PartialEq, Debug, Clone)]
pub enum FeatureValueType {
    #[default]
    StringType,
    Int64,
    Float64,
    Bool,
    Time,
    Bytes,
    Invalid,
}

#[derive(sqlx::FromRow, Default, Clone)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    pub value_type: FeatureValueType,

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub group_id: i64,
}

pub struct CreateFeatureOpt {
    pub group_id: i64,
    pub feature_name: String,
    pub description: String,
    pub value_type: FeatureValueType,
}

impl std::convert::From<Feature> for CreateFeatureOpt {
    fn from(f: Feature) -> Self {
        Self {
            group_id: f.group_id,
            feature_name: f.name,
            description: f.description,
            value_type: f.value_type,
        }
    }
}

impl std::convert::From<String> for FeatureValueType {
    fn from(v: String) -> Self {
        match v.as_str() {
            "string" => FeatureValueType::StringType,
            "int64" => FeatureValueType::Int64,
            "float64" => FeatureValueType::Float64,
            "bool" => FeatureValueType::Bool,
            "time" => FeatureValueType::Time,
            "bytes" => FeatureValueType::Bytes,
            _ => FeatureValueType::Invalid,
        }
    }
}

#[derive(sqlx::FromRow, Default, Clone)]
pub struct Group {
    pub id: i64,
    pub name: String,
    pub category: Category,

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub entity_id: i64,
}

pub struct CreateGroupOpt {
    pub entity_id: i64,
    pub name: String,
    pub category: Category,
    pub description: String,
}

impl From<Group> for CreateGroupOpt {
    fn from(g: Group) -> Self {
        Self {
            entity_id: g.entity_id,
            name: g.name,
            category: g.category,
            description: g.description,
        }
    }
}

#[derive(sqlx::Type, Default, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    #[default]
    Batch,
    Stream,
}

pub enum GetOpt {
    ID(i64),
    Name(String),
}

pub enum ListOpt {
    All,
    IDs(Vec<i64>),
}
