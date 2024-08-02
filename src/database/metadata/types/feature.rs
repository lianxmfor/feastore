use chrono::{DateTime, Utc};
use clap::builder::PossibleValue;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::GroupCategory;

#[derive(sqlx::FromRow, Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    #[serde(rename(serialize = "group", deserialize = "group"))]
    pub group_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<GroupCategory>,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: FeatureValueType,
    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    #[serde(skip)]
    pub group_id: i64,
}

#[derive(sqlx::FromRow, Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyFeature {
    // just for printing, the value is always None or Some("Group")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "group", deserialize = "group"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: FeatureValueType,

    pub description: String,
}

pub struct CreateFeatureOpt {
    pub group_id: i64,
    pub feature_name: String,
    pub description: String,
    pub value_type: FeatureValueType,
}

impl ApplyFeature {
    pub fn from(f: Feature, full_information: bool) -> Self {
        Self {
            kind: if full_information {
                Some("Feature".to_string())
            } else {
                None
            },
            name: f.name,
            value_type: f.value_type,
            description: f.description,
            group_name: if full_information {
                Some(f.group_name)
            } else {
                None
            },
        }
    }
}

impl From<Feature> for CreateFeatureOpt {
    fn from(f: Feature) -> Self {
        Self {
            group_id: f.group_id,
            feature_name: f.name,
            description: f.description,
            value_type: f.value_type,
        }
    }
}

#[derive(Serialize, Deserialize, sqlx::Type, Default, PartialEq, Debug, Clone)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
pub enum FeatureValueType {
    #[default]
    #[serde(rename = "string")]
    StringType,
    Int64,
    Float64,
    Bool,
    Time,
    Bytes,
    Invalid,
}

impl ValueEnum for FeatureValueType {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::StringType,
            Self::Int64,
            Self::Float64,
            Self::Bool,
            Self::Time,
            Self::Bytes,
        ]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Self::StringType => Some(PossibleValue::new("string")),
            Self::Int64 => Some(PossibleValue::new("int64")),
            Self::Float64 => Some(PossibleValue::new("float64")),
            Self::Bool => Some(PossibleValue::new("bool")),
            Self::Time => Some(PossibleValue::new("time")),
            Self::Bytes => Some(PossibleValue::new("bytes")),
            Self::Invalid => None,
        }
    }
}
