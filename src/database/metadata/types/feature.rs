use chrono::{DateTime, Utc};
use clap::builder::PossibleValue;
use clap::ValueEnum;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

use super::Group2;

#[derive(sqlx::FromRow, Clone)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    pub group_id: i64,
    pub value_type: ValueType,
    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    #[sqlx(skip)]
    pub group: Option<Group2>,
}

impl Serialize for Feature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(ref group) = self.group {
            let mut state = serializer.serialize_struct("Feature", 8)?;
            state.serialize_field("id", &self.id)?;
            state.serialize_field("name", &self.name)?;
            state.serialize_field("group", &group.name)?;
            state.serialize_field("category", &group.category)?;
            state.serialize_field("value-type", &self.value_type)?;
            state.serialize_field("description", &self.description)?;
            state.serialize_field("create_time", &self.create_time)?;
            state.serialize_field("modify_time", &self.modify_time)?;
            state.end()
        } else {
            let mut state = serializer.serialize_struct("Feature", 6)?;
            state.serialize_field("id", &self.id)?;
            state.serialize_field("name", &self.name)?;
            state.serialize_field("value-type", &self.value_type)?;
            state.serialize_field("description", &self.description)?;
            state.serialize_field("create_time", &self.create_time)?;
            state.serialize_field("modify_time", &self.modify_time)?;
            state.end()
        }
    }
}

impl Feature {
    pub fn full_name(&self) -> String {
        match self.group {
            Some(ref group) => format!("{}.{}", group.name, self.name),
            None => self.name.to_string(),
        }
    }

    pub fn group_name(&self) -> Option<String> {
        match self.group {
            Some(ref group) => Some(group.name.to_string()),
            None => None,
        }
    }
}

#[derive(sqlx::FromRow, Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct RichFeature {
    // just for printing, the value is always None or Some("Group")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "group", deserialize = "group"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: ValueType,

    pub description: String,
}

pub struct CreateFeatureOpt {
    pub group_id: i64,
    pub feature_name: String,
    pub description: String,
    pub value_type: ValueType,
}

impl RichFeature {
    pub fn from2(f: Feature) -> Self {
        Self {
            kind: Some("Feature".to_string()),
            name: f.name.to_string(),
            group_name: f.group_name(),
            value_type: f.value_type,
            description: f.description,
        }
    }

    pub fn full_name(&self) -> String {
        if let Some(ref group_name) = self.group_name {
            format!("{}.{}", group_name, self.name)
        } else {
            panic!("expected group name to be not nil");
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
pub enum ValueType {
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

impl ValueEnum for ValueType {
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
