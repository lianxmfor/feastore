use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: FeatureValueType,

    pub description: String,
    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,

    pub group_id: i64,
    #[serde(rename(serialize = "group-name", deserialize = "group-name"))]
    pub group_name: String,
}

#[derive(sqlx::FromRow, Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename = "Feature")]
pub struct ApplyFeature {
    pub name: String,
    #[serde(rename(serialize = "group-name", deserialize = "group-name"))]
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
    pub fn from(f: Feature, need_group_name: bool) -> Self {
        Self {
            name: f.name,
            value_type: f.value_type,
            description: f.description,
            group_name: if need_group_name {
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

#[derive(Serialize, Deserialize, sqlx::Type, Default, PartialEq, Debug, Clone, ValueEnum)]
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

impl From<&str> for FeatureValueType {
    fn from(s: &str) -> Self {
        match s {
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

#[cfg(test)]
mod tests {
    use super::FeatureValueType;
    #[test]
    fn convert_to_feature_value_type_work() {
        let test_cases = vec![
            // TODO: use fake crate to generate string
            ("string", FeatureValueType::StringType),
            ("int64", FeatureValueType::Int64),
            ("float64", FeatureValueType::Float64),
            ("bool", FeatureValueType::Bool),
            ("time", FeatureValueType::Time),
            ("bytes", FeatureValueType::Bytes),
            ("", FeatureValueType::Invalid),
        ];

        for (literal, value) in test_cases {
            let get_value: FeatureValueType = literal.into();
            assert_eq!(get_value, value);
        }
    }
}
