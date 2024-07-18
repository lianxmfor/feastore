use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

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

#[derive(Deserialize, sqlx::Type, Default, PartialEq, Debug, Clone)]
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

impl std::convert::From<&str> for FeatureValueType {
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

mod tests {
    use super::*;

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
