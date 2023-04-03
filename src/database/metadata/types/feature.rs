use chrono::{DateTime, Utc};

use super::{FeatureValueType, Group};

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

pub enum GetFeatureOpt {
    Id(i64),
    Name(String),
}

pub enum ListFeatureOpt {
    /// return all rows from DB.
    All,
    /// return rows which id in the id list from DB.
    Ids(Vec<i64>),
}
