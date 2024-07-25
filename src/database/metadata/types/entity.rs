use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub description: String,

    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,
}
