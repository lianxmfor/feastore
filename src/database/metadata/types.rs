use chrono::DateTime;
use chrono::Utc;

#[derive(sqlx::FromRow)]
pub struct Entity {
    pub id: i64,
    pub name: String,
    pub description: String,

    pub create_time: DateTime<Utc>,
    pub modify_time: DateTime<Utc>,
}

pub enum GetEntityOpt {
    Id(i64),
    Name(String),
}