use chrono::DateTime;
use chrono::Utc;

pub struct Entity {
    id: i64,
    name: String,
    description: String,

    create_time: DateTime<Utc>,
    modify_time: DateTime<Utc>,
}