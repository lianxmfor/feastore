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

pub enum ListEntityOpt {
    /// return all rows from DB.
    All,
    /// return rows which id in the id list from DB.
    Ids(Vec<i64>),
}

#[derive(sqlx::Type, Default, PartialEq, Debug, Clone)]
pub enum Category {
    #[default]
    Batch,
    Stream,
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

pub enum GetGroupOpt {
    Id(i64),
    Name(String),
}

pub enum ListGroupOpt {
    /// return all rows from DB.
    All,
    /// return rows which id in the id list from DB.
    Ids(Vec<i64>),
}
