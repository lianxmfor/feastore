use async_trait::async_trait;

use crate::database::error::Error;
use crate::database::metadata::{DBStore, Entity};
use crate::database::Result;


use super::DB;

#[async_trait]
impl DBStore for DB {
    async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        let res = sqlx::query("INSERT INTO ENTITY (name, description) VALUES (?, ?)")
                .bind(name)
                .bind(description)
                .execute(&self.pool)
                .await;

        match res {
            Err(sqlx::Error::Database(e)) => {
                if e.message() == format!("UNIQUE constraint failed: entity.name" ) {
                    Err(Error::ColumnAlreadyExist(name.to_string()))
                } else {
                    Err(e.into())
                }
            }
            _ => {
                Ok(res?.last_insert_rowid())
            }
        }
    }

    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        todo!()
    }

    async fn get_entity(&self, name: &str) -> Result<Option<Entity>> {
        todo!()
    }

    async fn list_entity(&self, ids: Vec<i64>) -> Result<Vec<Entity>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use crate::database::error::Error;

    use super::*;

    async fn prepare_db(pool: SqlitePool) -> DB {
        let db = DB::from_pool(pool);
        db.create_database().await;
        db
    } 

    #[sqlx::test]
    fn create_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let res: Result<i64> = db.create_entity("user", "description").await;
        assert!(res.is_ok() && res.unwrap() == 1);

        let res: Result<i64> = db.create_entity("user", "description").await;
        assert_eq!(match res.err() {
            Some(Error::ColumnAlreadyExist(name)) => name == "user",
            _ => false,
        }, true);
    }
}