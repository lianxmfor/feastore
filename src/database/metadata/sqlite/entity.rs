use async_trait::async_trait;
use sqlx::FromRow;

use crate::database::error::Error;
use crate::database::metadata::{DBStore, Entity, GetEntityOpt};
use crate::database::Result;

use super::DB;

#[async_trait]
impl DBStore for DB {
    async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        let res = sqlx::query("INSERT INTO entity (name, description) VALUES (?, ?)")
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
        let rows_affected = sqlx::query("UPDATE entity SET description = ? WHERE id = ?")
                .bind(new_description)
                .bind(id)
                .execute(&self.pool)
                .await?
                .rows_affected();

        if rows_affected != 1 {
            Err(Error::ColumnNotFound("entity".to_owned(), id.to_string()))
        } else {
            Ok(())
        }
    }

    async fn get_entity(&self, opt: GetEntityOpt) -> Result<Option<Entity>> {
        let query = match opt {
            GetEntityOpt::Id(id) => {
                sqlx::query_as("SELECT * FROM entity WHERE id = ?")
                    .bind(id)
            },
            GetEntityOpt::Name(name) => {
                sqlx::query_as("SELECT * FROM entity WHERE name = ?")
                    .bind(name)
            }
        };

        Ok(query.fetch_optional(&self.pool).await?)
    }

    async fn list_entity(&self, ids: Vec<i64>) -> Result<Vec<Entity>> {
        let query_str = format!("SELECT * FROM entity WHERE id in (?{})", ", ?".repeat(ids.len()-1));
        let mut query = sqlx::query(&query_str);

        for id in ids {
            query = query.bind(id);
        }

        let res = query
                .fetch_all(&self.pool)
                .await?
                .iter()
                .map(|row| {
                    Entity::from_row(row)
                })
                .collect::<std::result::Result<Vec<Entity>, sqlx::Error>>();

        match res {
            Ok(res) => Ok(res),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{SqlitePool, pool};

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

    #[sqlx::test]
    fn get_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let id = db.create_entity("name", "description").await.unwrap();

        let entity = db.get_entity(GetEntityOpt::Id(id)).await.unwrap().unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let entity = db.get_entity(GetEntityOpt::Name("name".to_owned())).await.unwrap().unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let res = db.get_entity(GetEntityOpt::Name("not_exist".to_owned())).await.unwrap();
        assert!(res.is_none());

        let res = db.get_entity(GetEntityOpt::Id(id+1)).await.unwrap();
        assert!(res.is_none());
    }

    #[sqlx::test]
    fn update_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let id = db.create_entity("name", "description").await.unwrap();

        assert!(db.update_entity(id, "new_description").await.is_ok());

        let entity = db.get_entity(GetEntityOpt::Id(id)).await.unwrap().unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "new_description");

        assert_eq!(db.update_entity(id+1, "new_description").await.is_err_and(|e| match e{
            Error::ColumnNotFound(table, id) => table == "entity" && id == "2",
            _ => false,
        }), true);
    }

    #[sqlx::test]
    fn list_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entitys = db.list_entity(vec![1,2,3]).await.unwrap();
        assert_eq!(entitys.len(), 0);

        assert!(db.create_entity("name", "description").await.is_ok());
        let entitys = db.list_entity(vec![1,2,3]).await.unwrap();
        assert_eq!(entitys.len(), 1);

        assert!(db.create_entity("name2", "description").await.is_ok());
        let entitys = db.list_entity(vec![1,2,3]).await.unwrap();
        assert_eq!(entitys.len(), 2);

        assert!(db.create_entity("name3", "description").await.is_ok());
        let entitys = db.list_entity(vec![1,2,3]).await.unwrap();
        assert_eq!(entitys.len(), 3);

        assert!(db.create_entity("name4", "description").await.is_ok());
        let entitys = db.list_entity(vec![1,2,3]).await.unwrap();
        assert_eq!(entitys.len(), 3);
    }
}