use async_trait::async_trait;
use sqlx::FromRow;

use crate::database::error::Error;
use crate::database::metadata::types::{
    CreateFeatureOpt, CreateGroupOpt, Entity, Feature, GetEntityOpt, GetFeatureOpt, GetGroupOpt,
    Group, ListEntityOpt, ListFeatureOpt, ListGroupOpt,
};
use crate::database::metadata::DBStore;
use crate::database::Result;


pub mod implements;
pub mod schema;


use sqlx::SqlitePool;

use schema::{META_TABLE_SCHEMAS, META_VIEW_SCHEMAS};

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    pub async fn from(db_file: &str) -> DB {
        let pool = SqlitePool::connect(format!("sqlite://{}", db_file).as_str())
            .await
            .unwrap();

        DB { pool }
    }

    fn from_pool(pool: SqlitePool) -> DB {
        DB { pool }
    }

    async fn create_database(&self) {
        for table_schema in META_TABLE_SCHEMAS.values() {
            sqlx::query(&table_schema)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        for view_schema in META_VIEW_SCHEMAS.values() {
            sqlx::query(&view_schema).execute(&self.pool).await.unwrap();
        }

        for table in META_TABLE_SCHEMAS.keys() {
            //TODO: use template engine instead {}
            let trigger = format!(
                r"
                    CREATE TRIGGER {}_update_modify_time
                    AFTER UPDATE ON {}
                    BEGIN
                        update {} SET modify_time = datetime('now') WHERE id = NEW.id;
                    END;
            ",
                table, table, table
            );
            sqlx::query(&trigger).execute(&self.pool).await.unwrap();
        }
    }
}

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
                if e.message() == format!("UNIQUE constraint failed: entity.name") {
                    Err(Error::ColumnAlreadyExist(name.to_string()))
                } else {
                    Err(e.into())
                }
            }
            _ => Ok(res?.last_insert_rowid()),
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
            GetEntityOpt::Id(id) => sqlx::query_as("SELECT * FROM entity WHERE id = ?").bind(id),
            GetEntityOpt::Name(name) => {
                sqlx::query_as("SELECT * FROM entity WHERE name = ?").bind(name)
            }
        };

        Ok(query.fetch_optional(&self.pool).await?)
    }

    async fn list_entity(&self, opt: ListEntityOpt) -> Result<Vec<Entity>> {
        let mut query_str = "SELECT * FROM entity".to_owned();

        let query = match opt {
            ListEntityOpt::All => sqlx::query(&query_str),
            ListEntityOpt::Ids(ids) => {
                if ids.len() == 0 {
                    return Ok(Vec::new());
                }

                query_str = format!("{query_str} WHERE id in (?{})", ", ?".repeat(ids.len() - 1));
                let mut query = sqlx::query(&query_str);
                for id in ids {
                    query = query.bind(id);
                }
                query
            }
        };

        let res = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(|row| Entity::from_row(row))
            .collect::<std::result::Result<Vec<Entity>, sqlx::Error>>();

        res.or_else(|e| Err(e.into()))
    }

    async fn create_group(&self, group: CreateGroupOpt) -> Result<i64> {
        let res = sqlx::query("INSERT INTO feature_group (name, category, description, entity_id) VALUES (?, ?, ?, ?)")
            .bind(&group.name)
            .bind(group.category)
            .bind(group.description)
            .bind(group.entity_id)
            .execute(&self.pool)
            .await;

        match res {
            Err(sqlx::Error::Database(e)) => {
                if e.message() == format!("UNIQUE constraint failed: feature_group.name") {
                    Err(Error::ColumnAlreadyExist(group.name))
                } else {
                    Err(e.into())
                }
            }
            _ => Ok(res?.last_insert_rowid()),
        }
    }

    async fn update_group(&self, id: i64, new_description: &str) -> Result<()> {
        let rows_affected = sqlx::query("UPDATE feature_group SET description = ? WHERE id = ?")
            .bind(new_description)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();

        if rows_affected != 1 {
            Err(Error::ColumnNotFound(
                "feature_group".to_owned(),
                id.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    async fn get_group(&self, opt: GetGroupOpt) -> Result<Option<Group>> {
        let query = match opt {
            GetGroupOpt::Id(id) => {
                sqlx::query_as("SELECT * FROM feature_group WHERE id = ?").bind(id)
            }
            GetGroupOpt::Name(name) => {
                sqlx::query_as("SELECT * FROM feature_group WHERE name = ?").bind(name)
            }
        };

        Ok(query.fetch_optional(&self.pool).await?)
    }

    async fn list_group(&self, opt: ListGroupOpt) -> Result<Vec<Group>> {
        let mut query_str = "SELECT * FROM feature_group".to_owned();

        let query = match opt {
            ListGroupOpt::All => sqlx::query(&query_str),
            ListGroupOpt::Ids(ids) => {
                if ids.len() == 0 {
                    return Ok(Vec::new());
                }

                query_str = format!("{query_str} WHERE id in (?{})", ", ?".repeat(ids.len() - 1));
                let mut query = sqlx::query(&query_str);
                for id in ids {
                    query = query.bind(id);
                }
                query
            }
        };

        let res = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(|row| Group::from_row(row))
            .collect::<std::result::Result<Vec<Group>, sqlx::Error>>();

        res.or_else(|e| Err(e.into()))
    }

    async fn create_feature(&self, opt: CreateFeatureOpt) -> Result<i64> {
        let res = sqlx::query(
            "INSERT INTO feature (group_id, name, value_type, description) VALUES (?, ?, ?, ?)",
        )
        .bind(opt.group_id)
        .bind(&opt.feature_name)
        .bind(opt.value_type)
        .bind(opt.description)
        .execute(&self.pool)
        .await;

        match res {
            Err(sqlx::Error::Database(e)) => {
                if e.message()
                    == format!("UNIQUE constraint failed: feature.group_id, feature.name")
                {
                    Err(Error::ColumnAlreadyExist(opt.feature_name))
                } else {
                    println!("{}", e.message());
                    Err(e.into())
                }
            }
            _ => Ok(res?.last_insert_rowid()),
        }
    }

    async fn update_feature(&self, id: i64, new_description: &str) -> Result<()> {
        let rows_affected = sqlx::query("UPDATE feature  SET description = ? WHERE id = ?")
            .bind(new_description)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();

        if rows_affected != 1 {
            Err(Error::ColumnNotFound("feature".to_owned(), id.to_string()))
        } else {
            Ok(())
        }
    }

    async fn get_feature(&self, opt: GetFeatureOpt) -> Result<Option<Feature>> {
        let query = match opt {
            GetFeatureOpt::Id(id) => sqlx::query_as("SELECT * FROM feature WHERE id = ?").bind(id),
            GetFeatureOpt::Name(name) => {
                sqlx::query_as("SELECT * FROM feature WHERE name = ?").bind(name)
            }
        };

        Ok(query.fetch_optional(&self.pool).await?)
    }

    async fn list_feature(&self, opt: ListFeatureOpt) -> Result<Vec<Feature>> {
        let mut query_str = "SELECT * FROM feature".to_owned();

        let query = match opt {
            ListFeatureOpt::All => sqlx::query(&query_str),
            ListFeatureOpt::Ids(ids) => {
                if ids.len() == 0 {
                    return Ok(Vec::new());
                }

                query_str = format!("{query_str} WHERE id in (?{})", ", ?".repeat(ids.len() - 1));

                let mut query = sqlx::query(&query_str);
                for id in ids {
                    query = query.bind(id);
                }
                query
            }
        };

        let res = query
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(|row| Feature::from_row(row))
            .collect::<std::result::Result<Vec<Feature>, sqlx::Error>>();

        res.or_else(|e| Err(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use crate::database::metadata::types::FeatureValueType;
    use crate::database::{error::Error, metadata::types::Category};

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
        assert_eq!(
            match res.err() {
                Some(Error::ColumnAlreadyExist(name)) => name == "user",
                _ => false,
            },
            true
        );
    }

    #[sqlx::test]
    fn get_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let id = db.create_entity("name", "description").await.unwrap();

        let entity = db.get_entity(GetEntityOpt::Id(id)).await.unwrap().unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let entity = db
            .get_entity(GetEntityOpt::Name("name".to_owned()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let res = db
            .get_entity(GetEntityOpt::Name("not_exist".to_owned()))
            .await
            .unwrap();
        assert!(res.is_none());

        let res = db.get_entity(GetEntityOpt::Id(id + 1)).await.unwrap();
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

        assert_eq!(
            db.update_entity(id + 1, "new_description")
                .await
                .is_err_and(|e| match e {
                    Error::ColumnNotFound(table, id) => table == "entity" && id == "2",
                    _ => false,
                }),
            true
        );
    }

    #[sqlx::test]
    fn list_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entitys = db.list_entity(ListEntityOpt::All).await.unwrap();
        assert_eq!(entitys.len(), 0);

        assert!(db.create_entity("name", "description").await.is_ok());
        let entitys = db.list_entity(ListEntityOpt::All).await.unwrap();
        assert_eq!(entitys.len(), 1);

        assert!(db.create_entity("name2", "description").await.is_ok());
        let entitys = db.list_entity(ListEntityOpt::All).await.unwrap();
        assert_eq!(entitys.len(), 2);

        assert!(db.create_entity("name3", "description").await.is_ok());
        let entitys = db
            .list_entity(ListEntityOpt::Ids(vec![1, 2]))
            .await
            .unwrap();
        assert_eq!(entitys.len(), 2);

        let entitys = db
            .list_entity(ListEntityOpt::Ids(vec![1, 2, 3, 4]))
            .await
            .unwrap();
        assert_eq!(entitys.len(), 3);

        let entitys = db
            .list_entity(ListEntityOpt::Ids(Vec::new()))
            .await
            .unwrap();
        assert_eq!(entitys.len(), 0);
    }

    #[sqlx::test]
    fn crate_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = db.create_entity("name", "description").await.unwrap();

        let res = db
            .create_group(CreateGroupOpt {
                name: "name".to_owned(),
                category: Category::Batch,
                description: "description".to_owned(),
                entity_id,
            })
            .await;
        assert!(res.is_ok_and(|id| id == 1));

        let res = db
            .create_group(CreateGroupOpt {
                name: "name1".to_owned(),
                category: Category::Stream,
                description: "description".to_owned(),
                entity_id,
            })
            .await;
        assert!(res.is_ok_and(|id| id == 2));

        let res = db
            .create_group(CreateGroupOpt {
                name: "name".to_owned(),
                category: Category::Batch,
                description: "description".to_owned(),
                entity_id,
            })
            .await;

        assert!(res.is_err_and(|e| match e {
            Error::ColumnAlreadyExist(name) => name == "name",
            _ => false,
        }));
    }

    #[sqlx::test]
    fn get_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;
        let entity_id = db.create_entity("name", "description").await.unwrap();

        let group_zero = Group {
            id: 1,
            name: "name".to_owned(),
            category: Category::Batch,
            description: "description".to_owned(),
            entity_id,
            ..Default::default()
        };
        let id = db.create_group(group_zero.clone().into()).await.unwrap();

        let group = db.get_group(GetGroupOpt::Id(id)).await.unwrap().unwrap();
        assert_eq_of_group(&group, &group_zero);

        let group = db
            .get_group(GetGroupOpt::Name(group_zero.name.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq_of_group(&group, &group_zero);

        let res = db.get_group(GetGroupOpt::Id(id + 1)).await;
        assert!(res.is_ok_and(|res| res.is_none()));

        let res = db
            .get_group(GetGroupOpt::Name("not_exist".to_owned()))
            .await;
        assert!(res.is_ok_and(|res| res.is_none()));
    }

    fn assert_eq_of_group(left: &Group, right: &Group) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.name, right.name);
        assert_eq!(left.category, right.category);
        assert_eq!(left.description, right.description);
    }

    #[sqlx::test]
    fn update_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(db.update_group(1, "new_description").await.is_err_and(|e| {
            match e {
                Error::ColumnNotFound(table, id) => table == "feature_group" && id == "1",
                _ => false,
            }
        }));

        let entity_id = db.create_entity("name", "description").await.unwrap();
        let origin_group = Group {
            id: 1,
            name: "name".to_owned(),
            category: Category::Batch,
            description: "description".to_owned(),
            entity_id,
            ..Default::default()
        };
        let group_id = db.create_group(origin_group.clone().into()).await.unwrap();
        assert!(db.update_group(group_id, "new_description").await.is_ok());
        let group = db.get_group(GetGroupOpt::Id(group_id)).await.unwrap();
        assert!(group.is_some_and(|g| {
            g.id == origin_group.id
                && g.entity_id == origin_group.entity_id
                && g.category == origin_group.category
                && g.name == origin_group.name
                && g.description == "new_description"
        }));
    }

    #[sqlx::test]
    fn list_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = db.create_entity("entity", "description").await.unwrap();

        let groups = db.list_group(ListGroupOpt::All).await.unwrap();
        assert_eq!(groups.len(), 0);

        assert!(db
            .create_group(CreateGroupOpt {
                entity_id: entity_id,
                name: "name1".to_owned(),
                category: Category::Batch,
                description: "description".to_owned()
            })
            .await
            .is_ok());
        let groups = db.list_group(ListGroupOpt::All).await.unwrap();
        assert_eq!(groups.len(), 1);

        assert!(db
            .create_group(CreateGroupOpt {
                entity_id: entity_id,
                name: "name2".to_owned(),
                category: Category::Batch,
                description: "description".to_owned()
            })
            .await
            .is_ok());
        let groups = db.list_group(ListGroupOpt::All).await.unwrap();
        assert_eq!(groups.len(), 2);

        let group = db
            .list_group(ListGroupOpt::Ids(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(group.len(), 2);
    }

    #[sqlx::test]
    fn create_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = db.create_entity("name", "description").await.unwrap();
        let group_id = db
            .create_group(CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "group_name".to_owned(),
                description: "description".to_owned(),
            })
            .await
            .unwrap();

        let res = db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await;
        assert!(res.is_ok_and(|id| id == 1));

        let res = db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_name2".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await;
        assert!(res.is_ok_and(|id| id == 2));

        let res = db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await;

        assert!(res.is_err_and(|e| match e {
            Error::ColumnAlreadyExist(name) => name == format!("feature_name"),
            _ => false,
        }));

        let new_group_id = db
            .create_group(CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "new_group_name".to_owned(),
                description: "description".to_owned(),
            })
            .await
            .unwrap();

        let res = db
            .create_feature(CreateFeatureOpt {
                group_id: new_group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await;
        assert!(res.is_ok_and(|id| id == 3));
    }

    #[sqlx::test]
    fn get_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;
        let entity_id = db.create_entity("name", "description").await.unwrap();
        let group_id = db
            .create_group(CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "new_group_name".to_owned(),
                description: "description".to_owned(),
            })
            .await
            .unwrap();

        let id = db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await
            .unwrap();

        let feature = db
            .get_feature(GetFeatureOpt::Id(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(feature.id, 1);
        assert_eq!(feature.name, "feature".to_owned());
        assert_eq!(feature.group_id, group_id);
        assert_eq!(feature.description, "description".to_owned());

        let feature = db
            .get_feature(GetFeatureOpt::Name("feature".to_owned()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(feature.id, 1);
        assert_eq!(feature.name, "feature".to_owned());
        assert_eq!(feature.group_id, group_id);
        assert_eq!(feature.description, "description".to_owned());

        let res = db.get_feature(GetFeatureOpt::Id(id + 1)).await;
        assert!(res.is_ok_and(|res| res.is_none()));
    }

    #[sqlx::test]
    fn update_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(db
            .update_feature(1, "new_description")
            .await
            .is_err_and(|e| match e {
                Error::ColumnNotFound(table, id) => table == "feature" && id == "1",
                _ => false,
            }));

        let entity_id = db.create_entity("name", "description").await.unwrap();
        let group_id = db
            .create_group(CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "name".to_owned(),
                description: "description".to_owned(),
            })
            .await
            .unwrap();

        let feature_id = db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_nam".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await
            .unwrap();

        assert!(db
            .update_feature(feature_id, "new_description")
            .await
            .is_ok());

        let feature = db.get_feature(GetFeatureOpt::Id(feature_id)).await.unwrap();
        assert!(feature.is_some_and(|f| {
            f.id == feature_id
                && f.group_id == group_id
                && f.description == "new_description".to_owned()
                && f.value_type == FeatureValueType::Float64
        }));
    }

    #[sqlx::test]
    fn list_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(db
            .update_feature(1, "new_description")
            .await
            .is_err_and(|e| match e {
                Error::ColumnNotFound(table, id) => table == "feature" && id == "1",
                _ => false,
            }));

        let entity_id = db.create_entity("name", "description").await.unwrap();
        let group_id = db
            .create_group(CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "name".to_owned(),
                description: "description".to_owned(),
            })
            .await
            .unwrap();

        let features = db.list_feature(ListFeatureOpt::All).await.unwrap();
        assert_eq!(features.len(), 0);

        assert!(db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await
            .is_ok());

        let features = db.list_feature(ListFeatureOpt::All).await.unwrap();
        assert_eq!(features.len(), 1);

        assert!(db
            .create_feature(CreateFeatureOpt {
                group_id,
                feature_name: "feature_name2".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            })
            .await
            .is_ok());

        let features = db.list_feature(ListFeatureOpt::All).await.unwrap();
        assert_eq!(features.len(), 2);

        let features = db
            .list_feature(ListFeatureOpt::Ids(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(features.len(), 2);
    }
}
