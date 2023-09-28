#![allow(unused_variables, dead_code)]
pub mod schema;

use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};

use crate::database::error::Error;
use crate::database::metadata::types::{self, *};
use crate::database::{Result, SQLiteOpt};
use crate::feastore::apply::ApplyStage;
use schema::{META_TABLE_SCHEMAS, META_VIEW_SCHEMAS};

pub struct DB {
    pub pool: SqlitePool,
}

impl DB {
    pub(crate) async fn from(db_file: SQLiteOpt) -> Self {
        let pool = SqlitePool::connect(format!("sqlite://{}", &db_file.db_file).as_str())
            .await
            .expect(&format!("open {} failed!", db_file.db_file));

        let db = Self { pool };
        db.create_schemas().await;
        db
    }
    fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl DB {
    async fn create_schemas(&self) {
        for table_schema in META_TABLE_SCHEMAS.values() {
            sqlx::query(&table_schema)
                .execute(&self.pool)
                .await
                .expect(format!("create schemai {} failed!", table_schema).as_str());
        }

        for view_schema in META_VIEW_SCHEMAS.values() {
            sqlx::query(&view_schema).execute(&self.pool).await.unwrap();
        }

        for table in META_TABLE_SCHEMAS.keys() {
            //TODO: use template engine instead {}
            let trigger = format!(
                r"
                    CREATE TRIGGER IF NOT EXISTS {table}_update_modify_time
                    AFTER UPDATE ON {table}
                    BEGIN
                        update {table} SET modify_time = datetime('now') WHERE id = NEW.id;
                    END;"
            );
            sqlx::query(&trigger).execute(&self.pool).await.unwrap();
        }
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    pub(crate) async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        create_entity(&self.pool, name, description).await
    }
    pub(crate) async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        update_entity(&self.pool, id, new_description).await
    }
    pub(crate) async fn get_entity(&self, opt: GetOpt) -> Result<Option<Entity>> {
        get_entity(&self.pool, opt).await
    }
    pub(crate) async fn list_entity(&self, opt: ListOpt) -> Result<Vec<Entity>> {
        list_entity(&self.pool, opt).await
    }

    pub(crate) async fn create_group(&self, group: CreateGroupOpt) -> Result<i64> {
        create_group(&self.pool, group).await
    }

    pub(crate) async fn update_group(&self, id: i64, new_description: &str) -> Result<()> {
        update_group(&self.pool, id, new_description).await
    }

    pub(crate) async fn get_group(&self, opt: GetOpt) -> Result<Option<Group>> {
        get_group(&self.pool, opt).await
    }

    pub(crate) async fn list_group(&self, opt: ListOpt) -> Result<Vec<Group>> {
        list_group(&self.pool, opt).await
    }

    pub(crate) async fn create_feature(&self, feature: CreateFeatureOpt) -> Result<i64> {
        create_feature(&self.pool, feature).await
    }

    pub(crate) async fn update_feature(&self, id: i64, new_description: &str) -> Result<()> {
        update_feature(&self.pool, id, new_description).await
    }

    pub(crate) async fn get_feature(&self, opt: GetOpt) -> Result<Option<Feature>> {
        get_feature(&self.pool, opt).await
    }

    pub(crate) async fn list_feature(&self, opt: ListOpt) -> Result<Vec<Feature>> {
        list_feature(&self.pool, opt).await
    }

    pub(crate) async fn apply(&self, stage: ApplyStage) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for ne in stage.new_entities {
            let e = get_entity(&mut tx, GetOpt::Name(ne.name.clone())).await?;
            match e {
                None => {
                    create_entity(&mut tx, ne.name.as_str(), ne.description.as_str()).await?;
                }
                Some(e) => {
                    if e.description != ne.description {
                        update_entity(&mut tx, e.id, ne.description.as_str()).await?;
                    }
                }
            }
        }

        for ng in stage.new_groups {
            let entity_name = if let Some(n) = ng.entity_name {
                n
            } else {
                continue;
            };

            let g = get_group(&mut tx, GetOpt::Name(ng.name.clone())).await?;
            match g {
                None => {
                    if let Some(e) = get_entity(&mut tx, GetOpt::Name(entity_name)).await? {
                        create_group(
                            &mut tx,
                            CreateGroupOpt {
                                entity_id: e.id,
                                name: ng.name,
                                category: ng.category,
                                description: ng.description,
                            },
                        )
                        .await?;
                    }
                }
                Some(g) => {
                    if g.description != ng.description {
                        update_group(&mut tx, g.id, ng.description.as_str()).await?;
                    }
                }
            }
        }

        for nf in stage.new_features {
            let group_name = if let Some(n) = nf.group_name {
                n
            } else {
                continue;
            };

            let f = get_feature(&mut tx, GetOpt::Name(nf.name.clone())).await?;
            match f {
                None => {
                    if let Some(g) = get_group(&mut tx, GetOpt::Name(group_name)).await? {
                        create_feature(
                            &mut tx,
                            CreateFeatureOpt {
                                group_id: g.id,
                                feature_name: nf.name,
                                description: nf.description,
                                value_type: nf.value_type,
                            },
                        )
                        .await?;
                    }
                }
                Some(feature) => {
                    if feature.description != nf.description {
                        update_feature(&mut tx, feature.id, nf.description.as_str()).await?;
                    }
                }
            }
        }

        tx.commit().await.map_err(|e| e.into())
    }
}

async fn apply(tx: &Transaction<'_, Sqlite>, stage: ApplyStage) -> Result<()> {
    Ok(())
}

async fn create_entity<'a, A>(conn: A, name: &str, description: &str) -> Result<i64>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let res = sqlx::query("INSERT INTO entity (name, description) VALUES (?, ?)")
        .bind(name)
        .bind(description)
        .execute(&mut *conn)
        .await;

    match res {
        Err(sqlx::Error::Database(e)) => {
            if e.message() == "UNIQUE constraint failed: entity.name" {
                Err(Error::ColumnAlreadyExist(name.to_string()))
            } else {
                Err(e.into())
            }
        }
        _ => Ok(res?.last_insert_rowid()),
    }
}

async fn update_entity<'a, A>(conn: A, id: i64, new_description: &str) -> Result<()>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let rows_affected = sqlx::query("UPDATE entity SET description = ? WHERE id = ?")
        .bind(new_description)
        .bind(id)
        .execute(&mut *conn)
        .await?
        .rows_affected();

    if rows_affected != 1 {
        Err(Error::ColumnNotFound("entity".to_owned(), id.to_string()))
    } else {
        Ok(())
    }
}

async fn get_entity<'a, A>(conn: A, opt: types::GetOpt) -> Result<Option<types::Entity>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let query = match opt {
        types::GetOpt::ID(id) => sqlx::query_as("SELECT * FROM entity WHERE id = ?").bind(id),
        types::GetOpt::Name(name) => {
            sqlx::query_as("SELECT * FROM entity WHERE name = ?").bind(name)
        }
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_entity<'a, A>(conn: A, opt: types::ListOpt) -> Result<Vec<types::Entity>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let mut query_str = "SELECT * FROM entity".to_owned();

    let query = match opt {
        types::ListOpt::All => sqlx::query(&query_str),
        types::ListOpt::IDs(ids) => {
            if ids.is_empty() {
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
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(types::Entity::from_row)
        .collect::<std::result::Result<Vec<types::Entity>, sqlx::Error>>();

    res.map_err(|e| e.into())
}

async fn create_group<'a, A>(conn: A, group: types::CreateGroupOpt) -> Result<i64>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let res = sqlx::query(
        "INSERT INTO feature_group (name, category, description, entity_id) VALUES (?, ?, ?, ?)",
    )
    .bind(&group.name)
    .bind(group.category)
    .bind(group.description)
    .bind(group.entity_id)
    .execute(&mut *conn)
    .await;

    match res {
        Err(sqlx::Error::Database(e)) => {
            if e.message() == "UNIQUE constraint failed: feature_group.name" {
                Err(Error::ColumnAlreadyExist(group.name))
            } else {
                Err(e.into())
            }
        }
        _ => Ok(res?.last_insert_rowid()),
    }
}

async fn update_group<'a, A>(conn: A, id: i64, new_description: &str) -> Result<()>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let rows_affected = sqlx::query("UPDATE feature_group SET description = ? WHERE id = ?")
        .bind(new_description)
        .bind(id)
        .execute(&mut *conn)
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

async fn get_group<'a, A>(conn: A, opt: types::GetOpt) -> Result<Option<types::Group>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let query = match opt {
        types::GetOpt::ID(id) => {
            sqlx::query_as("SELECT * FROM feature_group WHERE id = ?").bind(id)
        }
        types::GetOpt::Name(name) => {
            sqlx::query_as("SELECT * FROM feature_group WHERE name = ?").bind(name)
        }
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_group<'a, A>(conn: A, opt: types::ListOpt) -> Result<Vec<types::Group>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let mut query_str = "SELECT * FROM feature_group".to_owned();

    let query = match opt {
        types::ListOpt::All => sqlx::query(&query_str),
        types::ListOpt::IDs(ids) => {
            if ids.is_empty() {
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
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(types::Group::from_row)
        .collect::<std::result::Result<Vec<types::Group>, sqlx::Error>>();

    res.map_err(|e| e.into())
}

async fn create_feature<'a, A>(conn: A, opt: types::CreateFeatureOpt) -> Result<i64>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let res = sqlx::query(
        "INSERT INTO feature (group_id, name, value_type, description) VALUES (?, ?, ?, ?)",
    )
    .bind(opt.group_id)
    .bind(&opt.feature_name)
    .bind(opt.value_type)
    .bind(opt.description)
    .execute(&mut *conn)
    .await;

    match res {
        Err(sqlx::Error::Database(e)) => {
            if e.message() == "UNIQUE constraint failed: feature.group_id, feature.name" {
                Err(Error::ColumnAlreadyExist(opt.feature_name))
            } else {
                println!("{}", e.message());
                Err(e.into())
            }
        }
        _ => Ok(res?.last_insert_rowid()),
    }
}

async fn update_feature<'a, A>(conn: A, id: i64, new_description: &str) -> Result<()>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let rows_affected = sqlx::query("UPDATE feature  SET description = ? WHERE id = ?")
        .bind(new_description)
        .bind(id)
        .execute(&mut *conn)
        .await?
        .rows_affected();

    if rows_affected != 1 {
        Err(Error::ColumnNotFound("feature".to_owned(), id.to_string()))
    } else {
        Ok(())
    }
}

async fn get_feature<'a, A>(conn: A, opt: types::GetOpt) -> Result<Option<Feature>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let query = match opt {
        types::GetOpt::ID(id) => sqlx::query_as("SELECT * FROM feature WHERE id = ?").bind(id),
        types::GetOpt::Name(name) => {
            sqlx::query_as("SELECT * FROM feature WHERE name = ?").bind(name)
        }
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_feature<'a, A>(conn: A, opt: ListOpt) -> Result<Vec<Feature>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let mut query_str = "SELECT * FROM feature".to_owned();

    let query = match opt {
        ListOpt::All => sqlx::query(&query_str),
        ListOpt::IDs(ids) => {
            if ids.is_empty() {
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
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(Feature::from_row)
        .collect::<std::result::Result<Vec<Feature>, sqlx::Error>>();

    res.map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use crate::database::metadata::sqlite::DB;
    use crate::database::metadata::types::FeatureValueType;
    use crate::database::{error::Error, metadata::types::Category};

    use super::*;

    async fn prepare_db(pool: SqlitePool) -> DB {
        let db = DB::from_pool(pool);
        db.create_schemas().await;
        db
    }

    #[sqlx::test]
    async fn create_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let res: Result<i64> = super::create_entity(&db.pool, "user", "description").await;
        assert!(res.is_ok() && res.unwrap() == 1);

        let res: Result<i64> = super::create_entity(&db.pool, "user", "description").await;
        assert!(match res.err() {
            Some(Error::ColumnAlreadyExist(name)) => name == "user",
            _ => false,
        });
    }

    #[sqlx::test]
    async fn get_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();

        let entity = super::get_entity(&db.pool, GetOpt::ID(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let entity = super::get_entity(&db.pool, GetOpt::Name("name".to_owned()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let res = super::get_entity(&db.pool, GetOpt::Name("not_exist".to_owned()))
            .await
            .unwrap();
        assert!(res.is_none());

        let res = super::get_entity(&db.pool, GetOpt::ID(id + 1))
            .await
            .unwrap();
        assert!(res.is_none());
    }

    #[sqlx::test]
    async fn update_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();

        assert!(super::update_entity(&db.pool, id, "new_description")
            .await
            .is_ok());

        let entity = super::get_entity(&db.pool, GetOpt::ID(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "new_description");

        assert!(super::update_entity(&db.pool, id + 1, "new_description")
            .await
            .is_err_and(|e| match e {
                Error::ColumnNotFound(table, id) => table == "entity" && id == "2",
                _ => false,
            }));
    }

    #[sqlx::test]
    async fn list_entity(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entities = super::list_entity(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(entities.len(), 0);

        assert!(super::create_entity(&db.pool, "name", "description")
            .await
            .is_ok());
        let entities = super::list_entity(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(entities.len(), 1);

        assert!(super::create_entity(&db.pool, "name2", "description")
            .await
            .is_ok());
        let entities = super::list_entity(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(entities.len(), 2);

        assert!(super::create_entity(&db.pool, "name3", "description")
            .await
            .is_ok());
        let entities = super::list_entity(&db.pool, ListOpt::IDs(vec![1, 2]))
            .await
            .unwrap();
        assert_eq!(entities.len(), 2);

        let entities = super::list_entity(&db.pool, ListOpt::IDs(vec![1, 2, 3, 4]))
            .await
            .unwrap();
        assert_eq!(entities.len(), 3);

        let entities = super::list_entity(&db.pool, ListOpt::IDs(Vec::new()))
            .await
            .unwrap();
        assert_eq!(entities.len(), 0);
    }

    #[sqlx::test]
    async fn crate_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();

        let res = super::create_group(
            &db.pool,
            CreateGroupOpt {
                name: "name".to_owned(),
                category: Category::Batch,
                description: "description".to_owned(),
                entity_id,
            },
        )
        .await;
        assert!(res.is_ok_and(|id| id == 1));

        let res = super::create_group(
            &db.pool,
            CreateGroupOpt {
                name: "name1".to_owned(),
                category: Category::Stream,
                description: "description".to_owned(),
                entity_id,
            },
        )
        .await;
        assert!(res.is_ok_and(|id| id == 2));

        let res = super::create_group(
            &db.pool,
            CreateGroupOpt {
                name: "name".to_owned(),
                category: Category::Batch,
                description: "description".to_owned(),
                entity_id,
            },
        )
        .await;

        assert!(res.is_err_and(|e| match e {
            Error::ColumnAlreadyExist(name) => name == "name",
            _ => false,
        }));
    }

    #[sqlx::test]
    async fn get_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;
        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();

        let group_zero = Group {
            id: 1,
            name: "name".to_owned(),
            category: Category::Batch,
            description: "description".to_owned(),
            entity_id,
            ..Default::default()
        };
        let id = super::create_group(&db.pool, group_zero.clone().into())
            .await
            .unwrap();

        let group = super::get_group(&db.pool, GetOpt::ID(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq_of_group(&group, &group_zero);

        let group = super::get_group(&db.pool, GetOpt::Name(group_zero.name.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq_of_group(&group, &group_zero);

        let res = super::get_group(&db.pool, GetOpt::ID(id + 1)).await;
        assert!(res.is_ok_and(|res| res.is_none()));

        let res = super::get_group(&db.pool, GetOpt::Name("not_exist".to_owned())).await;
        assert!(res.is_ok_and(|res| res.is_none()));
    }

    fn assert_eq_of_group(left: &Group, right: &Group) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.name, right.name);
        assert_eq!(left.category, right.category);
        assert_eq!(left.description, right.description);
    }

    #[sqlx::test]
    async fn update_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(super::update_group(&db.pool, 1, "new_description")
            .await
            .is_err_and(|e| {
                match e {
                    Error::ColumnNotFound(table, id) => table == "feature_group" && id == "1",
                    _ => false,
                }
            }));

        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();
        let origin_group = Group {
            id: 1,
            name: "name".to_owned(),
            category: Category::Batch,
            description: "description".to_owned(),
            entity_id,
            ..Default::default()
        };
        let group_id = super::create_group(&db.pool, origin_group.clone().into())
            .await
            .unwrap();
        assert!(super::update_group(&db.pool, group_id, "new_description")
            .await
            .is_ok());
        let group = super::get_group(&db.pool, GetOpt::ID(group_id))
            .await
            .unwrap();
        assert!(group.is_some_and(|g| {
            g.id == origin_group.id
                && g.entity_id == origin_group.entity_id
                && g.category == origin_group.category
                && g.name == origin_group.name
                && g.description == "new_description"
        }));
    }

    #[sqlx::test]
    async fn list_group(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = super::create_entity(&db.pool, "entity", "description")
            .await
            .unwrap();

        let groups = super::list_group(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(groups.len(), 0);

        assert!(super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                name: "name1".to_owned(),
                category: Category::Batch,
                description: "description".to_owned()
            }
        )
        .await
        .is_ok());
        let groups = super::list_group(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(groups.len(), 1);

        assert!(super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                name: "name2".to_owned(),
                category: Category::Batch,
                description: "description".to_owned()
            }
        )
        .await
        .is_ok());
        let groups = super::list_group(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(groups.len(), 2);

        let group = super::list_group(&db.pool, ListOpt::IDs(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(group.len(), 2);
    }

    #[sqlx::test]
    async fn create_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();
        let group_id = super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "group_name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let res = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await;
        assert!(res.is_ok_and(|id| id == 1));

        let res = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name2".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await;
        assert!(res.is_ok_and(|id| id == 2));

        let res = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await;

        assert!(res.is_err_and(|e| match e {
            Error::ColumnAlreadyExist(name) => name == "feature_name",
            _ => false,
        }));

        let new_group_id = super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "new_group_name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let res = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id: new_group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await;
        assert!(res.is_ok_and(|id| id == 3));
    }

    #[sqlx::test]
    async fn get_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;
        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();
        let group_id = super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "new_group_name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let id = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await
        .unwrap();

        let feature = super::get_feature(&db.pool, GetOpt::ID(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(feature.id, 1);
        assert_eq!(feature.name, "feature".to_owned());
        assert_eq!(feature.group_id, group_id);
        assert_eq!(feature.description, "description".to_owned());

        let feature = super::get_feature(&db.pool, GetOpt::Name("feature".to_owned()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(feature.id, 1);
        assert_eq!(feature.name, "feature".to_owned());
        assert_eq!(feature.group_id, group_id);
        assert_eq!(feature.description, "description".to_owned());

        let res = super::get_feature(&db.pool, GetOpt::ID(id + 1)).await;
        assert!(res.is_ok_and(|res| res.is_none()));
    }

    #[sqlx::test]
    async fn update_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(super::update_feature(&db.pool, 1, "new_description")
            .await
            .is_err_and(|e| match e {
                Error::ColumnNotFound(table, id) => table == "feature" && id == "1",
                _ => false,
            }));

        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();
        let group_id = super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let feature_id = super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_nam".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            },
        )
        .await
        .unwrap();

        assert!(
            super::update_feature(&db.pool, feature_id, "new_description")
                .await
                .is_ok()
        );

        let feature = super::get_feature(&db.pool, GetOpt::ID(feature_id))
            .await
            .unwrap();
        assert!(feature.is_some_and(|f| {
            f.id == feature_id
                && f.group_id == group_id
                && f.description == "new_description"
                && f.value_type == FeatureValueType::Float64
        }));
    }

    #[sqlx::test]
    async fn list_feature(pool: SqlitePool) {
        let db = prepare_db(pool).await;

        assert!(super::update_feature(&db.pool, 1, "new_description")
            .await
            .is_err_and(|e| match e {
                Error::ColumnNotFound(table, id) => table == "feature" && id == "1",
                _ => false,
            }));

        let entity_id = super::create_entity(&db.pool, "name", "description")
            .await
            .unwrap();
        let group_id = super::create_group(
            &db.pool,
            CreateGroupOpt {
                entity_id,
                category: Category::Batch,
                name: "name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let features = super::list_feature(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(features.len(), 0);

        assert!(super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            }
        )
        .await
        .is_ok());

        let features = super::list_feature(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(features.len(), 1);

        assert!(super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name2".to_owned(),
                description: "description".to_owned(),
                value_type: FeatureValueType::Float64,
            }
        )
        .await
        .is_ok());

        let features = super::list_feature(&db.pool, ListOpt::All).await.unwrap();
        assert_eq!(features.len(), 2);

        let features = super::list_feature(&db.pool, ListOpt::IDs(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(features.len(), 2);
    }
}
