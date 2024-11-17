use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};

use crate::database::metadata::sqlite::schema;
use crate::database::metadata::types::{Feature, Group2, ListFeatureOpt, ListGroupOpt};
use crate::database::metadata::{
    CreateFeatureOpt, CreateGroupOpt, Entity, GetOpt, Group, ListOpt, RichEntity, RichFeature,
    RichGroup,
};
use crate::database::{Error, Result, SQLiteOpt};
use crate::feastore::apply::ApplyStage;

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    pub(crate) async fn from(db_file: SQLiteOpt) -> Self {
        let pool = SqlitePool::connect(format!("sqlite://{}", &db_file.db_file).as_str())
            .await
            .unwrap_or_else(|_| panic!("open {} failed!", db_file.db_file));

        let db = Self { pool };
        db.create_schemas().await;
        db
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    async fn create_schemas(&self) {
        schema::create_tables(&self.pool).await;
        schema::create_views(&self.pool).await;
        schema::create_trigger(&self.pool).await;
    }

    pub(crate) async fn apply(&self, stage: ApplyStage) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        match self.apply_internal(&mut tx, stage).await {
            Ok(_) => tx.commit().await.map_err(|e| e.into()),
            Err(_) => tx.rollback().await.map_err(|e| e.into()),
        }
    }

    async fn apply_internal(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        mut stage: ApplyStage,
    ) -> Result<()> {
        for e in stage.new_entities.drain(..) {
            self.apply_entity(tx, e).await?;
        }

        for mut g in stage.new_groups.drain(..) {
            if let Some(name) = g.entity_name.take() {
                self.apply_group(tx, &name, g).await?;
            }
        }

        for f in stage.new_features.drain(..) {
            self.apply_feature(tx, f).await?;
        }

        Ok(())
    }

    pub(crate) async fn list_rich_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<RichEntity>> {
        let entities = list_entity(&self.pool, opt).await?;
        let groups = self.list_rich_group(ListOpt::All).await?;

        let mut res = vec![];
        for entity in entities {
            let entity_name = Some(entity.name.to_owned());
            let the_groups = groups
                .iter()
                .filter(|g| g.entity_name == entity_name)
                .map(|g| g.clone())
                .collect::<Vec<RichGroup>>();

            res.push(RichEntity::from(entity, Some(the_groups)));
        }
        Ok(res)
    }

    pub(crate) async fn list_rich_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<RichGroup>> {
        let groups = self.list_group(opt).await?;
        let features = self
            .list_feature(ListFeatureOpt::EntityIDs(vec![1])) // FIXME
            .await?;

        let mut res = vec![];
        for group in groups {
            let the_features: Vec<_> = features
                .iter()
                .filter(|feature| feature.group_id == group.id)
                .map(|f| RichFeature::from2(f.to_owned()))
                .collect();

            res.push(RichGroup::from(group, Some(the_features)));
        }

        Ok(res)
    }

    async fn apply_entity(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        entity: RichEntity,
    ) -> Result<()> {
        let old_entity = get_entity(&mut *tx, GetOpt::Name(&entity.name)).await?;

        if let Some(oe) = old_entity {
            if oe.description != entity.description {
                update_entity(&mut *tx, oe.id, &entity.description).await?;
            }
            return Ok(());
        }

        create_entity(&mut *tx, &entity.name, &entity.description).await?;

        Ok(())
    }

    async fn apply_group(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        entity_name: &str,
        group: RichGroup,
    ) -> Result<()> {
        let old_group = get_group(&mut *tx, GetOpt::Name(&group.name)).await?;

        if let Some(og) = old_group {
            if og.description != group.description {
                update_group(&mut *tx, og.id, &group.description).await?;
            }
            return Ok(());
        }

        if let Some(e) = get_entity(&mut *tx, GetOpt::Name(entity_name)).await? {
            create_group(
                &mut *tx,
                CreateGroupOpt {
                    entity_id: e.id,
                    name: group.name,
                    category: group.category,
                    snapshot_interval: group.snapshot_interval,
                    description: group.description,
                },
            )
            .await?;
        }

        Ok(())
    }

    async fn apply_feature(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        feature: RichFeature,
    ) -> Result<()> {
        let old_feature = get_feature(&mut *tx, GetOpt::Name(&feature.name)).await?;

        if let Some(of) = old_feature {
            if of.description != feature.description {
                update_feature(&mut *tx, of.id, &feature.description).await?;
            }
            return Ok(());
        }

        if let Some(group_name) = feature.group_name {
            if let Some(g) = get_group(&mut *tx, GetOpt::Name(&group_name)).await? {
                create_feature(
                    &mut *tx,
                    CreateFeatureOpt {
                        group_id: g.id,
                        feature_name: feature.name,
                        description: feature.description,
                        value_type: feature.value_type,
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn list_group2(&self, opt: ListGroupOpt) -> Result<Vec<Group2>> {
        let mut groups = list_group2(&self.pool, opt).await?;
        let entity_ids = groups.iter().map(|g| g.entity_id).collect::<Vec<i64>>();
        let entities = list_entity(&self.pool, ListOpt::IDs(entity_ids)).await?;

        for group in groups.iter_mut() {
            let entity = entities.iter().find(|e| group.entity_id == e.id);
            group.entity = entity.cloned();
        }

        Ok(groups)
    }

    pub(crate) async fn list_feature(&self, opt: ListFeatureOpt) -> Result<Vec<Feature>> {
        let mut features = list_feature2(&self.pool, opt).await?;
        let ids = features.iter().map(|f| f.group_id).collect();
        let groups = self.list_group2(ListGroupOpt::GroupIDs(ids)).await?;

        features.iter_mut().for_each(|f| {
            f.group = groups.iter().find(|g| f.group_id == g.id).cloned();
        });

        Ok(features)
    }

    pub(crate) async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        create_entity(&self.pool, name, description).await
    }
    pub(crate) async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        update_entity(&self.pool, id, new_description).await
    }
    pub(crate) async fn get_entity<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Entity>> {
        get_entity(&self.pool, opt).await
    }
    pub(crate) async fn list_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Entity>> {
        list_entity(&self.pool, opt).await
    }

    pub(crate) async fn create_group(&self, group: CreateGroupOpt) -> Result<i64> {
        create_group(&self.pool, group).await
    }

    pub(crate) async fn update_group(&self, id: i64, new_description: &str) -> Result<()> {
        update_group(&self.pool, id, new_description).await
    }

    pub(crate) async fn get_group<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Group>> {
        get_group(&self.pool, opt).await
    }

    pub(crate) async fn list_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Group>> {
        list_group(&self.pool, opt).await
    }

    pub(crate) async fn create_feature(&self, feature: CreateFeatureOpt) -> Result<i64> {
        create_feature(&self.pool, feature).await
    }

    pub(crate) async fn update_feature(&self, id: i64, new_description: &str) -> Result<()> {
        update_feature(&self.pool, id, new_description).await
    }

    pub(crate) async fn get_feature<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Feature>> {
        get_feature(&self.pool, opt).await
    }
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
                Err(e.to_string().into())
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

async fn get_entity<'a, A>(conn: A, opt: GetOpt<'a>) -> Result<Option<Entity>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let query = match opt {
        GetOpt::ID(id) => sqlx::query_as("SELECT * FROM entity WHERE id = ?").bind(id),
        GetOpt::Name(name) => sqlx::query_as("SELECT * FROM entity WHERE name = ?").bind(name),
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_entity<'a, A>(conn: A, opt: ListOpt<'a>) -> Result<Vec<Entity>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let mut query_str = "SELECT * FROM entity".to_owned();

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
        ListOpt::Names(names) => {
            if names.is_empty() {
                return Ok(Vec::new());
            }

            query_str = format!(
                "{query_str} WHERE name in (?{})",
                ", ?".repeat(names.len() - 1)
            );
            let mut query = sqlx::query(&query_str);
            for name in names {
                query = query.bind(name);
            }
            query
        }
    };

    let res = query
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(Entity::from_row)
        .collect::<std::result::Result<Vec<Entity>, sqlx::Error>>();

    res.map_err(|e| e.into())
}

async fn create_group<'a, A>(conn: A, group: CreateGroupOpt) -> Result<i64>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let res = sqlx::query(
        "INSERT INTO feature_group (name, category, snapshot_interval, description, entity_id) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&group.name)
    .bind(group.category)
    .bind(group.snapshot_interval)
    .bind(group.description)
    .bind(group.entity_id)
    .execute(&mut *conn)
    .await;

    match res {
        Err(sqlx::Error::Database(e)) => {
            if e.message() == "UNIQUE constraint failed: feature_group.name" {
                Err(Error::ColumnAlreadyExist(group.name))
            } else {
                Err(e.to_string().into())
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

async fn get_group<'a, A>(conn: A, opt: GetOpt<'a>) -> Result<Option<Group>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let mut query_str = r#"
        SELECT g.id, g.name, e.name as entity_name, g.category, g.entity_id, g.snapshot_interval, g.description, g.create_time, g.modify_time
        FROM feature_group as g LEFT JOIN entity as e on g.entity_id = e.id
    "#.to_string();

    let query = match opt {
        GetOpt::ID(id) => {
            query_str = format!("{query_str} WHERE g.id = ?");
            sqlx::query_as(&query_str).bind(id)
        }
        GetOpt::Name(name) => {
            query_str = format!("{query_str} WHERE g.name = ?");
            sqlx::query_as(&query_str).bind(name)
        }
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_group<'a, A>(conn: A, opt: ListOpt<'a>) -> Result<Vec<Group>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;

    let mut query_str = r#"
        SELECT g.id, g.name, e.name as entity_name, g.category, g.entity_id, g.snapshot_interval, g.description, g.create_time, g.modify_time
        FROM feature_group as g LEFT JOIN entity as e on g.entity_id = e.id"#.to_string();

    let query = match opt {
        ListOpt::All => sqlx::query(&query_str),
        ListOpt::IDs(ids) => {
            if ids.is_empty() {
                return Ok(Vec::new());
            }

            query_str = format!(
                "{query_str} WHERE g.id in (?{})",
                ", ?".repeat(ids.len() - 1)
            );
            let mut query = sqlx::query(&query_str);
            for id in ids {
                query = query.bind(id);
            }
            query
        }
        ListOpt::Names(names) => {
            if names.is_empty() {
                return Ok(Vec::new());
            }

            query_str = format!(
                "{query_str} WHERE g.name in (?{})",
                ", ?".repeat(names.len() - 1)
            );
            let mut query = sqlx::query(&query_str);
            for name in names {
                query = query.bind(name);
            }
            query
        }
    };

    let res = query
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(Group::from_row)
        .collect::<std::result::Result<Vec<Group>, sqlx::Error>>();

    res.map_err(|e| e.into())
}

async fn list_group2<'a, A>(conn: A, opt: ListGroupOpt) -> Result<Vec<Group2>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let query = "SELECT id, name, category, entity_id, snapshot_interval, description, create_time, modify_time FROM feature_group".to_string();

    let (cond, ids) = build_list_group_cond(&opt);
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let query = format!("{query} WHERE {cond} in (?{})", ", ?".repeat(ids.len() - 1));
    let mut query = sqlx::query(&query);
    for id in ids {
        query = query.bind(id);
    }
    query
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(Group2::from_row)
        .collect::<std::result::Result<Vec<Group2>, sqlx::Error>>()
        .map_err(|e| e.into())
}

fn build_list_group_cond(opt: &ListGroupOpt) -> (&'static str, &Vec<i64>) {
    match opt {
        ListGroupOpt::EntityIDs(ids) => ("entity_id", ids),
        ListGroupOpt::GroupIDs(ids) => ("id", ids),
    }
}

async fn create_feature<'a, A>(conn: A, opt: CreateFeatureOpt) -> Result<i64>
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
                Err(e.to_string().into())
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

async fn get_feature<'a, A>(conn: A, opt: GetOpt<'a>) -> Result<Option<Feature>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let mut query =
        "SELECT id, name, group_id, value_type, description, create_time, modify_time FROM feature"
            .to_string();

    let query = match opt {
        GetOpt::ID(id) => {
            query = format!("{query} WHERE id = ?");
            sqlx::query_as(&query).bind(id)
        }
        GetOpt::Name(name) => {
            query = format!("{query} WHERE name = ?");
            sqlx::query_as(&query).bind(name)
        }
    };

    Ok(query.fetch_optional(&mut *conn).await?)
}

async fn list_feature2<'a, A>(conn: A, opt: ListFeatureOpt) -> Result<Vec<Feature>>
where
    A: sqlx::Acquire<'a, Database = sqlx::Sqlite>,
{
    let mut conn = conn.acquire().await?;
    let mut query =
        "SELECT id, name, group_id, value_type, description, create_time, modify_time FROM feature"
            .to_string();

    let query = if let Some((cond, ids)) = build_list_feature_cond(&opt) {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        query = format!(
            "{query} WHERE {} in (?{})",
            cond,
            ", ?".repeat(ids.len() - 1)
        );
        let mut query = sqlx::query(&query);
        for id in ids {
            query = query.bind(id);
        }
        query
    } else {
        sqlx::query(&query)
    };

    query
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .map(Feature::from_row)
        .collect::<std::result::Result<Vec<Feature>, sqlx::Error>>()
        .map_err(|e| e.into())
}

fn build_list_feature_cond(opt: &ListFeatureOpt) -> Option<(&'static str, &Vec<i64>)> {
    match opt {
        ListFeatureOpt::GroupIDs(ids) => Some(("group_id", ids)),
        ListFeatureOpt::FeatureIDs(ids) => Some(("id", ids)),
        ListFeatureOpt::EntityIDs(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::error::Error;
    use crate::database::metadata::types::{Category, ValueType};
    use sqlx::SqlitePool;

    async fn prepare_db(pool: SqlitePool) -> DB {
        let db = DB { pool };
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

        let entity = super::get_entity(&db.pool, GetOpt::Name(&"name"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.id, id);
        assert_eq!(entity.name, "name");
        assert_eq!(entity.description, "description");

        let res = super::get_entity(&db.pool, GetOpt::Name(&"not_exist"))
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

        let entities = super::list_entity(&db.pool, ListOpt::Names(vec![&"name", &"name2"]))
            .await
            .unwrap();
        assert_eq!(entities.len(), 2);

        let entities = super::list_entity(
            &db.pool,
            ListOpt::Names(vec![&"name", &"name2", &"name3", &"name4"]),
        )
        .await
        .unwrap();
        assert_eq!(entities.len(), 3);

        let entities = super::list_entity(&db.pool, ListOpt::Names(Vec::new()))
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
                snapshot_interval: None,
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
                snapshot_interval: None,
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
                snapshot_interval: None,
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
            snapshot_interval: Some(123),
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

        let group = super::get_group(&db.pool, GetOpt::Name(&group_zero.name))
            .await
            .unwrap()
            .unwrap();
        assert_eq_of_group(&group, &group_zero);

        let res = super::get_group(&db.pool, GetOpt::ID(id + 1)).await;
        assert!(res.is_ok_and(|res| res.is_none()));

        let res = super::get_group(&db.pool, GetOpt::Name(&"not_exist")).await;
        assert!(res.is_ok_and(|res| res.is_none()));
    }

    fn assert_eq_of_group(left: &Group, right: &Group) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.name, right.name);
        assert_eq!(left.category, right.category);
        assert_eq!(left.snapshot_interval, right.snapshot_interval);
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
            snapshot_interval: Some(123456),
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

        if let Some(ref group) = group {
            assert_eq!(group.snapshot_interval, origin_group.snapshot_interval);
        }
        assert!(group.is_some_and(|g| {
            g.id == origin_group.id
                && g.entity_id == origin_group.entity_id
                && g.category == origin_group.category
                && g.snapshot_interval == origin_group.snapshot_interval
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
                snapshot_interval: None,
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
                snapshot_interval: None,
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

        let group = super::list_group(&db.pool, ListOpt::Names(vec![&"name1", &"name2", &"name3"]))
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
                snapshot_interval: None,
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
                value_type: ValueType::Float64,
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
                value_type: ValueType::Float64,
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
                value_type: ValueType::Float64,
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
                snapshot_interval: None,
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
                value_type: ValueType::Float64,
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
                snapshot_interval: None,
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
                value_type: ValueType::Float64,
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

        let feature = super::get_feature(&db.pool, GetOpt::Name(&"feature"))
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
                snapshot_interval: None,
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
                value_type: ValueType::Float64,
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
                && f.value_type == ValueType::Float64
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
                snapshot_interval: None,
                name: "name".to_owned(),
                description: "description".to_owned(),
            },
        )
        .await
        .unwrap();

        let features = super::list_feature2(&db.pool, ListFeatureOpt::EntityIDs(vec![1]))
            .await
            .unwrap();
        assert_eq!(features.len(), 0);

        assert!(super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name".to_owned(),
                description: "description".to_owned(),
                value_type: ValueType::Float64,
            }
        )
        .await
        .is_ok());

        let features = super::list_feature2(&db.pool, ListFeatureOpt::EntityIDs(vec![1]))
            .await
            .unwrap();
        assert_eq!(features.len(), 1);

        assert!(super::create_feature(
            &db.pool,
            CreateFeatureOpt {
                group_id,
                feature_name: "feature_name2".to_owned(),
                description: "description".to_owned(),
                value_type: ValueType::Float64,
            }
        )
        .await
        .is_ok());

        let features = super::list_feature2(&db.pool, ListFeatureOpt::EntityIDs(vec![1]))
            .await
            .unwrap();
        assert_eq!(features.len(), 2);

        let features = super::list_feature2(&db.pool, ListFeatureOpt::FeatureIDs(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(features.len(), 2);

        // let features = super::list_feature2(
        //     &db.pool,
        //     ListOpt::Names(vec![&"feature_name", &"feature_name2", &"feature_name3"]),
        // )
        // .await
        // .unwrap();
        // assert_eq!(features.len(), 2);
    }
}
