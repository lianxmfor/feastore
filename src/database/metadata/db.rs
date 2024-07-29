use crate::database::metadata::{
    sqlite, CreateFeatureOpt, CreateGroupOpt, Entity, Feature, GetOpt, Group, ListOpt,
};
use crate::database::Result;
use crate::feastore::apply;
use crate::feastore::BackendOpt;

pub enum DataStore {
    Sqlite(sqlite::DB),
}

impl DataStore {
    pub(crate) async fn open(opt: BackendOpt) -> Self {
        if let Some(opt) = opt.sqlite {
            let db = sqlite::DB::from(opt).await;
            Self::Sqlite(db)
        } else {
            panic!("not backend found here!")
        }
    }

    pub(crate) async fn close(&self) {
        match self {
            DataStore::Sqlite(db) => db.close().await,
        }
    }
}

impl DataStore {
    pub(crate) async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        match self {
            Self::Sqlite(db) => db.create_entity(name, description).await,
        }
    }

    pub(crate) async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        match self {
            Self::Sqlite(db) => db.update_entity(id, new_description).await,
        }
    }

    pub(crate) async fn get_entity<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Entity>> {
        match self {
            Self::Sqlite(db) => db.get_entity(opt).await,
        }
    }

    pub(crate) async fn list_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Entity>> {
        match self {
            Self::Sqlite(db) => db.list_entity(opt).await,
        }
    }

    pub(crate) async fn create_group(&self, group: CreateGroupOpt) -> Result<i64> {
        match self {
            Self::Sqlite(db) => db.create_group(group).await,
        }
    }

    pub(crate) async fn update_group(&self, id: i64, new_description: &str) -> Result<()> {
        match self {
            Self::Sqlite(db) => db.update_group(id, new_description).await,
        }
    }

    pub(crate) async fn get_group<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Group>> {
        match self {
            Self::Sqlite(db) => db.get_group(opt).await,
        }
    }

    pub(crate) async fn list_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Group>> {
        match self {
            Self::Sqlite(db) => db.list_group(opt).await,
        }
    }

    pub(crate) async fn create_feature(&self, feature: CreateFeatureOpt) -> Result<i64> {
        match self {
            Self::Sqlite(db) => db.create_feature(feature).await,
        }
    }

    pub(crate) async fn update_feature(&self, id: i64, new_description: &str) -> Result<()> {
        match self {
            Self::Sqlite(db) => db.update_feature(id, new_description).await,
        }
    }

    pub(crate) async fn get_feature<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Feature>> {
        match self {
            Self::Sqlite(db) => db.get_feature(opt).await,
        }
    }

    pub(crate) async fn list_feature<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Feature>> {
        match self {
            Self::Sqlite(db) => db.list_feature(opt).await,
        }
    }

    pub(crate) async fn apply(&self, stage: apply::ApplyStage) -> Result<()> {
        match self {
            Self::Sqlite(db) => db.apply(stage).await,
        }
    }

    pub(crate) async fn list_entities_with_full_information<'a>(
        &self,
        opt: ListOpt<'a>,
    ) -> Result<Vec<Entity>> {
        match self {
            Self::Sqlite(db) => db.list_entity_with_full_information(opt).await,
        }
    }

    pub(crate) async fn list_group_with_full_information<'a>(
        &self,
        opt: ListOpt<'a>,
    ) -> Result<Vec<Group>> {
        match self {
            Self::Sqlite(db) => db.list_groups_with_full_information(opt).await,
        }
    }
}
