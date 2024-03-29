pub mod apply;
pub mod database;
pub mod types;

pub use database::metadata;
pub use types::FeaStoreConfig;

use apply::Stage;
use database::Result;
use metadata::{DataStore, Entity, Feature, GetOpt, Group, ListOpt};

pub struct Store {
    metadata: DataStore,
}

impl Store {
    pub async fn open(opt: FeaStoreConfig) -> Store {
        let metadata_store = DataStore::open(opt.metadata).await;

        Store {
            metadata: metadata_store,
        }
    }

    pub async fn apply<R: std::io::Read>(&self, r: R) -> Result<()> {
        let stage = Stage::from_reader(r)?;
        self.metadata.apply(stage).await
    }

    pub async fn close(&self) {
        self.metadata.close().await;
    }

    pub async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        self.metadata.create_entity(name, description).await
    }

    pub async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        self.metadata.update_entity(id, new_description).await
    }

    pub async fn get_entity<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Entity>> {
        self.metadata.get_entity(opt).await
    }

    pub async fn get_group<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Group>> {
        self.metadata.get_group(opt).await
    }

    pub async fn get_feature<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Feature>> {
        self.metadata.get_feature(opt).await
    }

    pub async fn list_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Entity>> {
        self.metadata.list_entity(opt).await
    }

    pub async fn list_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Group>> {
        self.metadata.list_group(opt).await
    }

    pub async fn list_feature<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Feature>> {
        self.metadata.list_feature(opt).await
    }
}
