pub mod apply;
pub mod database;
pub mod types;

pub use database::metadata;
pub use types::FeaStoreConfig;

use apply::{ApplyOpt, ApplyStage};
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

    pub async fn apply<R: std::io::Read>(&self, opt: ApplyOpt<R>) -> Result<()> {
        let stage = ApplyStage::from_opt(opt)?;
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

    pub async fn get_entity(&self, opt: GetOpt) -> Result<Option<Entity>> {
        self.metadata.get_entity(opt).await
    }

    pub async fn get_group(&self, opt: GetOpt) -> Result<Option<Group>> {
        self.metadata.get_group(opt).await
    }

    pub async fn get_feature(&self, opt: GetOpt) -> Result<Option<Feature>> {
        self.metadata.get_feature(opt).await
    }

    pub async fn list_entity(&self, opt: ListOpt) -> Result<Vec<Entity>> {
        self.metadata.list_entity(opt).await
    }

    pub async fn list_group(&self, opt: ListOpt) -> Result<Vec<Group>> {
        self.metadata.list_group(opt).await
    }

    pub async fn list_feature(&self, opt: ListOpt) -> Result<Vec<Feature>> {
        self.metadata.list_feature(opt).await
    }
}
