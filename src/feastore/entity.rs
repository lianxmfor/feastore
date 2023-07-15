use super::FeaStore;
use crate::database::metadata::types::{Entity, ListOpt};
use crate::database::Result;

impl FeaStore {
    pub async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        self.metadata.create_entity(name, description).await
    }

    pub async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        self.metadata.update_entity(id, new_description).await
    }

    pub async fn list_entity(&self, opt: ListOpt) -> Result<Vec<Entity>> {
        self.metadata.list_entity(opt).await
    }
}
