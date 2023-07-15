pub mod sqlite;
pub mod types;

use async_trait::async_trait;

use crate::database::Result;
use crate::feastore::opt::BackendOpt;

#[async_trait]
pub trait DBStore {
    async fn close(&self);

    async fn create_entity(&self, name: &str, description: &str) -> Result<i64>;
    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_entity(&self, opt: types::GetOpt) -> Result<Option<types::Entity>>;
    async fn list_entity(&self, opt: types::ListOpt) -> Result<Vec<types::Entity>>;

    async fn create_group(&self, group: types::CreateGroupOpt) -> Result<i64>;
    async fn update_group(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_group(&self, opt: types::GetOpt) -> Result<Option<types::Group>>;
    async fn list_group(&self, opt: types::ListOpt) -> Result<Vec<types::Group>>;

    async fn create_feature(&self, feature: types::CreateFeatureOpt) -> Result<i64>;
    async fn update_feature(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_feature(&self, opt: types::GetOpt) -> Result<Option<types::Feature>>;
    async fn list_feature(&self, opt: types::ListOpt) -> Result<Vec<types::Feature>>;
}

pub async fn open(opt: BackendOpt) -> impl DBStore {
    match opt {
        BackendOpt::SQLite(opt) => sqlite::DB::from(opt).await,
    }
}
