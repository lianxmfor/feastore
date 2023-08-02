pub mod sqlite;
pub mod types;

use async_trait::async_trait;

use crate::database::metadata::types::*;
use crate::database::Result;
use crate::feastore::opt::BackendOpt;

#[async_trait]
pub trait DBStore {
    async fn close(&self);

    async fn create_entity(&self, name: &str, description: &str) -> Result<i64>;
    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_entity(&self, opt: GetOpt) -> Result<Option<Entity>>;
    async fn list_entity(&self, opt: ListOpt) -> Result<Vec<Entity>>;

    async fn create_group(&self, group: CreateGroupOpt) -> Result<i64>;
    async fn update_group(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_group(&self, opt: GetOpt) -> Result<Option<Group>>;
    async fn list_group(&self, opt: ListOpt) -> Result<Vec<Group>>;

    async fn create_feature(&self, feature: CreateFeatureOpt) -> Result<i64>;
    async fn update_feature(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_feature(&self, opt: GetOpt) -> Result<Option<Feature>>;
    async fn list_feature(&self, opt: ListOpt) -> Result<Vec<Feature>>;
}

pub async fn open(opt: BackendOpt) -> impl DBStore {
    if let Some(opt) = opt.sqlite {
        sqlite::DB::from(opt).await
    } else {
        panic!("not backend founed here!")
    }
}
