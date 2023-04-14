pub mod sqlite;
pub mod types;

use async_trait::async_trait;

use self::types::{
    CreateFeatureOpt, CreateGroupOpt, Entity, Feature, GetEntityOpt, GetFeatureOpt, GetGroupOpt,
    Group, ListEntityOpt, ListFeatureOpt, ListGroupOpt,
};
use crate::database::Result;
use crate::feastore::opt::BackendOpt;

#[async_trait]
pub trait DBStore {
    async fn close(&self);

    async fn create_entity(&self, name: &str, description: &str) -> Result<i64>;
    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_entity(&self, opt: GetEntityOpt) -> Result<Option<Entity>>;
    async fn list_entity(&self, opt: ListEntityOpt) -> Result<Vec<Entity>>;

    async fn create_group(&self, group: CreateGroupOpt) -> Result<i64>;
    async fn update_group(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_group(&self, opt: GetGroupOpt) -> Result<Option<Group>>;
    async fn list_group(&self, opt: ListGroupOpt) -> Result<Vec<Group>>;

    async fn create_feature(&self, feature: CreateFeatureOpt) -> Result<i64>;
    async fn update_feature(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_feature(&self, opt: GetFeatureOpt) -> Result<Option<Feature>>;
    async fn list_feature(&self, opt: ListFeatureOpt) -> Result<Vec<Feature>>;
}

pub async fn open(opt: BackendOpt) -> impl DBStore {
    match opt {
        BackendOpt::SQLite(opt) => sqlite::DB::from(opt).await,
    }
}
