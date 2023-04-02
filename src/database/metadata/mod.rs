pub mod sqlite;
pub mod types;

use async_trait::async_trait;

use self::types::{
    CreateGroupOpt, Entity, GetEntityOpt, GetGroupOpt, Group, ListEntityOpt, ListGroupOpt,
};
use crate::database::Result;

#[async_trait]
trait DBStore {
    async fn create_entity(&self, name: &str, description: &str) -> Result<i64>;
    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_entity(&self, opt: GetEntityOpt) -> Result<Option<Entity>>;
    async fn list_entity(&self, opt: ListEntityOpt) -> Result<Vec<Entity>>;

    async fn create_group(&self, group: CreateGroupOpt) -> Result<i64>;
    async fn update_group(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_group(&self, opt: GetGroupOpt) -> Result<Option<Group>>;
    async fn list_group(&self, opt: ListGroupOpt) -> Result<Vec<Group>>;
}
