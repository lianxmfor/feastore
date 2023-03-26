pub mod sqlite;
pub mod types;

use async_trait::async_trait;

use crate::database::Result;
use self::types::{Entity, GetEntityOpt, ListEntityOpt};

#[async_trait]
trait DBStore {
    async fn create_entity(&self, name: &str, description: &str) -> Result<i64>;
    async fn update_entity(&self, id: i64, new_description: &str) -> Result<()>;
    async fn get_entity(&self, opt: GetEntityOpt) -> Result<Option<Entity>>;
    async fn list_entity(&self, ids: ListEntityOpt) -> Result<Vec<Entity>>;
}