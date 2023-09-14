pub mod apply;
pub mod entity;
pub mod opt;

use crate::database::metadata;
use opt::FeaStoreConfig;

pub struct FeaStore {
    metadata: metadata::DataStore,
}

impl FeaStore {
    pub async fn open(opt: FeaStoreConfig) -> FeaStore {
        let metadata_store = metadata::DataStore::open(opt.metadata).await;

        FeaStore {
            metadata: metadata_store,
        }
    }

    pub async fn close(&self) {
        self.metadata.close().await;
    }
}
