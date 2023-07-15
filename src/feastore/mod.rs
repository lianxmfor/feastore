pub mod entity;
pub mod opt;

use crate::database::metadata;
use opt::FeastoreConfig;

pub struct FeaStore {
    metadata: Box<dyn metadata::DBStore>,
}

impl FeaStore {
    pub async fn open(opt: FeastoreConfig) -> FeaStore {
        let metadata_store = metadata::open(opt.metadata).await;

        FeaStore {
            metadata: Box::new(metadata_store),
        }
    }

    pub async fn close(&self) {
        self.metadata.close().await;
    }
}
