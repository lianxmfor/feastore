pub mod opt;

use crate::database::metadata;
use opt::FeastoreOpt;

pub struct FeaStore {
    metadata: Box<dyn metadata::DBStore>,
}

impl FeaStore {
    pub async fn open(opt: FeastoreOpt) -> FeaStore {
        let metadata_store = metadata::open(opt.metadata).await;

        FeaStore { metadata: Box::new(metadata_store) }
    }
}
