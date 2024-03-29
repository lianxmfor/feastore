use serde::{Deserialize, Serialize};

use crate::store::database::SQLiteOpt;

#[derive(Serialize, Deserialize)]
pub struct FeaStoreConfig {
    pub metadata: BackendOpt,
}

#[derive(Serialize, Deserialize)]
pub struct BackendOpt {
    pub sqlite: Option<SQLiteOpt>,
}
