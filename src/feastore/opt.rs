use serde::{Deserialize, Serialize};

use crate::database::SQLiteOpt;

#[derive(Serialize, Deserialize)]
pub struct FeastoreConfig {
    pub metadata: BackendOpt,
}

#[derive(Serialize, Deserialize)]
pub struct BackendOpt {
    pub sqlite: Option<SQLiteOpt>,
}
