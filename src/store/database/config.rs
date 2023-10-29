#[allow(unused)]
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SQLiteOpt {
    pub db_file: String,
}
