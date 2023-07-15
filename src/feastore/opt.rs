use crate::database::SQLiteOpt;

pub struct FeastoreConfig {
    pub metadata: BackendOpt,
}

pub enum BackendOpt {
    SQLite(SQLiteOpt),
}
