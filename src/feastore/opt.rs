use crate::database::SQLiteOpt;

pub struct FeastoreOpt {
    pub metadata: BackendOpt,
}

pub enum BackendOpt {
    SQLite(SQLiteOpt),
}
