use sqlx::error::DatabaseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
   #[error("found {0} already exist in database")]
   ColumnAlreadyExist(String),

   #[error("column {1} not found from table {0}")]
   ColumnNotFound(String, String),

   #[error("sqlx error")]
   SqlxError(#[from] sqlx::Error),

   #[error("database error")]
   DatabaseError(#[from] Box<dyn DatabaseError>),
}