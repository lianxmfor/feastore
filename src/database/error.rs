use sqlx::error::DatabaseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
   #[error("found {0} already exist in database")]
   ColumnAlreadyExist(String),

   #[error("sqlx error")]
   SqlxError(#[from] sqlx::Error),

   #[error("database error")]
   DatabaseError(#[from] Box<dyn DatabaseError>),
}