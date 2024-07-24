pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("found {0} already exist in database")]
    ColumnAlreadyExist(String),

    #[error("{0} not found by id {1}")]
    ColumnNotFound(String, String),

    #[error("{0}")]
    SqlxError(String),

    #[error("{0}")]
    Other(String),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self::Other(s.to_string())
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::SqlxError(value.to_string())
    }
}
