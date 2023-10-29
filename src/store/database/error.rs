#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("found {0} already exist in database")]
    ColumnAlreadyExist(String),

    #[error("column {1} not found from table {0}")]
    ColumnNotFound(String, String),

    #[error("{0}")]
    SqlxError(String),

    #[error("{0}")]
    Other(String),
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<&'static str> for Error {
    fn from(value: &'static str) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::SqlxError(value.to_string())
    }
}
