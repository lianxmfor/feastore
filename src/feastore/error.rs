pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("{0}")]
    DataExist(String),
    #[error("{0}")]
    DataNotFound(String),
    #[error("{0}")]
    SqlxError(String),
    #[error("{0}")]
    Error(String),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Self::Error(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self::Error(s.to_string())
    }
}

use crate::database::Error as MetadataError;
impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        match err {
            MetadataError::ColumnAlreadyExist(msg) => Self::DataExist(msg),
            MetadataError::ColumnNotFound(entity, id) => {
                Self::DataNotFound(format!("{} not found by id {}", entity, id))
            }
            MetadataError::SqlxError(msg) => Self::SqlxError(msg),
            MetadataError::Other(msg) => Self::Error(msg),
        }
    }
}
