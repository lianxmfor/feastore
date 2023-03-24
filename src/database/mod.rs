pub mod metadata;
pub mod error;

pub type Result<T> = std::result::Result<T, error::Error>;