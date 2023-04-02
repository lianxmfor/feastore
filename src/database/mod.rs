pub mod error;
pub mod metadata;

pub type Result<T> = std::result::Result<T, error::Error>;
