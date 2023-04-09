pub mod error;
pub mod metadata;
pub mod config;

pub use config::SQLiteOpt;

pub type Result<T> = std::result::Result<T, error::Error>;
