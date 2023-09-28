#![feature(is_some_and)]

mod database;
mod feastore;

pub use crate::feastore::apply::ApplyOpt;
pub use crate::feastore::opt;
pub use crate::feastore::FeaStore;
pub use database::metadata;
pub use database::metadata::types;

pub use database::metadata::types::*;
