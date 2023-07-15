use clap::{Args, Subcommand};

use crate::FeaStore;

#[derive(Debug, Subcommand)]
pub enum Commands {
    Entity,
    Group,
    Feature,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GetCommands {
    #[command(subcommand)]
    command: Option<Commands>,
}

impl GetCommands {
    pub fn execute(&self, _feastore: FeaStore) {
        todo!()
    }
}
