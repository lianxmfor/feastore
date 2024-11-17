use std::fs;

use anyhow::{Context, Result};
use clap::Args;
use feastore::Store;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ApplyCmd {
    /// filepath
    #[arg(short, long)]
    filepath: String,
}

impl ApplyCmd {
    pub async fn run(self, store: Store) -> Result<()> {
        let reader = fs::OpenOptions::new()
            .read(true)
            .open(&self.filepath)
            .context("config file open failed.")?;

        store.apply(reader).await.map_err(|err| err.into())
    }
}
