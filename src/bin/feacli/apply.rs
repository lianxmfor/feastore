use std::fs;

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
    pub async fn run(self, store: Store) {
        let reader = fs::OpenOptions::new()
            .read(true)
            .open(&self.filepath)
            .expect("open file failed.");

        store.apply(reader).await.expect("apply failed.")
    }
}
