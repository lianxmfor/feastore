use std::fs;

use clap::Args;

use feastore::store::apply::ApplyOpt;
use feastore::store::Store;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Command {
    /// filepath
    #[arg(short, long)]
    filepath: String,
}

impl Command {
    pub async fn execute(&self, store: Store) {
        let reader = fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&self.filepath)
            .expect("open file failed.");

        store
            .apply(ApplyOpt { r: reader })
            .await
            .expect("apply failed.")
    }
}
