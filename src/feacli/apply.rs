use std::fs;

use clap::Args;
use feastore::ApplyOpt;
use feastore::FeaStore;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Command {
    /// filepath
    #[arg(short, long)]
    filepath: String,
}

impl Command {
    pub async fn execute(&self, feastore: FeaStore) {
        let reader = fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&self.filepath)
            .expect("open file failed.");

        feastore
            .apply(ApplyOpt { r: reader })
            .await
            .expect("apply failed.")
    }
}
