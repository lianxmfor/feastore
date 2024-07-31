mod apply;
mod get;
mod register;
mod update;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use feastore::{FeatureStoreConfig, Store};

#[derive(Subcommand)]
enum Commands {
    /// Apply a change
    Apply(apply::ApplyCmd),
    /// Register a new resource
    Register(register::RegisterCommand),
    /// update a resource
    Update(update::UpdateCommand),
    /// Get Resources
    Get(get::Command),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, global(true), default_value = default_config_file().into_os_string())]
    config: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

fn default_config_file() -> PathBuf {
    let mut dir = dirs::home_dir().unwrap();
    dir.push(".config.yaml");
    dir
}

impl Cli {
    pub async fn execute(self) -> Result<()> {
        let cfg = self.get_config()?;
        let feastore = Store::open(cfg).await;

        let result = match self.command {
            Commands::Apply(cmd) => cmd.run(feastore).await,
            Commands::Register(cmd) => cmd.run(feastore).await,
            Commands::Update(cmd) => cmd.run(feastore).await,
            Commands::Get(cmd) => cmd.run(feastore).await,
        };

        result.map_err(|err| err.into())
    }

    fn get_config(&self) -> Result<FeatureStoreConfig> {
        let settings = match self.config {
            Some(ref config_path) => {
                let config_path = PathBuf::from(config_path);
                config::Config::builder()
                    .add_source(config::File::from(config_path))
                    .add_source(
                        config::Environment::with_prefix("FEASTORE")
                            .prefix_separator("_")
                            .separator("__"),
                    )
                    .build()
                    .context("Failed to get config. make sure the config path or envs which has prefix FEASTORE_ is provided.")?
            }
            None => config::Config::builder()
                .add_source(
                    config::Environment::with_prefix("FEASTORE")
                        .prefix_separator("_")
                        .separator("__"),
                )
                .build()
                .context("Failed to get config. make sure the config path or envs which has prefix FEASTORE_ is provided.")?
        };

        settings
            .try_deserialize()
            .context("Failed to deserialize config. make sure the config content is right")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.execute().await
}
