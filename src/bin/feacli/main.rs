mod apply;
mod get;
mod register;
mod update;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
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
    pub async fn execute(self) {
        let cfg = self.get_config();
        let feastore = Store::open(cfg).await;

        match self.command {
            Commands::Apply(cmd) => cmd.run(feastore).await,
            //Commands::Edit(cmd) => cmd.execute(feastore).await,
            Commands::Register(cmd) => cmd.run(feastore).await,
            Commands::Update(cmd) => cmd.run(feastore).await,
            Commands::Get(cmd) => cmd.run(feastore).await,
        }
    }

    fn get_config(&self) -> FeatureStoreConfig {
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
                    .unwrap()
            }
            None => config::Config::builder()
                .add_source(
                    config::Environment::with_prefix("FEASTORE")
                        .prefix_separator("_")
                        .separator("__"),
                )
                .build()
                .unwrap(),
        };

        settings.try_deserialize().unwrap()
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    cli.execute().await;
}
