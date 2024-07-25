mod apply;
mod get;
mod register;
mod update;

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
    #[arg(long, global(true))]
    config: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    pub async fn execute(self) {
        let cfg = self.init_config();
        let feastore = Store::open(cfg).await;

        match self.command {
            Commands::Apply(cmd) => cmd.run(feastore).await,
            //Commands::Edit(cmd) => cmd.execute(feastore).await,
            Commands::Register(cmd) => cmd.run(feastore).await,
            Commands::Update(cmd) => cmd.run(feastore).await,
            Commands::Get(cmd) => cmd.run(feastore).await,
        }
    }

    fn init_config(&self) -> FeatureStoreConfig {
        let config: String = if let Some(cfg) = &self.config {
            cfg.to_owned()
        } else if let Ok(cfg) = std::env::var("FEASTORE_CONFIG") {
            cfg
        } else {
            String::from("/Users/lianxm/.config/feastore/config.yaml")
        };

        let f = std::fs::File::open(&config).unwrap_or_else(|_| panic!("open {config} failed!"));

        serde_yaml::from_reader(f).unwrap()
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    cli.execute().await;
}
