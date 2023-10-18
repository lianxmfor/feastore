mod apply;
mod get;

use clap::{Parser, Subcommand};
use feastore::store::opt::FeaStoreConfig;
use feastore::store::Store;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, global(true))]
    config: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply a change
    Apply(apply::Command),
    Edit(get::Command),
    Register(get::Command),
    /// Get Resources
    Get(get::Command),
}

impl Cli {
    pub async fn execute(&self) {
        let cfg = self.init_config();
        let feastore = Store::open(cfg).await;

        match &self.command {
            Commands::Apply(cmd) => cmd.execute(feastore).await,
            Commands::Edit(cmd) => cmd.execute(feastore).await,
            Commands::Register(cmd) => cmd.execute(feastore).await,
            Commands::Get(cmd) => cmd.execute(feastore).await,
        }
    }
}

impl Cli {
    fn init_config(&self) -> FeaStoreConfig {
        let config: String = if let Some(cfg) = &self.config {
            cfg.to_owned()
        } else if let Ok(cfg) = std::env::var("FEASTORE_CONFIG") {
            cfg
        } else {
            String::from("/Users/lianxm/.config/feastore/config.yaml")
        };

        let f = std::fs::File::open(&config).expect(&format!("open {config} failed!"));

        serde_yaml::from_reader(f).unwrap()
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    cli.execute().await;
}
