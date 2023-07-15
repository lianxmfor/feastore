mod get;

use clap::{Parser, Subcommand};

use feastore::FeaStore;
use feastore::opt::FeastoreConfig;
use get::GetCommands;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Apply(GetCommands),
    Edit(GetCommands),
    Register(GetCommands),
    Get(GetCommands),

}

impl Commands {
    fn execute(&self, feastore: FeaStore) {
        match self {
            Self::Apply(cmd) => cmd.execute(feastore),
            Self::Edit(cmd) => cmd.execute(feastore),
            Self::Register(cmd) => cmd.execute(feastore),
            Self::Get(cmd) => cmd.execute(feastore),
        }  
    }
}

fn init_config() -> FeastoreConfig {
    todo!()    
}

fn must_open_feastore(_cfg: FeastoreConfig) -> FeaStore {
    todo!()
}

fn main() {
    let cli = Cli::parse();
    cli.command.execute(must_open_feastore(init_config()));
}
