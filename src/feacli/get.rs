use clap::{Args, Subcommand};

use feastore::store::metadata::types::ListOpt;
use feastore::store::Store;

#[derive(Debug, Subcommand)]
enum SubCmd {
    /// Get existing entity given specific conditions
    Entity(GetEntity),
    /// Get existing entity given specific conditions
    Group(GetGroup),
    /// Get existing entity given specific conditions
    Feature(GetFeature),
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Command {
    /// show detailed information
    #[arg(short, long, global(true))]
    wide: bool,
    #[command(subcommand)]
    cmds: SubCmd,
}

impl Command {
    pub async fn execute(&self, feastore: Store) {
        match &self.cmds {
            SubCmd::Entity(cmd) => cmd.execute(feastore, self.wide).await,
            SubCmd::Group(cmd) => cmd.execute(feastore, self.wide).await,
            SubCmd::Feature(cmd) => cmd.execute(feastore, self.wide).await,
        }
    }
}

#[derive(Debug, Args)]
struct GetEntity {
    entity_names: Vec<String>,
}

impl GetEntity {
    pub async fn execute(&self, store: Store, wide: bool) {
        let opt = ListOpt::from(&self.entity_names);
        let entities = store.list_entity(opt).await.expect("get entity failed.");

        match wide {
            true => {
                println!(
                    "{: <5} {: <10} {: <20} {: <23} {}",
                    "ID", "NAME", "DESCRIPTION", "CREATE-TIME", "MODIFY-TIME"
                );
                for e in entities {
                    println!(
                        "{: <5} {: <10} {: <20} {: <10} {}",
                        e.id, e.name, e.description, e.create_time, e.modify_time
                    );
                }
            }
            false => {
                println!("{: <5} {: <10} {: <20}", "ID", "NAME", "DESCRIPTION");
                for e in entities {
                    println!("{: <5} {: <10} {: <20}", e.id, e.name, e.description,);
                }
            }
        }
    }
}

#[derive(Debug, Args)]
struct GetGroup {
    group_names: Vec<String>,
}

impl GetGroup {
    pub async fn execute(&self, store: Store, wide: bool) {
        let opt = ListOpt::from(&self.group_names);
        let groups = store.list_group(opt).await.expect("get group failed.");
        for g in groups {
            println!(
                "{},{},{:?},{},{},{}",
                g.id, g.name, g.category, g.description, g.create_time, g.modify_time,
            );
        }
    }
}

#[derive(Debug, Args)]
struct GetFeature {
    feature_names: Vec<String>,
}

impl GetFeature {
    pub async fn execute(&self, store: Store, wide: bool) {
        let opt = ListOpt::from(&self.feature_names);
        let groups = store.list_group(opt).await.expect("get feature failed.");

        for g in groups {
            println!(
                "{},{},{:?},{},{},{}",
                g.id, g.name, g.category, g.description, g.create_time, g.modify_time
            );
        }
    }
}
