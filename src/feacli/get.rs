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
    #[command(subcommand)]
    cmds: SubCmd,
}

impl Command {
    pub async fn execute(&self, feastore: Store) {
        match &self.cmds {
            SubCmd::Entity(cmd) => cmd.execute(feastore).await,
            SubCmd::Group(cmd) => cmd.execute(feastore).await,
            SubCmd::Feature(cmd) => cmd.execute(feastore).await,
        }
    }
}

#[derive(Debug, Args)]
struct GetEntity {
    entity_names: Vec<String>,
}

impl GetEntity {
    pub async fn execute(&self, store: Store) {
        let entities = store
            .list_entity(ListOpt::Names(self.entity_names.to_owned()))
            .await
            .expect("get entity failed.");

        for e in entities {
            println!(
                "{},{},{},{},{}",
                e.id, e.name, e.description, e.create_time, e.modify_time
            );
        }
    }
}

#[derive(Debug, Args)]
struct GetGroup {
    group_names: Vec<String>,
}

impl GetGroup {
    pub async fn execute(&self, store: Store) {
        let groups = store
            .list_group(ListOpt::Names(self.group_names.to_owned()))
            .await
            .expect("get group failed.");

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
    pub async fn execute(&self, store: Store) {}
}
