use clap::{Args, Subcommand};

use feastore::{FeaStore, GetOpt};

#[derive(Debug, Subcommand)]
enum SubCommands {
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
    commands: SubCommands,
}

impl Command {
    pub async fn execute(&self, feastore: FeaStore) {
        match &self.commands {
            SubCommands::Entity(cmd) => cmd.execute(feastore).await,
            SubCommands::Group(cmd) => cmd.execute(feastore).await,
            SubCommands::Feature(cmd) => cmd.execute(feastore).await,
        }
    }
}

#[derive(Debug, Args)]
struct GetEntity {
    entity_name: String,
}

impl GetEntity {
    pub async fn execute(&self, store: FeaStore) {
        let entitiy = store
            .get_entity(GetOpt::Name(self.entity_name.to_string()))
            .await
            .expect("get failed.");

        if let Some(e) = entitiy {
            println!(
                "{},{},{},{},{}",
                e.id, e.name, e.description, e.create_time, e.modify_time
            );
        }
    }
}

#[derive(Debug, Args)]
struct GetGroup {
    group_name: String,
}

impl GetGroup {
    pub async fn execute(&self, store: FeaStore) {
        let group = store
            .get_group(GetOpt::Name(self.group_name.to_string()))
            .await
            .expect("get group failed.");

        if let Some(g) = group {
            println!(
                "{},{},{:?},{},{},{}",
                g.id, g.name, g.category, g.description, g.create_time, g.modify_time,
            );
        }
    }
}

#[derive(Debug, Args)]
struct GetFeature {
    feature_name: String,
}

impl GetFeature {
    pub async fn execute(&self, store: FeaStore) {
        let feature = store
            .get_feature(GetOpt::Name(self.feature_name.to_string()))
            .await
            .expect("get feature failed.");

        if let Some(f) = feature {
            println!(
                "{},{},{:?},{},{},{}",
                f.id, f.name, f.value_type, f.description, f.create_time, f.modify_time
            );
        }
    }
}
