use clap::{Args, Subcommand};

use anyhow::Result;

use feastore::database::metadata::GetOpt;
use feastore::Store;

#[derive(Args)]
pub struct UpdateCommand {
    #[command(subcommand)]
    cmds: SubCmd,
}

#[derive(Subcommand)]
enum SubCmd {
    Entity(UpdateEntity),
    Group(UpdateGroup),
    Feature(UpdateFeature),
}

#[derive(Args)]
struct UpdateEntity {
    name: String,
    #[arg(short, long)]
    description: String,
}

#[derive(Args)]
struct UpdateGroup {
    name: String,
    #[arg(short, long)]
    description: String,
}

#[derive(Args)]
struct UpdateFeature {
    name: String,
    #[arg(short, long)]
    description: String,
}

impl UpdateCommand {
    pub async fn run(self, store: Store) -> Result<()> {
        match self.cmds {
            SubCmd::Entity(entity) => update_entity(entity, store).await,
            SubCmd::Group(group) => update_group(group, store).await,
            SubCmd::Feature(feature) => update_feature(feature, store).await,
        }
    }
}

async fn update_entity(entity: UpdateEntity, store: Store) -> Result<()> {
    let entity_id = if let Ok(Some(entity)) = store.get_entity(GetOpt::Name(&entity.name)).await {
        entity.id
    } else {
        return Ok(());
    };

    store
        .update_entity(entity_id, &entity.description)
        .await
        .map_err(|e| e.into())
}

async fn update_group(group: UpdateGroup, store: Store) -> Result<()> {
    let group_id = if let Ok(Some(group)) = store.get_group(GetOpt::Name(&group.name)).await {
        group.id
    } else {
        return Ok(());
    };

    store
        .update_group(group_id, &group.description)
        .await
        .map_err(|e| e.into())
}

async fn update_feature(feature: UpdateFeature, store: Store) -> Result<()> {
    let feature_id = if let Ok(Some(feature)) = store.get_feature(GetOpt::Name(&feature.name)).await
    {
        feature.id
    } else {
        return Ok(());
    };

    store
        .update_feature(feature_id, &feature.description)
        .await
        .map_err(|e| e.into())
}
