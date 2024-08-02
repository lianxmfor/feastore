use clap::{Args, Subcommand};
use feastore::database::metadata::{
    CreateFeatureOpt, CreateGroupOpt, FeatureValueType, GetOpt, GroupCategory,
};
use feastore::{Result, Store};

#[derive(Args)]
pub struct RegisterCommand {
    #[command(subcommand)]
    cmds: SubCmd,
}

#[derive(Subcommand)]
enum SubCmd {
    /// Register a new entity
    Entity(RegisterEntity),
    /// Register a new group
    Group(RegisterGroup),
    /// Register a new feature
    Feature(RegisterFeature),
}

#[derive(Args)]
struct RegisterEntity {
    name: String,
    #[arg(short, long)]
    description: String,
}

#[derive(Args)]
struct RegisterGroup {
    name: String,
    #[arg(short, long)]
    entity: String,
    #[arg(short, long)]
    category: GroupCategory,
    #[arg(short, long)]
    snapshot_interval: Option<i32>,
    #[arg(short, long)]
    description: String,
}

#[derive(Args)]
struct RegisterFeature {
    name: String,
    #[arg(short, long)]
    group: String,
    #[arg(short, long)]
    value_type: FeatureValueType,
    #[arg(short, long)]
    description: String,
}

impl RegisterCommand {
    pub async fn run(self, store: Store) -> Result<()> {
        match self.cmds {
            SubCmd::Entity(entity) => register_entity(entity, store).await,
            SubCmd::Group(group) => register_group(group, store).await,
            SubCmd::Feature(feature) => register_feature(feature, store).await,
        }
    }
}

async fn register_entity(entity: RegisterEntity, store: Store) -> Result<()> {
    store
        .create_entity(&entity.name, &entity.description)
        .await
        .map(|_| ())
}

async fn register_group(group: RegisterGroup, store: Store) -> Result<()> {
    let entity_id = if let Ok(Some(entity)) = store.get_entity(GetOpt::Name(&group.entity)).await {
        entity.id
    } else {
        return Ok(());
    };

    let opt = CreateGroupOpt {
        entity_id,
        name: group.name,
        category: group.category,
        snapshot_interval: group.snapshot_interval,
        description: group.description,
    };

    store.create_group(opt).await.map(|_| ())
}

async fn register_feature(feature: RegisterFeature, store: Store) -> Result<()> {
    let group_id = if let Ok(Some(group)) = store.get_group(GetOpt::Name(&feature.group)).await {
        group.id
    } else {
        return Ok(());
    };

    let opt = CreateFeatureOpt {
        group_id,
        feature_name: feature.name,
        value_type: feature.value_type,
        description: feature.description,
    };

    store.create_feature(opt).await.map(|_| ())
}
