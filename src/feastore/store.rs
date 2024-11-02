use crate::database::metadata::{
    CreateFeatureOpt, CreateGroupOpt, DataStore, Entity, Feature, GetOpt, Group, ListFeatureOpt,
    ListOpt, RichEntity, RichFeature, RichGroup,
};
use crate::feastore::{apply, FeatureStoreConfig, Result};
use crate::Error;

pub struct Store {
    metadata: DataStore,
}

impl Store {
    pub async fn open(opt: FeatureStoreConfig) -> Store {
        let metadata_store = DataStore::open(opt.metadata).await;

        Store {
            metadata: metadata_store,
        }
    }

    pub async fn apply<R: std::io::Read>(&self, r: R) -> Result<()> {
        let stage = apply::ApplyStage::from_reader(r)?;
        self.metadata.apply(stage).await.map_err(|e| e.into())
    }

    pub async fn close(&self) {
        self.metadata.close().await;
    }

    pub async fn create_entity(&self, name: &str, description: &str) -> Result<i64> {
        self.metadata
            .create_entity(name, description)
            .await
            .map_err(|e| e.into())
    }

    pub async fn update_entity(&self, id: i64, new_description: &str) -> Result<()> {
        self.metadata
            .update_entity(id, new_description)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_entity<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Entity>> {
        self.metadata.get_entity(opt).await.map_err(|e| e.into())
    }

    pub async fn create_group(&self, opt: CreateGroupOpt) -> Result<i64> {
        self.metadata.create_group(opt).await.map_err(|e| e.into())
    }

    pub async fn get_group<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Group>> {
        self.metadata.get_group(opt).await.map_err(|e| e.into())
    }

    pub async fn update_group(&self, id: i64, new_description: &str) -> Result<()> {
        self.metadata
            .update_group(id, new_description)
            .await
            .map_err(|e| e.into())
    }

    pub async fn create_feature(&self, opt: CreateFeatureOpt) -> Result<i64> {
        self.metadata
            .create_feature(opt)
            .await
            .map_err(|e| e.into())
    }

    pub async fn get_feature<'a>(&self, opt: GetOpt<'a>) -> Result<Option<Feature>> {
        self.metadata.get_feature(opt).await.map_err(|e| e.into())
    }

    pub async fn update_feature(&self, id: i64, new_description: &str) -> Result<()> {
        self.metadata
            .update_feature(id, new_description)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Entity>> {
        self.metadata.list_entity(opt).await.map_err(|e| e.into())
    }

    pub async fn list_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<Group>> {
        self.metadata.list_group(opt).await.map_err(|e| e.into())
    }

    pub async fn list_rich_entity<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<RichEntity>> {
        self.metadata
            .list_rich_entity(opt)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list_rich_group<'a>(&self, opt: ListOpt<'a>) -> Result<Vec<RichGroup>> {
        self.metadata
            .list_rich_group(opt)
            .await
            .map_err(|e| e.into())
    }

    pub async fn list_rich_feature(&self, feature_names: &[String]) -> Result<Vec<RichFeature>> {
        Ok(self
            .list_feature2(feature_names)
            .await?
            .into_iter()
            .map(|f| RichFeature::from2(f))
            .collect())
    }

    pub async fn list_feature2(&self, feature_names: &[String]) -> Result<Vec<Feature>> {
        let features = self
            .metadata
            .list_feature2(ListFeatureOpt::EntityIDs(vec![1]))
            .await
            .map_err(|e| Error::from(e))?;

        let features = if feature_names.is_empty() {
            features
        } else {
            features
                .into_iter()
                .filter(|f| is_contains(&f.full_name(), feature_names))
                .collect()
        };

        Ok(features)
    }
}

fn is_contains(name: &str, names: &[String]) -> bool {
    names.iter().any(|n| n == name)
}
