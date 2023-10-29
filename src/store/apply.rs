use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};
use serde_yaml as yaml;

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use crate::store::database::Result;
use crate::store::metadata::types::{Category, FeatureValueType};

pub struct ApplyOpt<R: std::io::Read> {
    pub r: R,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ApplyStage {
    pub new_entities: Vec<Entity>,
    pub new_groups: Vec<Group>,
    pub new_features: Vec<Feature>,
}

impl ApplyStage {
    pub fn from_opt<R: Read>(opt: ApplyOpt<R>) -> Result<Self> {
        let mut stage = ApplyStage::empty();

        for de in yaml::Deserializer::from_reader(opt.r) {
            let value = yaml::Value::deserialize(de).expect("Unable to parse");
            let sub_stage = Self::from_value(value)?;
            stage.merge(sub_stage);
        }

        Ok(stage)
    }
}

impl ApplyStage {
    fn empty() -> Self {
        Self {
            new_entities: Vec::new(),
            new_groups: Vec::new(),
            new_features: Vec::new(),
        }
    }

    fn from_value(value: yaml::Value) -> Result<Self> {
        match parse_kind(&value) {
            Some("Entity") => {
                let entity: Entity = yaml::from_value(value).expect("Unable to parse");
                Ok(Self::from_entity(entity))
            }
            Some("Group") => {
                let group: Group = yaml::from_value(value).expect("Unable to parse");
                Ok(Self::from_group(group))
            }
            Some("Feature") => {
                let feature: Feature = yaml::from_value(value).expect("Unable to parse");
                Ok(Self::from_feature(feature))
            }
            Some("Items") | Some("items") => {
                let items = if value["items"].is_sequence() {
                    "items"
                } else {
                    "Items"
                };

                let mut stage = Self::empty();
                match parse_items_kind(&value) {
                    Some("Entity") => {
                        let mut entities: HashMap<String, Vec<Entity>> =
                            yaml::from_value(value).expect("Unable to parse");

                        for entity in entities.remove(items).unwrap_or_default() {
                            stage.merge(Self::from_entity(entity));
                        }
                    }
                    Some("Group") => {
                        let mut groups: HashMap<String, Vec<Group>> =
                            yaml::from_value(value).expect("Unable to parse");

                        for group in groups.remove(items).unwrap_or_default() {
                            stage.merge(Self::from_group(group));
                        }
                    }
                    Some("Feature") | Some("Features") => {
                        let mut features: HashMap<String, Vec<Feature>> =
                            yaml::from_value(value).expect("Unable to parse");

                        for f in features.remove(items).unwrap_or_default() {
                            stage.merge(Self::from_feature(f));
                        }
                    }
                    Some(kind) => return Err(format!("invalid kind '{}'", kind).into()),
                    None => return Err("invalid yaml: missing kind or items".into()),
                }

                Ok(stage)
            }
            Some(kind) => Err(format!("invalid kind '{}'", kind).into()),
            None => Err("invalid yaml: missing kind or items".into()),
        }
    }

    fn from_entity(entity: Entity) -> Self {
        let mut stage = ApplyStage::empty();

        stage.new_entities.push(entity.flat());
        for g in entity.groups.unwrap_or_default() {
            stage.new_groups.push(g.flat(Some(entity.name.clone())));

            for feature in g.features.unwrap_or_default() {
                stage.new_features.push(feature.fill(Some(g.name.clone())));
            }
        }

        stage
    }

    fn from_group(group: Group) -> Self {
        let mut stage = ApplyStage::empty();

        stage
            .new_groups
            .push(group.flat(group.entity_name.to_owned()));

        for f in group.features.unwrap_or_default() {
            stage.new_features.push(f.fill(Some(group.name.to_owned())));
        }

        stage
    }

    fn from_feature(feature: Feature) -> Self {
        let mut stage = ApplyStage::empty();

        stage
            .new_features
            .push(feature.fill(feature.group_name.to_owned()));
        stage
    }

    fn merge(&mut self, other: Self) {
        self.new_entities.extend(other.new_entities);
        self.new_groups.extend(other.new_groups);
        self.new_features.extend(other.new_features);
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Feature {
    pub kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "group-name", deserialize = "group-name"))]
    pub group_name: Option<String>,
    #[serde(rename(serialize = "value-type", deserialize = "value-type"))]
    pub value_type: FeatureValueType,
    pub description: String,
}

impl Feature {
    fn fill(&self, group_name: Option<String>) -> Self {
        Self {
            kind: Some("Feature".to_string()),
            name: self.name.to_owned(),
            group_name,
            value_type: self.value_type.to_owned(),
            description: self.description.to_owned(),
        }
    }
}

#[serde_as]
#[derive(Deserialize, Debug, PartialEq)]
pub struct Group {
    kind: Option<String>,
    pub name: String,
    #[serde(rename(serialize = "entity-name", deserialize = "entity-name"))]
    pub entity_name: Option<String>,
    pub category: Category,
    #[serde(rename(serialize = "snapshot-interval", deserialize = "snapshot-interval"))]
    // TODO: instead serde_as with github.com/jean-airoldie/humantime-serde
    #[serde_as(as = "Option<DurationSeconds>")]
    snapshot_interval: Option<Duration>,
    pub description: String,

    features: Option<Vec<Feature>>,
}

impl Group {
    fn flat(&self, entity_name: Option<String>) -> Self {
        Self {
            kind: Some("Group".to_string()),
            name: self.name.to_owned(),
            entity_name,
            category: self.category.to_owned(),
            snapshot_interval: self.snapshot_interval,
            description: self.description.to_owned(),
            features: None,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct Entity {
    kind: Option<String>,
    pub name: String,
    pub description: String,

    groups: Option<Vec<Group>>,
}

impl Entity {
    fn flat(&self) -> Self {
        Self {
            kind: Some("Entity".to_string()),
            name: self.name.to_owned(),
            description: self.description.to_owned(),
            groups: None,
        }
    }
}

fn parse_kind(value: &yaml::Value) -> Option<&str> {
    if value["kind"].is_string() {
        return value["kind"].as_str();
    }
    if value["items"].is_sequence() {
        return Some("items");
    }
    if value["Items"].is_sequence() {
        return Some("Items");
    }
    None
}

fn parse_items_kind(value: &yaml::Value) -> Option<&str> {
    if let Some(sequence) = value["items"].as_sequence() {
        if !sequence.is_empty() {
            return sequence[0]["kind"].as_str();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::database::metadata::types::Category::{Batch, Stream};
    use std::time::Duration;

    #[test]
    fn test_build_apply_stage() {
        struct TestCase {
            description: &'static str,
            opt: ApplyOpt<&'static [u8]>,

            want: Result<ApplyStage>,
        }

        let test_cases = vec![
            TestCase {
                description: "invalid yaml: missing kind or items",
                opt: ApplyOpt {
                    r: r#"
# kind: Entity
name: user
description: 'User ID'
"#
                    .as_bytes(),
                },
                want: Err(s("invalid yaml: missing kind or items").into()),
            },
            TestCase {
                description: "invalid kind",
                opt: ApplyOpt {
                    r: r#"
kind: Entit
name: user
description: 'description'
"#
                    .as_bytes(),
                },
                want: Err(s("invalid kind 'Entit'").into()),
            },
            TestCase {
                description: "single entity",
                opt: ApplyOpt {
                    r: r#"
kind: Entity
name: user
description: 'description'
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![Entity {
                        kind: Some(s("Entity")),
                        name: s("user"),
                        description: s("description"),
                        groups: None,
                    }],
                    new_groups: vec![],
                    new_features: vec![],
                }),
            },
            TestCase {
                description: "has many simple objects",
                opt: ApplyOpt {
                    r: r#"
kind: Entity
name: user
description: 'description'
---
kind: Group
name: account
entity-name: user
category: batch
description: 'description'
---
kind: Group
name: device
entity-name: user
category: batch
description: 'description'
---
kind: Group
name: user-click
entity-name: user
category: stream
snapshot-interval: 86400
description: 'description'
---
kind: Feature
name: model
group-name: device
category: batch
value-type: string
description: 'description'
---
kind: Feature
name: price
group-name: device
category: batch
value-type: int64
description: 'description'
                "#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![Entity {
                        kind: Some(s("Entity")),
                        name: s("user"),
                        description: s("description"),
                        groups: None,
                    }],
                    new_groups: vec![
                        Group {
                            kind: Some(s("Group")),
                            name: s("account"),
                            entity_name: Some(s("user")),
                            category: Batch,
                            snapshot_interval: None,
                            description: s("description"),
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("device"),
                            entity_name: Some(s("user")),
                            category: Batch,
                            snapshot_interval: None,
                            description: s("description"),
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("user-click"),
                            entity_name: Some(s("user")),
                            category: Stream,
                            snapshot_interval: Some(Duration::from_secs(86400)),
                            description: s("description"),
                            features: None,
                        },
                    ],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                    ],
                }),
            },
            TestCase {
                description: "comlex group",
                opt: ApplyOpt {
                    r: r#"
kind: Group
name: device
entity-name: user
category: batch
description: 'description'
features:
- name: model
  value-type: string
  description: 'description'
- name: price
  value-type: int64
  description: 'description'   
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![Group {
                        kind: Some(s("Group")),
                        entity_name: Some(s("user")),
                        name: s("device"),
                        category: Batch,
                        snapshot_interval: None,
                        description: s("description"),
                        features: None,
                    }],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                    ],
                }),
            },
            TestCase {
                description: "complex entity",
                opt: ApplyOpt {
                    r: r#"
kind: Entity
name: user
description: 'description'
groups:
- name: device
  category: batch
  description: description
  features:
  - name: model
    value-type: string
    description: 'description'
  - name: price
    value-type: int64
    description: 'description'
- name: user
  category: batch
  description: description
  features:
  - name: age
    value-type: int64
    description: 'description'
  - name: gender
    value-type: int64
    description: 'description'
- name: user-click
  category: stream
  snapshot-interval: 86400
  description: description
  features:
  - name: last_5_click_posts
    value-type: string
    description: 'description'
  - name: number_of_user_started_posts
    value-type: int64
    description: 'description'
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![Entity {
                        kind: Some(s("Entity")),
                        name: s("user"),
                        description: s("description"),
                        groups: None,
                    }],
                    new_groups: vec![
                        Group {
                            kind: Some(s("Group")),
                            name: s("device"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: None,
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("user"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: None,
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("user-click"),
                            category: Stream,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: Some(Duration::from_secs(86400)),
                            features: None,
                        },
                    ],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("age"),
                            group_name: Some(s("user")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("gender"),
                            group_name: Some(s("user")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("last_5_click_posts"),
                            group_name: Some(s("user-click")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("number_of_user_started_posts"),
                            group_name: Some(s("user-click")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                    ],
                }),
            },
            TestCase {
                description: "feature slice",
                opt: ApplyOpt {
                    r: r#"
items:
    - kind: Feature
      name: credit_score
      group-name: account
      value-type: int64
      description: "credit_score description"
    - kind: Feature
      name: account_age_days
      group-name: account
      value-type: int64
      description: "account_age_days description"
    - kind: Feature
      name: has_2fa_installed
      group-name: account
      value-type: bool
      description: "has_2fa_installed description"
    - kind: Feature
      name: transaction_count_7d
      group-name: transaction_stats
      value-type: int64
      description: "transaction_count_7d description"
    - kind: Feature
      name: transaction_count_30d
      group-name: transaction_stats
      value-type: int64
      description: "transaction_count_30d description"
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_30d description"),
                        },
                    ],
                }),
            },
            TestCase {
                description: "group slice",
                opt: ApplyOpt {
                    r: r#"
items:
    - kind: Group
      name: account
      entity-name: user
      category: batch
      description: user account info
      features:
        - name: credit_score
          value-type: int64
          description: credit_score description
        - name: account_age_days
          value-type: int64
          description: account_age_days description
        - name: has_2fa_installed
          value-type: bool
          description: has_2fa_installed description
    - kind: Group
      name: transaction_stats
      entity-name: user
      category: batch
      description: user transaction statistics
      features:
        - name: transaction_count_7d
          value-type: int64
          description: transaction_count_7d description
        - name: transaction_count_30d
          value-type: int64
          description: transaction_count_30d description
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![
                        Group {
                            kind: Some(s("Group")),
                            name: s("account"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user account info"),
                            snapshot_interval: None,
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("transaction_stats"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user transaction statistics"),
                            snapshot_interval: None,
                            features: None,
                        },
                    ],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_30d description"),
                        },
                    ],
                }),
            },
            TestCase {
                description: "entity slice",
                opt: ApplyOpt {
                    r: r#"
items:
    - kind: Entity
      name: user
      description: user ID
      groups:
        - name: account
          category: batch
          description: user account info
          features:
            - name: credit_score
              value-type: int64
              description: credit_score description
            - name: account_age_days
              value-type: int64
              description: account_age_days description
            - name: has_2fa_installed
              value-type: bool
              description: has_2fa_installed description
        - name: transaction_stats
          category: batch
          description: user transaction statistics
          features:
            - name: transaction_count_7d
              value-type: int64
              description: transaction_count_7d description
            - name: transaction_count_30d
              value-type: int64
              description: transaction_count_30d description
    - kind: Entity
      name: device
      description: device info
      groups:
        - name: phone
          category: batch
          description: phone info
          features:
            - name: model
              value-type: string
              description: model description
            - name: price
              value-type: int64
              description: price description
"#
                    .as_bytes(),
                },
                want: Ok(ApplyStage {
                    new_entities: vec![
                        Entity {
                            kind: Some(s("Entity")),
                            name: s("user"),
                            description: s("user ID"),
                            groups: None,
                        },
                        Entity {
                            kind: Some(s("Entity")),
                            name: s("device"),
                            description: s("device info"),
                            groups: None,
                        },
                    ],
                    new_groups: vec![
                        Group {
                            kind: Some(s("Group")),
                            name: s("account"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user account info"),
                            snapshot_interval: None,
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("transaction_stats"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user transaction statistics"),
                            snapshot_interval: None,
                            features: None,
                        },
                        Group {
                            kind: Some(s("Group")),
                            name: s("phone"),
                            category: Batch,
                            entity_name: Some(s("device")),
                            description: s("phone info"),
                            snapshot_interval: None,
                            features: None,
                        },
                    ],
                    new_features: vec![
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_30d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("model"),
                            group_name: Some(s("phone")),
                            value_type: FeatureValueType::StringType,
                            description: s("model description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("phone")),
                            value_type: FeatureValueType::Int64,
                            description: s("price description"),
                        },
                    ],
                }),
            },
        ];

        for case in test_cases {
            let stage = ApplyStage::from_opt(case.opt);
            assert_eq!(stage, case.want, "{}", case.description);
        }
    }

    #[test]
    fn test_parse_kind() {
        let entity = r#"
        kind: Entity
        name: user
        description: 'description'
        "#;
        let value: serde_yaml::Value = serde_yaml::from_str(entity).unwrap();
        assert_eq!(parse_kind(&value), Some("Entity"));

        let group = r#"
        kind: Group
        name: user
        description: 'description'
        "#;
        let value: serde_yaml::Value = serde_yaml::from_str(group).unwrap();
        assert_eq!(parse_kind(&value), Some("Group"));

        let feature = r#"
        kind: Feature
        name: user
        description: 'description'
        "#;
        let value: serde_yaml::Value = serde_yaml::from_str(feature).unwrap();
        assert_eq!(parse_kind(&value), Some("Feature"));

        let items = r#"
items:
    - kind: Feature
      name: credit_score
      group-name: account
      value-type: int64
      description: "credit_score description"
"#;

        let value: serde_yaml::Value = serde_yaml::from_str(items).unwrap();
        assert_eq!(parse_kind(&value), Some("items"));
    }

    #[test]
    fn test_parse_items_kind() {
        let items = r#"
items:
    - kind: Feature
      name: credit_score
      group-name: account
      value-type: int64
      description: "credit_score description"
    - kind: Feature
      name: account_age_days
      group-name: account
      value-type: int64
      description: "account_age_days description"
"#;

        let value: serde_yaml::Value = serde_yaml::from_str(items).unwrap();
        assert_eq!(parse_items_kind(&value), Some("Feature"));

        let items = r#"
items:
    - kind: Group
      name: account
      entity-name: user
      category: batch
      description: user account info
      features:
        - name: credit_score
          value-type: int64
          description: credit_score description
"#;
        let value: serde_yaml::Value = serde_yaml::from_str(items).unwrap();
        assert_eq!(parse_items_kind(&value), Some("Group"));
    }

    fn s(v: impl Into<String>) -> String {
        v.into()
    }
}
