use serde::Deserialize;
use serde_yaml as yaml;

use std::collections::HashMap;
use std::io;

use crate::database::metadata::{ApplyEntity, ApplyFeature, ApplyGroup};
use crate::feastore::error::Result;

#[derive(Debug, PartialEq)]
pub(crate) struct ApplyStage {
    pub new_entities: Vec<ApplyEntity>,
    pub new_groups: Vec<ApplyGroup>,
    pub new_features: Vec<ApplyFeature>,
}

impl ApplyStage {
    fn new() -> Self {
        Self {
            new_entities: Vec::new(),
            new_groups: Vec::new(),
            new_features: Vec::new(),
        }
    }

    pub fn from_reader<R: io::Read>(r: R) -> Result<Self> {
        let mut stage = ApplyStage::new();

        for de in yaml::Deserializer::from_reader(r) {
            let v = yaml::Value::deserialize(de).expect("Unable to parse. fuck");
            let sub_stage = Self::from_value(v)?;
            stage.merge(sub_stage);
        }

        Ok(stage)
    }

    fn from_value(value: yaml::Value) -> Result<Self> {
        let mut stage = Self::new();
        match parse_kind(&value) {
            Some("Entity") => {
                let entity: ApplyEntity = yaml::from_value(value).expect("Unable to parse");
                stage.from_entity(entity);
                Ok(stage)
            }
            Some("Group") => {
                let group: ApplyGroup = yaml::from_value(value).expect("Unable to parse");
                stage.from_group(group);
                Ok(stage)
            }
            Some("Feature") => {
                let feature: ApplyFeature = yaml::from_value(value).expect("Unable to parse");
                stage.from_feature(feature);
                Ok(stage)
            }
            Some("Items") => Self::from_item_value(value, "Items"),
            Some("items") => Self::from_item_value(value, "items"),
            Some(kind) => Err(format!("invalid kind '{}'", kind).into()),
            None => Err("invalid yaml: missing kind or items".into()),
        }
    }

    fn from_item_value(value: yaml::Value, items: &'static str) -> Result<Self> {
        let mut stage = Self::new();
        match parse_items_kind(&value) {
            Some("Entity") => {
                let mut entities: HashMap<String, Vec<ApplyEntity>> =
                    yaml::from_value(value).expect("Unable to parse");

                for e in entities.remove(items).unwrap_or_default() {
                    stage.from_entity(e);
                }
            }
            Some("Group") => {
                let mut groups: HashMap<String, Vec<ApplyGroup>> =
                    yaml::from_value(value).expect("Unable to parse");

                for g in groups.remove(items).unwrap_or_default() {
                    stage.from_group(g);
                }
            }
            Some("Feature") | Some("Features") => {
                let mut features: HashMap<String, Vec<ApplyFeature>> =
                    yaml::from_value(value).expect("Unable to parse");

                for f in features.remove(items).unwrap_or_default() {
                    stage.from_feature(f);
                }
            }
            Some(kind) => return Err(format!("invalid kind '{}'", kind).into()),
            None => return Err("invalid yaml: missing kind or items".into()),
        }

        Ok(stage)
    }

    fn from_entity(&mut self, mut entity: ApplyEntity) {
        for group in entity.take_groups().unwrap_or_default() {
            self.from_group(group);
        }
        self.new_entities.push(entity);
    }

    fn from_group(&mut self, mut group: ApplyGroup) {
        self.new_features
            .extend(group.take_features().unwrap_or_default());
        self.new_groups.push(group);
    }

    fn from_feature(&mut self, feature: ApplyFeature) {
        self.new_features.push(feature);
    }

    fn merge(&mut self, other: Self) {
        self.new_entities.extend(other.new_entities);
        self.new_groups.extend(other.new_groups);
        self.new_features.extend(other.new_features);
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
    use crate::database::metadata::GroupCategory::{Batch, Stream};
    use crate::database::metadata::{ApplyEntity, ApplyGroup, FeatureValueType};
    use std::time::Duration;

    #[test]
    fn test_build_apply_stage() {
        struct TestCase {
            description: &'static str,
            r: &'static [u8],

            want: Result<ApplyStage>,
        }

        let test_cases = vec![
            TestCase {
                description: "invalid yaml: missing kind or items",
                r: r#"
# kind: Entity
name: user
description: 'User ID'
             "#
                .as_bytes(),
                want: Err(s("invalid yaml: missing kind or items").into()),
            },
            TestCase {
                description: "invalid kind",
                r: r#"
kind: Entit
name: user
description: 'description'
             "#
                .as_bytes(),
                want: Err(s("invalid kind 'Entit'").into()),
            },
            TestCase {
                description: "single entity",
                r: r#"
kind: Entity
name: user
description: 'description'
             "#
                .as_bytes(),
                want: Ok(ApplyStage {
                    new_entities: vec![ApplyEntity {
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
description: 'description'"#
                    .as_bytes(),
                want: Ok(ApplyStage {
                    new_entities: vec![ApplyEntity {
                        name: s("user"),
                        description: s("description"),
                        groups: None,
                    }],
                    new_groups: vec![
                        ApplyGroup {
                            name: s("account"),
                            entity_name: Some(s("user")),
                            category: Batch,
                            snapshot_interval: None,
                            description: s("description"),
                            features: None,
                        },
                        ApplyGroup {
                            name: s("device"),
                            entity_name: Some(s("user")),
                            category: Batch,
                            snapshot_interval: None,
                            description: s("description"),
                            features: None,
                        },
                        ApplyGroup {
                            name: s("user-click"),
                            entity_name: Some(s("user")),
                            category: Stream,
                            snapshot_interval: Some(Duration::from_secs(86400)),
                            description: s("description"),
                            features: None,
                        },
                    ],
                    new_features: vec![
                        ApplyFeature {
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        ApplyFeature {
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
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![ApplyGroup {
                        entity_name: Some(s("user")),
                        name: s("device"),
                        category: Batch,
                        snapshot_interval: None,
                        description: s("description"),
                        features: None,
                    }],
                    new_features: vec![
                        ApplyFeature {
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        ApplyFeature {
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
                want: Ok(ApplyStage {
                    new_entities: vec![ApplyEntity {
                        name: s("user"),
                        description: s("description"),
                        groups: None,
                    }],
                    new_groups: vec![
                        ApplyGroup {
                            name: s("device"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: None,
                            features: None,
                        },
                        ApplyGroup {
                            name: s("user"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: None,
                            features: None,
                        },
                        ApplyGroup {
                            name: s("user-click"),
                            category: Stream,
                            entity_name: Some(s("user")),
                            description: s("description"),
                            snapshot_interval: Some(Duration::from_secs(86400)),
                            features: None,
                        },
                    ],
                    new_features: vec![
                        ApplyFeature {
                            name: s("model"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        ApplyFeature {
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        ApplyFeature {
                            name: s("age"),
                            group_name: Some(s("user")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        ApplyFeature {
                            name: s("gender"),
                            group_name: Some(s("user")),
                            value_type: FeatureValueType::Int64,
                            description: s("description"),
                        },
                        ApplyFeature {
                            name: s("last_5_click_posts"),
                            group_name: Some(s("user-click")),
                            value_type: FeatureValueType::StringType,
                            description: s("description"),
                        },
                        ApplyFeature {
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
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![],
                    new_features: vec![
                        ApplyFeature {
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        ApplyFeature {
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        ApplyFeature {
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        ApplyFeature {
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        ApplyFeature {
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
                want: Ok(ApplyStage {
                    new_entities: vec![],
                    new_groups: vec![
                        ApplyGroup {
                            name: s("account"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user account info"),
                            snapshot_interval: None,
                            features: None,
                        },
                        ApplyGroup {
                            name: s("transaction_stats"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user transaction statistics"),
                            snapshot_interval: None,
                            features: None,
                        },
                    ],
                    new_features: vec![
                        ApplyFeature {
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        ApplyFeature {
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        ApplyFeature {
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        ApplyFeature {
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        ApplyFeature {
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
                want: Ok(ApplyStage {
                    new_entities: vec![
                        ApplyEntity {
                            name: s("user"),
                            description: s("user ID"),
                            groups: None,
                        },
                        ApplyEntity {
                            name: s("device"),
                            description: s("device info"),
                            groups: None,
                        },
                    ],
                    new_groups: vec![
                        ApplyGroup {
                            name: s("account"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user account info"),
                            snapshot_interval: None,
                            features: None,
                        },
                        ApplyGroup {
                            name: s("transaction_stats"),
                            category: Batch,
                            entity_name: Some(s("user")),
                            description: s("user transaction statistics"),
                            snapshot_interval: None,
                            features: None,
                        },
                        ApplyGroup {
                            name: s("phone"),
                            category: Batch,
                            entity_name: Some(s("device")),
                            description: s("phone info"),
                            snapshot_interval: None,
                            features: None,
                        },
                    ],
                    new_features: vec![
                        ApplyFeature {
                            name: s("credit_score"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("credit_score description"),
                        },
                        ApplyFeature {
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Int64,
                            description: s("account_age_days description"),
                        },
                        ApplyFeature {
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: FeatureValueType::Bool,
                            description: s("has_2fa_installed description"),
                        },
                        ApplyFeature {
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_7d description"),
                        },
                        ApplyFeature {
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: FeatureValueType::Int64,
                            description: s("transaction_count_30d description"),
                        },
                        ApplyFeature {
                            name: s("model"),
                            group_name: Some(s("phone")),
                            value_type: FeatureValueType::StringType,
                            description: s("model description"),
                        },
                        ApplyFeature {
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
            let stage = ApplyStage::from_reader(case.r);
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
