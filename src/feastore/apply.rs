use serde::Deserialize;
use serde_yaml::Value;

use std::collections::HashMap;

use crate::feastore::types::{ApplyOpt, ApplyStage, Entity, Feature, Group};

fn build_apply_stage<R: std::io::Read>(opt: ApplyOpt<R>) -> Result<ApplyStage, String> {
    let mut stage = ApplyStage::new();
    for document in serde_yaml::Deserializer::from_reader(opt.r) {
        let value = Value::deserialize(document).expect("Unable to parse");

        match parse_kind(&value) {
            Some("Entity") => {
                let entity: Entity = serde_yaml::from_value(value).expect("Unable to parse");

                stage.new_entities.push(build_entity(&entity));
                if let Some(groups) = entity.groups {
                    for group in groups {
                        stage
                            .new_groups
                            .push(build_group(&group, Some(entity.name.clone())));

                        if let Some(features) = group.features {
                            for feature in features {
                                stage
                                    .new_features
                                    .push(build_feature(&feature, Some(group.name.clone())))
                            }
                        }
                    }
                }
            }
            Some("Group") => {
                let group: Group = serde_yaml::from_value(value).expect("Unable to parse");

                stage
                    .new_groups
                    .push(build_group(&group, group.entity_name.clone()));

                if let Some(features) = group.features {
                    for feature in features {
                        stage
                            .new_features
                            .push(build_feature(&feature, Some(group.name.clone())));
                    }
                }
            }
            Some("Feature") => {
                let feature: Feature = serde_yaml::from_value(value).expect("Unable to parse");
                stage
                    .new_features
                    .push(build_feature(&feature, feature.group_name.clone()));
            }
            Some("Items") | Some("items") => match parse_items_kind(&value) {
                Some("Entity") => {
                    let entities: HashMap<String, Vec<Entity>> =
                        serde_yaml::from_value(value).expect("Unable to parse");
                    let entities = if let Some(entities) = entities.get("Items") {
                        Some(entities)
                    } else if let Some(entities) = entities.get("items") {
                        Some(entities)
                    } else {
                        None
                    };

                    if let Some(entities) = entities {
                        for entity in entities {
                            stage.new_entities.push(build_entity(&entity));

                            if let Some(groups) = &entity.groups {
                                for group in groups {
                                    stage
                                        .new_groups
                                        .push(build_group(&group, Some(entity.name.clone())));

                                    if let Some(features) = &group.features {
                                        for feature in features {
                                            stage.new_features.push(build_feature(
                                                feature,
                                                Some(group.name.clone()),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Some("Group") => {
                    let groups: HashMap<String, Vec<Group>> =
                        serde_yaml::from_value(value).expect("Unable to parse");
                    let groups = if let Some(groups) = groups.get("Items") {
                        Some(groups)
                    } else if let Some(groups) = groups.get("items") {
                        Some(groups)
                    } else {
                        None
                    };

                    if let Some(groups) = groups {
                        for group in groups {
                            stage
                                .new_groups
                                .push(build_group(&group, group.entity_name.clone()));

                            if let Some(features) = &group.features {
                                for feature in features {
                                    stage
                                        .new_features
                                        .push(build_feature(feature, Some(group.name.clone())));
                                }
                            }
                        }
                    }
                }
                Some("Feature") => {
                    let features: HashMap<String, Vec<Feature>> =
                        serde_yaml::from_value(value).expect("Unable to parse");

                    let features = if let Some(features) = features.get("Items") {
                        Some(features)
                    } else if let Some(features) = features.get("items") {
                        Some(features)
                    } else {
                        None
                    };

                    if let Some(features) = features {
                        for feature in features {
                            stage
                                .new_features
                                .push(build_feature(&feature, feature.group_name.clone()));
                        }
                    }
                }
                Some(_) => {}
                None => {}
            },
            Some(kind) => return Err(format!("invalid kind '{}'", kind)),
            None => return Err("invalid yaml: missing kind or items".to_string()),
        }
    }
    Ok(stage)
}

fn parse_kind(value: &serde_yaml::Value) -> Option<&str> {
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

fn parse_items_kind(value: &serde_yaml::Value) -> Option<&str> {
    if let Some(sequence) = value["items"].as_sequence() {
        if sequence.len() > 0 {
            return sequence[0]["kind"].as_str();
        }
    }
    None
}

fn build_entity(entity: &Entity) -> Entity {
    Entity {
        kind: Some("Entity".to_string()),
        name: entity.name.to_owned(),
        description: entity.description.to_owned(),
        groups: None,
    }
}

fn build_group(group: &Group, entity_name: Option<String>) -> Group {
    Group {
        kind: Some("Group".to_string()),
        name: group.name.to_owned(),
        entity_name,
        category: group.category.to_owned(),
        snapshot_interval: group.snapshot_interval,
        description: group.description.to_owned(),
        features: None,
    }
}

fn build_feature(feature: &Feature, group_name: Option<String>) -> Feature {
    Feature {
        kind: Some("Feature".to_string()),
        name: feature.name.to_owned(),
        group_name,
        value_type: feature.value_type.to_owned(),
        description: feature.description.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::metadata::types::Category::{Batch, Stream};
    use std::time::Duration;

    #[test]
    fn test_build_apply_stage() {
        struct TestCase {
            description: &'static str,
            opt: ApplyOpt<&'static [u8]>,

            want: Result<ApplyStage, String>,
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
                want: Err(s("invalid yaml: missing kind or items")),
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
                want: Err(s("invalid kind 'Entit'")),
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
                            value_type: s("string"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: s("int64"),
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
                            value_type: s("string"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: s("int64"),
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
                            value_type: s("string"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("device")),
                            value_type: s("int64"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("age"),
                            group_name: Some(s("user")),
                            value_type: s("int64"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("gender"),
                            group_name: Some(s("user")),
                            value_type: s("int64"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("last_5_click_posts"),
                            group_name: Some(s("user-click")),
                            value_type: s("string"),
                            description: s("description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("number_of_user_started_posts"),
                            group_name: Some(s("user-click")),
                            value_type: s("int64"),
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
                            value_type: s("int64"),
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: s("int64"),
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: s("bool"),
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
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
                            value_type: s("int64"),
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: s("int64"),
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: s("bool"),
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
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
                            value_type: s("int64"),
                            description: s("credit_score description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("account_age_days"),
                            group_name: Some(s("account")),
                            value_type: s("int64"),
                            description: s("account_age_days description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("has_2fa_installed"),
                            group_name: Some(s("account")),
                            value_type: s("bool"),
                            description: s("has_2fa_installed description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_7d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
                            description: s("transaction_count_7d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("transaction_count_30d"),
                            group_name: Some(s("transaction_stats")),
                            value_type: s("int64"),
                            description: s("transaction_count_30d description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("model"),
                            group_name: Some(s("phone")),
                            value_type: s("string"),
                            description: s("model description"),
                        },
                        Feature {
                            kind: Some(s("Feature")),
                            name: s("price"),
                            group_name: Some(s("phone")),
                            value_type: s("int64"),
                            description: s("price description"),
                        },
                    ],
                }),
            },
        ];

        for case in test_cases {
            let stage = build_apply_stage(case.opt);
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
