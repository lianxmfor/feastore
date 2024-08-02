#!/usr/bin/env bash
source ./utils.sh
source ./assert.sh

apply_single_complex_entity() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: 'description'
groups:
- name: device
  category: batch
  description: a description
  features:
  - name: model
    value-type: string
    description: 'description'
  - name: price
    value-type: int64
    description: 'description'
- name: user
  category: batch
  description: a description
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
  description: user click post feature
EOF

    entity_expected='id,name,description
1,user,description'
    entity_actual=$(feacli get entity -o csv)
    filter() { cut -d ',' -f 1-3 <<< "$1"; }
    assert_eq  "$(sort <<< "$entity_expected")" "$(filter "$entity_actual" | sort)" "apply_single_complex_entity: check entity"

    group_expected='id,name,entity,category,snapshot-interval,description,online-revision-id,create-time,modify-time
1,device,user,batch,,a description,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z
2,user,user,batch,,a description,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z
3,user-click,user,stream,86400,user click post feature'
    group_actual=$(feacli get group -o csv)
    filter() { cut -d ',' -f 1-6 <<<"$1"; }
    assert_eq "$(filter "$group_expected" | sort)" "$(filter "$group_actual" | sort)" "apply_single_complex_entity: check group"
    feature_expected='id,name,group,category,value-type,description
1,model,device,batch,string,description
2,price,device,batch,int64,description
3,age,user,batch,int64,description
4,gender,user,batch,int64,description'
    feature_actual=$(feacli get feature -o csv)
    filter() { cut -d ',' -f 1-6 <<<"$1"; }
    assert_eq "$(sort <<< "$feature_expected")" "$(filter "$feature_actual" | sort)" "apply_single_complex_entity: check feature"
}

apply_multiple_files_of_entity() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: 'description'
groups:
- name: student
  category: batch
  description: student feature group
---
kind: Entity
name: device
description: 'description'
---
kind: Entity
name: test
description: 'description'
EOF


  entity_expected='id,name,description
1,user,description
2,device,description
3,test,description'
    entity_actual=$(feacli get entity -o csv | cut -d ',' -f 1-3)
    assert_eq "$entity_expected" "$entity_actual" "apply_multiple_files_of_entity: feacli get entity" 

    group_expected='id,name,entity,category,snapshot-interval,description,online-revision-id,create-time,modify-time
1,student,user,batch,,student feature group,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z'
    group_actual=$(feacli get group -o csv)
    filter() { cut -d ',' -f 1-6 <<<"$1"; }
    assert_eq "$(filter "$group_expected"| sort)" "$(filter "$group_actual" | sort)" "apply_multiple_files_of_entity: check group" 
}

apply_entity_items() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
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
EOF

      entity_expected='id,name,description
1,user,user ID
2,device,device info'
      entity_actual=$(feacli get entity -o csv | cut -d ',' -f 1-3)
      assert_eq "$(sort <<< "$entity_expected")" "$(sort <<< "$entity_actual")" "apply_entity_items: feacli apply mutiple entity: check entity" 

    group_expected='id,name,entity,category,snapshot-interval,description
1,account,user,batch,,user account info
2,transaction_stats,user,batch,,user transaction statistics
3,phone,device,batch,,phone info'
      group_actual=$(feacli get group -o csv | cut -d ',' -f 1-6)
      assert_eq "$group_expected" "$group_actual" "apply_entity_items: feacli apply multiple entity: check group" 

      feature_expected='id,name,group,category,value-type,description
1,credit_score,account,batch,int64,credit_score description
2,account_age_days,account,batch,int64,account_age_days description
3,has_2fa_installed,account,batch,bool,has_2fa_installed description
4,transaction_count_7d,transaction_stats,batch,int64,transaction_count_7d description
5,transaction_count_30d,transaction_stats,batch,int64,transaction_count_30d description
6,model,phone,batch,string,model description
7,price,phone,batch,int64,price description'
      feature_actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
      assert_eq "$feature_expected" "$feature_actual" "apply_entity_items: feacli apply multiple entity: check feature" 
}

apply_single_complex_entity
apply_multiple_files_of_entity
apply_entity_items
