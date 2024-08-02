#!/usr/bin/env bash
set -euo pipefail

source ./utils.sh
source ./assert.sh

apply_single_complex_group() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: 'description'
---
kind: Group
name: device
entity: user
category: batch
description: 'description'
features:
- name: model
  value-type: string
  description: 'description'
- name: price
  value-type: int64
  description: 'description'
- name: radio
  value-type: int64
  description: 'description'
EOF

    group_expected='id,name,entity,category,snapshot-interval,description,online-revision-id,create-time,modify-time
1,device,user,batch,,description,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z'
    group_actual=$(feacli get group -o csv)
    filter() { cut -d ',' -f 1-6 <<<"$1"; }
    assert_eq "$(filter "$group_expected" | sort)" "$(filter "$group_actual" | sort)" "apply_single_complex_group: check group" 

    feature_expected='id,name,group,category,value-type,description
1,model,device,batch,string,description
2,price,device,batch,int64,description
3,radio,device,batch,int64,description'
    feature_actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
    assert_eq "$(sort <<< "$feature_expected")" "$(sort <<< "$feature_actual")" "apply_single_complex_group: check feature" 
}

apply_multiple_files_of_group() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: 'description'
---
kind: Group
name: device
entity: user
category: batch
description: 'description'
---
kind: Group
name: account
entity: user
category: batch
description: 'description'
---
kind: Group
name: user-click
entity: user
category: stream
snapshot-interval: 86400
description: user click post feature
EOF

    group_expected='id,name,entity,category,snapshot-interval,description,online-revision-id,create-time,modify-time
1,device,user,batch,,description,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z
2,account,user,batch,,description,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z
3,user-click,user,stream,86400,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z'
    group_actual=$(feacli get group -o csv)
    filter() { cut -d ',' -f 1-5 <<<"$1"; }
    assert_eq "$(filter "$group_expected" | sort)" "$(filter "$group_actual" | sort)" "apply_multiple_files_of_group: check group" 
}

apply_group_items() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: 'description'
---
kind: Entity
name: device
description: 'description'
---
items:
  - kind: Group
    name: account
    entity: user
    category: batch
    description: user account info
    features:
      - name: state
        value-type: string
        description: ""
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
    entity: user
    category: batch
    description: user transaction statistics
    features:
      - name: transaction_count_7d
        value-type: int64
        description: transaction_count_7d description
      - name: transaction_count_30d
        value-type: int64
        description: transaction_count_30d description
  - kind: Group
    name: phone
    entity: device
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

    group_expected='id,name,entity,category,snapshot-interval,description
1,account,user,batch,,user account info
2,transaction_stats,user,batch,,user transaction statistics
3,phone,device,batch,,phone info'
    group_actual=$(feacli get group -o csv)
    filter() { cut -d ',' -f 1-5 <<<"$1"; }
    assert_eq "$(filter "$group_expected" | sort)" "$(filter "$group_actual" | sort)" "apply_single_complex_group: check group" 

    feature_expected='id,name,group,category,value-type,description
1,state,account,batch,string,
2,credit_score,account,batch,int64,credit_score description
3,account_age_days,account,batch,int64,account_age_days description
4,has_2fa_installed,account,batch,bool,has_2fa_installed description
5,transaction_count_7d,transaction_stats,batch,int64,transaction_count_7d description
6,transaction_count_30d,transaction_stats,batch,int64,transaction_count_30d description
7,model,phone,batch,string,model description
8,price,phone,batch,int64,price description'
    feature_actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
    assert_eq "$feature_expected" "$feature_actual" "apply_single_complex_group: check group" 
}

apply_single_complex_group
apply_multiple_files_of_group
apply_group_items
