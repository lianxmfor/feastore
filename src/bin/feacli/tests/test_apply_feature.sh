#!/usr/bin/env bash
set -euo pipefail

source ./utils.sh
source ./assert.sh

apply_multiple_files_of_feature() {
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
kind: Feature
name: model
group: device
category: batch
value-type: string
description: 'description'
---
kind: Feature
name: price
group: device
category: batch
value-type: int64
description: 'description'
EOF

    feature_expected='id,name,group,category,value-type,description
1,model,device,batch,string,description
2,price,device,batch,int64,description'
    feature_actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
    assert_eq "$(sort <<< "$feature_expected")" "$(sort <<< "$feature_actual")" "apply_multiple_files_of_feature: check feature" 
}

apply_feature_items() {
    init_store

    cat <<EOF | feacli apply -f /dev/stdin
kind: Entity
name: user
description: user ID
---
kind: Group
name: account
entity: user
category: batch
description: user account info
---
kind: Group
name: transaction_stats
entity: user
category: batch
description: user transaction statistics
---
items:
  - kind: Feature
    name: credit_score
    group: account
    value-type: int64
    description: "credit_score description"
  - kind: Feature
    name: account_age_days
    group: account
    value-type: int64
    description: "account_age_days description"
  - kind: Feature
    name: has_2fa_installed
    group: account
    value-type: bool
    description: "has_2fa_installed description"
  - kind: Feature
    name: transaction_count_7d
    group: transaction_stats
    value-type: int64
    description: "transaction_count_7d description"
  - kind: Feature
    name: transaction_count_30d
    group: transaction_stats
    value-type: int64
    description: "transaction_count_30d description"
EOF

    feature_expected='id,name,group,category,value-type,description
1,credit_score,account,batch,int64,credit_score description
2,account_age_days,account,batch,int64,account_age_days description
3,has_2fa_installed,account,batch,bool,has_2fa_installed description
4,transaction_count_7d,transaction_stats,batch,int64,transaction_count_7d description
5,transaction_count_30d,transaction_stats,batch,int64,transaction_count_30d description'
    feature_actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
    assert_eq "$(sort <<< "$feature_expected")" "$(sort <<< "$feature_actual")" "apply_feature_items: check feature" 
}

apply_multiple_files_of_feature
apply_feature_items
