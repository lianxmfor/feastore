#!/usr/bin/env bash

set -euo pipefail

source ./utils.sh
source ./assert.sh

init_store
register_features
#import_device_sample > /dev/null

case='feacli get entity works'
expected='
id,name,description
1,device,device
2,user,user
'
actual=$(feacli get entity -o csv | cut -d ',' -f 1-3 )
assert_eq "$expected" "$actual" "$case" 

case='feacli get entity -o yaml: one entity'
expected='
kind: Entity
name: device
description: device
groups:
- name: phone
  category: batch
  description: phone
  features:
  - name: price
    value-type: int64
    description: price
  - name: model
    value-type: string
    description: model
'
actual=$(feacli get entity -n device -o yaml)
assert_eq "$expected" "$actual" "$case" 

case='feacli get entity -o yaml: multiple entities'
expected='
items:
- kind: Entity
  name: device
  description: device
  groups:
  - name: phone
    category: batch
    description: phone
    features:
    - name: price
      value-type: int64
      description: price
    - name: model
      value-type: string
      description: model
- kind: Entity
  name: user
  description: user
  groups:
  - name: student
    category: batch
    description: student
    features:
    - name: name
      value-type: string
      description: name
    - name: gender
      value-type: string
      description: gender
    - name: age
      value-type: int64
      description: age
  - name: user-click
    category: stream
    snapshot-interval: 1
    description: user click post feature
    features:
    - name: last_5_click_posts
      value-type: string
      description: user last 5 click posts
    - name: number_of_user_starred_posts
      value-type: int64
      description: number of posts that users starred today
'
actual=$(feacli get entity -o yaml)
assert_eq "$expected" "$actual" "$case" 
