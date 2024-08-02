#!/usr/bin/env bash

set -euo pipefail
source ./utils.sh
source ./assert.sh

init_store
register_features

case='feacli get group works'
expected='id,name,entity,category,snapshot-interval,description,online-revision-id,create-time,modify-time
1,phone,device,batch,,phone,<NULL>,2021-11-30T07:51:03Z,2021-11-30T08:19:13Z
2,student,user,batch,,student
3,user-click,user,stream,1,user click post feature
'
actual=$(feacli get group -o csv)
ignore_time() { cut -d ',' -f 1-5 <<<"$1"; }
assert_eq "$(ignore_time "$expected" | sort)" "$(ignore_time "$actual" | sort)" "$case" 

case='feacli get simplified group works'
expected='id,name,entity,category,snapshot-interval,description
1,phone,device,batch,,phone
2,student,user,batch,,student
3,user-click,user,stream,1,user click post feature
'
actual=$(feacli get group -o csv | cut -d ',' -f 1-6)
assert_eq "$(sort <<< "$expected")" "$(sort <<< "$actual")" "$case" 
case='feacli get one group works'
expected='id,name,entity,category,snapshot-interval,description
1,phone,device,batch,,phone
'
actual=$(feacli get group -n phone -o csv | cut -d ',' -f 1-6)
assert_eq "$(sort <<< "$expected")" "$(sort <<< "$actual")" "$case" 

case='feacli get group -o yaml: one group'
expected='
kind: Group
name: phone
entity: device
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
actual=$(feacli get group -n phone -o yaml)
assert_eq "$expected" "$actual" "$case" 

case='feacli get group -o yaml: multiple groups'
expected='
items:
- kind: Group
  name: phone
  entity: device
  category: batch
  description: phone
  features:
  - name: price
    value-type: int64
    description: price
  - name: model
    value-type: string
    description: model
- kind: Group
  name: student
  entity: user
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
- kind: Group
  name: user-click
  entity: user
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

actual=$(feacli get group -o yaml)
assert_eq "$expected" "$actual" "$case" 
