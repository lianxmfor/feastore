#!/usr/bin/env bash

set -euo pipefail

source ./utils.sh
source ./assert.sh

init_store
register_features

case='feacli get features works'
expected='
id,name,group,category,value-type,description
1,price,phone,batch,int64,price
2,model,phone,batch,string,model
3,name,student,batch,string,name
4,gender,student,batch,string,gender
5,age,student,batch,int64,age
6,last_5_click_posts,user-click,stream,string,user last 5 click posts
7,number_of_user_starred_posts,user-click,stream,int64,number of posts that users starred today
'
actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
assert_eq "$expected" "$actual" "$case" 

case='feacli get simplified features works'
expected='
id,name,group,category,value-type,description
1,price,phone,batch,int64,price
2,model,phone,batch,string,model
3,name,student,batch,string,name
4,gender,student,batch,string,gender
5,age,student,batch,int64,age
6,last_5_click_posts,user-click,stream,string,user last 5 click posts
7,number_of_user_starred_posts,user-click,stream,int64,number of posts that users starred today
'
actual=$(feacli get feature -o csv | cut -d ',' -f 1-6)
assert_eq "$(sort <<< "$expected")" "$(sort <<< "$actual")" "$case" 

case='feacli get one feature works'
expected='
id,name,group,category,value-type,description
2,model,phone,batch,string,model
'
actual=$(feacli get feature -n phone.model -o csv)
ignore_time() { cut -d ',' -f 1-6 <<<"$1"; }
assert_eq "$(sort <<< "$expected")" "$(ignore_time "$actual" | sort)" "$case" 

case='feacli get feature in yaml: one feature'
expected='
kind: Feature
name: model
group: phone
value-type: string
description: model
'
actual=$(feacli get feature -n phone.model -o yaml)
assert_eq "$expected" "$actual" "$case" 

case='feacli get feature: multiple features'
expected='
items:
- kind: Feature
  name: price
  group: phone
  value-type: int64
  description: price
- kind: Feature
  name: model
  group: phone
  value-type: string
  description: model
- kind: Feature
  name: name
  group: student
  value-type: string
  description: name
- kind: Feature
  name: gender
  group: student
  value-type: string
  description: gender
- kind: Feature
  name: age
  group: student
  value-type: int64
  description: age
- kind: Feature
  name: last_5_click_posts
  group: user-click
  value-type: string
  description: user last 5 click posts
- kind: Feature
  name: number_of_user_starred_posts
  group: user-click
  value-type: int64
  description: number of posts that users starred today
'
actual=$(feacli get feature -o yaml)
assert_eq "$expected" "$actual" "$case" 


