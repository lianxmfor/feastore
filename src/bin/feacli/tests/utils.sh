#!/usr/bin/env bash
set -Eeuo pipefail
shopt -s expand_aliases

alias feacli="../../../../target/debug/feacli --config config.yaml"

function init_store {
  echo "init_store" >> /dev/null
  db_file="/tmp/feastore.db"
  [ -f "$db_file" ] && rm "$db_file"
  touch "$db_file"
}

# register features for the sample data
register_features() {
    feacli register entity device --description "device"
    feacli register entity user   --description "user"

    feacli register group phone      --entity device --category "batch"  --description "phone"
    feacli register group student    --entity user   --category "batch"  --description "student"
    feacli register group user-click \
      --entity user \
      --category "stream" \
      --snapshot-interval 1 \
      --description "user click post feature"

    feacli register feature price  --group phone   --value-type "int64"  --description "price"
    feacli register feature model  --group phone   --value-type "string" --description "model"

    feacli register feature name   --group student --value-type "string" --description "name"
    feacli register feature gender --group student --value-type "string" --description "gender"
    feacli register feature age    --group student --value-type "int64"  --description "age"

    feacli register feature last_5_click_posts \
      --group user-click \
      --value-type "string" \
      --description "user last 5 click posts"

    feacli register feature number_of_user_starred_posts \
      --group user-click \
      --value-type "int64"  \
      --description "number of posts that users starred today"
}
