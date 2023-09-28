use phf::phf_map;

pub static META_TABLE_SCHEMAS: phf::Map<&'static str, &'static str> = phf_map! {
    "entity" => r#"
        CREATE TABLE IF NOT EXISTS entity (
            id              INTEGER NOT     NULL PRIMARY KEY AUTOINCREMENT,
            name            VARCHAR(32)     NOT NULL,
            description     VARCHAR(64)     DEFAULT '',
            create_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modify_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name)
        );
    "#,
    "feature_group" => r#"
        CREATE TABLE IF NOT EXISTS feature_group (
            id              INTEGER         NOT NULL PRIMARY KEY AUTOINCREMENT,
            name            VARCHAR(32)     NOT NULL,
            category        VARCHAR(16)     NOT NULL,
            entity_id       INT             NOT NULL,
            description     VARCHAR(64)     DEFAULT '',
            create_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modify_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name),
            FOREIGN KEY (entity_id) REFERENCES entity(id)
        )
    "#,
    "feature" => r#"
        CREATE TABLE IF NOT EXISTS feature (
            id              INTEGER         NOT NULL PRIMARY KEY AUTOINCREMENT,
            name            VARCHAR(32)     NOT NULL,
            group_id        INT             NOT NULL,
            value_type      INT             NOT NULL,
            description     VARCHAR(64)     DEFAULT '',
            create_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modify_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (group_id, name),
            FOREIGN KEY (group_id) REFERENCES feature_group(id)
        )
    "#,
};

pub static META_VIEW_SCHEMAS: phf::Map<&'static str, &'static str> = phf_map! {};
