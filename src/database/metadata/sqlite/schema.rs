use phf::phf_map;

pub static META_TABLE_SCHEMAS: phf::Map<&'static str, &'static str> = phf_map! {
    "entity" => r#"
        CREATE TABLE entity (
            id              INTEGER NOT     NULL PRIMARY KEY AUTOINCREMENT,
            name            VARCHAR(32)     NOT NULL,
            description     VARCHAR(64)     DEFAULT '',
            create_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            modify_time     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name)
        );
    "#,
};

pub static META_VIEW_SCHEMAS: phf::Map<&'static str, &'static str> = phf_map! {};
