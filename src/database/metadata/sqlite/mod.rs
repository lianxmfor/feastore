pub mod schema;
pub mod implements;

use sqlx::SqlitePool;

use schema::{META_TABLE_SCHEMAS, META_VIEW_SCHEMAS};

struct DB {
    pool: SqlitePool,
}

impl DB {
    pub async fn from(db_file: &str) -> DB {
        let pool = SqlitePool::connect(format!("sqlite://{}", db_file).as_str())
            .await
            .unwrap();
        DB { pool }
    }

    fn from_pool(pool: SqlitePool) -> DB {
        DB { pool }
    }

    async fn create_database(&self) {
        for table_schema in META_TABLE_SCHEMAS.values() {
            sqlx::query(&table_schema)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        for view_schema in META_VIEW_SCHEMAS.values() {
            sqlx::query(&view_schema).execute(&self.pool).await.unwrap();
        }

        for table in META_TABLE_SCHEMAS.keys() {
            //TODO: use template engine instead {}
            let trigger = format!(
                r"
                    CREATE TRIGGER {}_update_modify_time
                    AFTER UPDATE ON {}
                    BEGIN
                        update {} SET modify_time = datetime('now') WHERE id = NEW.id;
                    END;
            ",
                table, table, table
            );
            sqlx::query(&trigger).execute(&self.pool).await.unwrap();
        }
    }
}
