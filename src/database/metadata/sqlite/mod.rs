use sqlx::SqlitePool;

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
}