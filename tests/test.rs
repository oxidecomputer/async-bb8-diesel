// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl, AsyncSaveChangesDsl, ConnectionError};
use diesel::OptionalExtension;
use diesel::{pg::PgConnection, prelude::*};

mod harness;

use harness::{CockroachInstance, CockroachStarterBuilder};

table! {
    user (id) {
        id -> Int4,
        name -> Text,
    }
}

const SCHEMA: &'static str = r#"
    CREATE DATABASE test;
    CREATE TABLE IF NOT EXISTS test.public.user (
        id INT4 PRIMARY KEY,
        name STRING(512)
    );
"#;

#[derive(AsChangeset, Insertable, Queryable, PartialEq, Clone)]
#[diesel(table_name = user)]
pub struct User {
    pub id: i32,
    pub name: String,
}

#[derive(AsChangeset, Identifiable)]
#[diesel(table_name = user)]
pub struct UserUpdate<'a> {
    pub id: i32,
    pub name: &'a str,
}

// Creates a new CRDB database under test
async fn test_start() -> CockroachInstance {
    let crdb = CockroachStarterBuilder::new()
        .redirect_stdio_to_files()
        .build()
        .expect("Failed to create CockroachDB builder")
        .start()
        .await
        .expect("Failed to start CockroachDB");

    let client = crdb.connect().await.expect("Could not connect to database");

    client
        .batch_execute(&SCHEMA)
        .await
        .expect("Failed to initialize database");

    crdb
}

// Terminates a test CRDB database
async fn test_end(mut crdb: CockroachInstance) {
    crdb.cleanup()
        .await
        .expect("Failed to clean up CockroachDB");
}

#[tokio::test]
async fn test_insert_load_update_delete() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use user::dsl;
    // Insert by values
    let _ = diesel::insert_into(dsl::user)
        .values((dsl::id.eq(0), dsl::name.eq("Jim")))
        .execute_async(&*conn)
        .await
        .unwrap();

    // Insert by structure
    let _ = diesel::insert_into(dsl::user)
        .values(User {
            id: 1,
            name: "Xiang".to_string(),
        })
        .execute_async(&*conn)
        .await
        .unwrap();

    // Load
    let users = dsl::user.get_results_async::<User>(&*conn).await.unwrap();
    assert_eq!(users.len(), 2);

    // Update
    let _ = diesel::update(dsl::user)
        .filter(dsl::id.eq(0))
        .set(dsl::name.eq("Jim, But Different"))
        .execute_async(&*conn)
        .await
        .unwrap();

    // Update via save_changes
    let update = &UserUpdate {
        id: 0,
        name: "The Artist Formerly Known As Jim",
    };
    let _ = update.save_changes_async::<User>(&*conn).await.unwrap();

    // Delete
    let _ = diesel::delete(dsl::user)
        .filter(dsl::id.eq(0))
        .execute_async(&*conn)
        .await
        .unwrap();

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use user::dsl;

    // Transaction with multiple operations
    conn.transaction_async(|conn| async move {
        diesel::insert_into(dsl::user)
            .values((dsl::id.eq(3), dsl::name.eq("Sally")))
            .execute_async(&conn)
            .await
            .unwrap();
        diesel::insert_into(dsl::user)
            .values((dsl::id.eq(4), dsl::name.eq("Arjun")))
            .execute_async(&conn)
            .await
            .unwrap();
        Ok::<(), ConnectionError>(())
    })
    .await
    .unwrap();

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction_custom_error() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    // Demonstrates an error which may be returned from transactions.
    #[derive(thiserror::Error, Debug)]
    enum MyError {
        #[error("DB error")]
        Db(#[from] ConnectionError),

        #[error("Custom transaction error")]
        Other,
    }

    impl From<diesel::result::Error> for MyError {
        fn from(error: diesel::result::Error) -> Self {
            MyError::Db(ConnectionError::Query(error))
        }
    }

    use user::dsl;

    // Transaction returning custom error types.
    let _: MyError = conn
        .transaction_async(|conn| async move {
            diesel::insert_into(dsl::user)
                .values((dsl::id.eq(1), dsl::name.eq("Ishmael")))
                .execute_async(&conn)
                .await?;
            return Err::<(), MyError>(MyError::Other {});
        })
        .await
        .unwrap_err();

    test_end(crdb).await;
}

#[tokio::test]
async fn test_optional_extension() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use user::dsl;

    // Access the result via OptionalExtension
    assert!(dsl::user
        .filter(dsl::id.eq(12345))
        .first_async::<User>(&*conn)
        .await
        .optional()
        .unwrap()
        .is_none());

    test_end(crdb).await;
}
