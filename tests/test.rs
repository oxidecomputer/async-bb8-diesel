// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use async_bb8_diesel::{
    AsyncConnection, AsyncRunQueryDsl, AsyncSaveChangesDsl, AsyncSimpleConnection, ConnectionError,
    OptionalExtension, RunError,
};
use crdb_harness::{CockroachInstance, CockroachStarterBuilder};
use diesel::{pg::PgConnection, prelude::*};

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
async fn test_transaction_automatic_retry_success_case() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use user::dsl;

    // Transaction that can retry but does not need to.
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);
    conn.transaction_async_with_retry(
        |conn| async move {
            assert!(conn.transaction_depth().await.unwrap() > 0);
            diesel::insert_into(dsl::user)
                .values((dsl::id.eq(3), dsl::name.eq("Sally")))
                .execute_async(&conn)
                .await?;
            Ok(())
        },
        || async { panic!("Should not attempt to retry this operation") },
    )
    .await
    .expect("Transaction failed");
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction_automatic_retry_explicit_rollback() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use std::sync::{Arc, Mutex};

    let transaction_attempted_count = Arc::new(Mutex::new(0));
    let should_retry_query_count = Arc::new(Mutex::new(0));

    // Test a transaction that:
    //
    // 1. Retries on the first call
    // 2. Explicitly rolls back on the second call
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);
    let err = conn
        .transaction_async_with_retry(
            |_conn| {
                let transaction_attempted_count = transaction_attempted_count.clone();
                async move {
                    let mut count = transaction_attempted_count.lock().unwrap();
                    *count += 1;

                    if *count < 2 {
                        eprintln!("test: Manually restarting txn");
                        return Err::<(), _>(RunError::User(diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::SerializationFailure,
                            Box::new("restart transaction".to_string()),
                        )));
                    }
                    eprintln!("test: Manually rolling back txn");
                    return Err(RunError::User(diesel::result::Error::RollbackTransaction));
                }
            },
            || async {
                *should_retry_query_count.lock().unwrap() += 1;
                true
            },
        )
        .await
        .expect_err("Transaction should have failed");

    assert_eq!(
        err,
        RunError::User(diesel::result::Error::RollbackTransaction)
    );
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);

    // The transaction closure should have been attempted twice, but
    // we should have only asked whether or not to retry once -- after
    // the first failure, but not the second.
    assert_eq!(*transaction_attempted_count.lock().unwrap(), 2);
    assert_eq!(*should_retry_query_count.lock().unwrap(), 1);

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction_automatic_retry_injected_errors() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    use std::sync::{Arc, Mutex};

    let transaction_attempted_count = Arc::new(Mutex::new(0));
    let should_retry_query_count = Arc::new(Mutex::new(0));

    // Tests a transaction that is forced to retry by CockroachDB.
    //
    // By setting this session variable, we expect that:
    // - "any statement executed inside of an explicit transaction (with the
    // exception of SET statements) will return a transaction retry error."
    // - "after the 3rd retry error, the transaction will proceed as
    // normal"
    //
    // See: https://www.cockroachlabs.com/docs/v23.1/transaction-retry-error-example#test-transaction-retry-logic
    // for more details
    const EXPECTED_ERR_COUNT: usize = 3;
    conn.batch_execute_async("SET inject_retry_errors_enabled = true")
        .await
        .expect("Failed to inject error");
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);
    conn.transaction_async_with_retry(
        |conn| {
            let transaction_attempted_count = transaction_attempted_count.clone();
            async move {
                *transaction_attempted_count.lock().unwrap() += 1;

                use user::dsl;
                let _ = diesel::insert_into(dsl::user)
                    .values((dsl::id.eq(0), dsl::name.eq("Jim")))
                    .execute_async(&conn)
                    .await?;
                Ok(())
            }
        },
        || async {
            *should_retry_query_count.lock().unwrap() += 1;
            true
        },
    )
    .await
    .expect("Transaction should have succeeded");
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);

    // The transaction closure should have been attempted twice, but
    // we should have only asked whether or not to retry once -- after
    // the first failure, but not the second.
    assert_eq!(
        *transaction_attempted_count.lock().unwrap(),
        EXPECTED_ERR_COUNT + 1
    );
    assert_eq!(
        *should_retry_query_count.lock().unwrap(),
        EXPECTED_ERR_COUNT
    );

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction_automatic_retry_does_not_retry_non_retryable_errors() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    // Test a transaction that:
    //
    // Fails with a non-retryable error. It should exit immediately.
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);
    assert_eq!(
        conn.transaction_async_with_retry(
            |_| async { Err::<(), _>(RunError::User(diesel::result::Error::NotFound)) },
            || async { panic!("Should not attempt to retry this operation") }
        )
        .await
        .expect_err("Transaction should have failed"),
        RunError::User(diesel::result::Error::NotFound),
    );
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);

    test_end(crdb).await;
}

#[tokio::test]
async fn test_transaction_automatic_retry_nested_transactions_fail() {
    let crdb = test_start().await;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new(&crdb.pg_config().url);
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    #[derive(Debug, PartialEq)]
    struct OnlyReturnFromOuterTransaction {}

    // This outer transaction should succeed immediately...
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);
    assert_eq!(
        OnlyReturnFromOuterTransaction {},
        conn.transaction_async_with_retry(
            |conn| async move {
                // ... but this inner transaction should fail! We do not support
                // retryable nested transactions.
                let err = conn
                    .transaction_async_with_retry(
                        |_| async {
                            panic!("Shouldn't run");

                            // Adding this unreachable statement for type inference
                            #[allow(unreachable_code)]
                            Ok(())
                        },
                        || async { panic!("Shouldn't retry inner transaction") },
                    )
                    .await
                    .expect_err("Nested transaction should have failed");
                assert_eq!(
                    err,
                    RunError::User(diesel::result::Error::AlreadyInTransaction)
                );

                // We still want to show that control exists within the outer
                // transaction, so we explicitly return here.
                Ok(OnlyReturnFromOuterTransaction {})
            },
            || async { panic!("Shouldn't retry outer transaction") },
        )
        .await
        .expect("Transaction should have succeeded")
    );
    assert_eq!(conn.transaction_depth().await.unwrap(), 0);

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

    impl From<RunError<diesel::result::Error>> for MyError {
        fn from(error: RunError<diesel::result::Error>) -> Self {
            MyError::Db(ConnectionError::from(error))
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
