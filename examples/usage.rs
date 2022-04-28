use async_bb8_diesel::{
    AsyncConnection, AsyncRunQueryDsl, AsyncSaveChangesDsl, ConnectionError, PoolError,
};
use diesel::{pg::PgConnection, prelude::*};

table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[derive(AsChangeset, Insertable, Queryable, PartialEq, Clone)]
#[diesel(table_name = users)]
pub struct User {
    pub id: i32,
    pub name: String,
}

#[derive(AsChangeset, Identifiable)]
#[diesel(table_name = users)]
pub struct UserUpdate<'a> {
    pub id: i32,
    pub name: &'a str,
}

// Demonstrates an error which may be returned from transactions.
#[derive(thiserror::Error, Debug)]
enum MyError {
    #[error("DB error")]
    Db(#[from] PoolError),

    #[error("Custom transaction error")]
    Other,
}

impl From<diesel::result::Error> for MyError {
    fn from(error: diesel::result::Error) -> Self {
        MyError::Db(PoolError::Connection(ConnectionError::Query(error)))
    }
}

#[tokio::main]
async fn main() {
    use users::dsl;

    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new("localhost:1234");
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    // Insert by values
    let _ = diesel::insert_into(dsl::users)
        .values((dsl::id.eq(0), dsl::name.eq("Jim")))
        .execute_async(&pool)
        .await
        .unwrap();

    // Insert by structure
    let _ = diesel::insert_into(dsl::users)
        .values(User {
            id: 0,
            name: "Jim".to_string(),
        })
        .execute_async(&pool)
        .await
        .unwrap();

    // Load
    let _ = dsl::users.get_result_async::<User>(&pool).await.unwrap();

    // Update
    let _ = diesel::update(dsl::users)
        .filter(dsl::id.eq(0))
        .set(dsl::name.eq("Jim, But Different"))
        .execute_async(&pool)
        .await
        .unwrap();

    // Update via save_changes
    let update = &UserUpdate { id: 0, name: "Jim" };
    let _ = update.save_changes_async::<User>(&pool).await.unwrap();

    // Delete
    let _ = diesel::delete(dsl::users)
        .filter(dsl::id.eq(0))
        .execute_async(&pool)
        .await
        .unwrap();

    // Transaction with multiple operations
    pool.transaction(|conn| {
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(0), dsl::name.eq("Jim")))
            .execute(conn)
            .unwrap();
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(1), dsl::name.eq("Another Jim")))
            .execute(conn)
            .unwrap();
        Ok::<(), PoolError>(())
    })
    .await
    .unwrap();

    // Transaction returning custom error types.
    let _: MyError = pool
        .transaction(|_| {
            return Err::<(), MyError>(MyError::Other {});
        })
        .await
        .unwrap_err();
}
