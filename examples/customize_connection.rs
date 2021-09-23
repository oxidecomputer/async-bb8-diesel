//! An example showing how to cutomize connections while using pooling.

use async_bb8_diesel::{AsyncError, AsyncRunQueryDsl, AsyncSimpleConnection, DieselConnection, ManageError};
use async_trait::async_trait;
use diesel::{pg::PgConnection, prelude::*};

table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[derive(AsChangeset, Insertable, Queryable, PartialEq, Clone)]
#[table_name = "users"]
pub struct User {
    pub id: i32,
    pub name: String,
}

#[derive(Debug)]
struct ConnectionCustomizer {}

type DieselPgConn = DieselConnection<PgConnection>;

#[async_trait]
impl bb8::CustomizeConnection<DieselPgConn, ManageError> for ConnectionCustomizer {
    async fn on_acquire(&self, connection: &mut DieselPgConn) -> Result<(), ManageError> {
        connection
            .batch_execute_async("hi")
            .await
            .unwrap(); // TODO: FIX ME!
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    use users::dsl;

    let manager = async_bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
    let pool = bb8::Pool::builder()
        .connection_customizer(Box::new(ConnectionCustomizer {}))
        .build(manager)
        .await
        .unwrap();


    // Insert.
    let _ = diesel::insert_into(dsl::users)
        .values((dsl::id.eq(0), dsl::name.eq("Jim")))
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
}
