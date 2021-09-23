//! An example showing how to cutomize connections while using pooling.

use async_bb8_diesel::{PoolError, AsyncRunQueryDsl, AsyncSimpleConnection, DieselConnection, ConnectionError};
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
impl bb8::CustomizeConnection<DieselPgConn, ConnectionError> for ConnectionCustomizer {
    async fn on_acquire(&self, connection: &mut DieselPgConn) -> Result<(), ConnectionError> {

        // TODO TODO TODO:
        //
        // This trait *must* return DieselConnectionManager<T>::Error.
        // - This is currently "ConnectionError".
        //
        // However, batch_execute_async wants to return 'AsyncResult'.
        // - Is this right?
        // - Kinda seems like we might want to return different errors
        // from a connection vs the pool...
        connection
            .batch_execute_async("please execute some raw sql for me")
            .await
//            .unwrap(); // TODO: FIX ME!
//        Ok(())
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
        .execute_async(&*pool.get().await.unwrap())
        .await
        .unwrap();
}
