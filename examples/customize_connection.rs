//! An example showing how to cutomize connections while using pooling.

use std::future::Future;
use std::pin::Pin;

use async_bb8_diesel::{AsyncSimpleConnection, Connection, ConnectionError};
use diesel::pg::PgConnection;

#[derive(Debug)]
struct ConnectionCustomizer {}

type DieselPgConn = Connection<PgConnection>;

impl bb8::CustomizeConnection<DieselPgConn, ConnectionError> for ConnectionCustomizer {
    fn on_acquire<'a>(
        &'a self,
        connection: &'a mut DieselPgConn,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConnectionError>> + Send + 'a>> {
        Box::pin(async move {
            let res = connection
                .batch_execute_async("please execute some raw sql for me")
                .await;
            res.map_err(ConnectionError::from)
        })
    }
}

#[tokio::main]
async fn main() {
    let manager = async_bb8_diesel::ConnectionManager::<PgConnection>::new("localhost:1234");
    let _ = bb8::Pool::builder()
        .connection_customizer(Box::new(ConnectionCustomizer {}))
        .build(manager)
        .await
        .unwrap();
}
