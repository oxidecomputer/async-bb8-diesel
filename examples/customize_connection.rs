//! An example showing how to cutomize connections while using pooling.

use async_bb8_diesel::{AsyncSimpleConnection, ConnectionError, DieselConnection};
use async_trait::async_trait;
use diesel::pg::PgConnection;

#[derive(Debug)]
struct ConnectionCustomizer {}

type DieselPgConn = DieselConnection<PgConnection>;

#[async_trait]
impl bb8::CustomizeConnection<DieselPgConn, ConnectionError> for ConnectionCustomizer {
    async fn on_acquire(&self, connection: &mut DieselPgConn) -> Result<(), ConnectionError> {
        connection
            .batch_execute_async("please execute some raw sql for me")
            .await
    }
}

#[tokio::main]
async fn main() {
    let manager = async_bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
    let _ = bb8::Pool::builder()
        .connection_customizer(Box::new(ConnectionCustomizer {}))
        .build(manager)
        .await
        .unwrap();
}
