//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.
#![cfg_attr(feature = "usdt-probes", feature(asm))]
#[usdt::provider]
mod async_bb8_diesel {
    // TODO: It'd be nice to include the database URL here, but
    // the diesel connection types only store them in private
    // fields. We can clone it an pass it from the manager to
    // each connection, but then we incur a cost even when the
    // probes are disabled, which is not ideal.
    fn new_connection() {}
    fn query(sql: String) {}
}

mod async_traits;
mod connection;
mod connection_manager;
mod error;

pub use async_traits::{
    AsyncConnection, AsyncRunQueryDsl, AsyncSaveChangesDsl, AsyncSimpleConnection,
};
pub use connection::Connection;
pub use connection_manager::ConnectionManager;
pub use error::{ConnectionError, ConnectionResult, PoolError, PoolResult};
