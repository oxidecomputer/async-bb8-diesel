//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

mod async_traits;
mod connection;
mod connection_manager;
mod error;

pub use async_traits::{
    AsyncConnection, AsyncRunQueryDsl, AsyncSaveChangesDsl, AsyncSimpleConnection,
};
pub use connection::Connection;
pub use connection_manager::ConnectionManager;
pub use error::{ConnectionError, ConnectionResult, OptionalExtension, PoolError, PoolResult};
