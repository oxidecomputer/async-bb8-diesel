//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

use thiserror::Error;

/// Syntactic sugar around a Result returning an [`ConnectionError`].
pub type ConnectionResult<R> = Result<R, ConnectionError>;

/// Errors returned directly from Connection.
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Failed to checkout a connection: {0}")]
    Checkout(#[from] diesel::r2d2::Error),

    #[error("Failed to issue a query: {0}")]
    Query(#[from] diesel::result::Error),
}

/// Syntactic sugar around a Result returning an [`PoolError`].
pub type PoolResult<R> = Result<R, PoolError>;

/// Describes an error performing an operation from a connection pool.
///
/// This is a superset of [`ConnectionError`] which also may
/// propagate errors attempting to access the connection pool.
#[derive(Error, Debug)]
pub enum PoolError {
    #[error("Failed to checkout a connection: {0}")]
    Connection(#[from] ConnectionError),

    #[error("BB8 Timeout accessing connection")]
    Timeout,
}

impl From<diesel::result::Error> for PoolError {
    fn from(error: diesel::result::Error) -> Self {
        PoolError::Connection(ConnectionError::Query(error))
    }
}

impl From<bb8::RunError<ConnectionError>> for PoolError {
    fn from(error: bb8::RunError<ConnectionError>) -> Self {
        match error {
            bb8::RunError::User(e) => PoolError::Connection(e),
            bb8::RunError::TimedOut => PoolError::Timeout,
        }
    }
}
