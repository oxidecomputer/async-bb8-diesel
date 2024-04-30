//! An async-safe connection pool for Diesel.

use crate::{Connection, ConnectionError};
use async_trait::async_trait;
use diesel::r2d2::{self, ManageConnection, R2D2Connection};
use std::sync::{Arc, Mutex};

/// A connection manager which implements [`bb8::ManageConnection`] to
/// integrate with bb8.
///
/// ```no_run
/// use async_bb8_diesel::AsyncRunQueryDsl;
/// use diesel::prelude::*;
/// use diesel::pg::PgConnection;
///
/// table! {
///     users (id) {
///         id -> Integer,
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     use users::dsl;
///
///     // Creates a Diesel-specific connection manager for bb8.
///     let mgr = async_bb8_diesel::ConnectionManager::<PgConnection>::new("localhost:1234");
///     let pool = bb8::Pool::builder().build(mgr).await.unwrap();
///
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute_async(&*pool.get().await.unwrap())
///         .await
///         .unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct ConnectionManager<T> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
}

impl<T: Send + 'static> ConnectionManager<T> {
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(r2d2::ConnectionManager::new(database_url))),
        }
    }

    pub fn update_database_url<S: Into<String>>(&self, database_url: S) {
        self.inner.lock().unwrap().update_database_url(database_url)
    }

    async fn run_blocking<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: Send + 'static + FnOnce(&r2d2::ConnectionManager<T>) -> R,
    {
        let cloned = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&*cloned.lock().unwrap()))
            .await
            // Intentionally panic if the inner closure panics.
            .unwrap()
    }
}

#[async_trait]
impl<T> bb8::ManageConnection for ConnectionManager<T>
where
    T: R2D2Connection + Send + 'static,
{
    type Connection = Connection<T>;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(Connection::new)
            .map_err(ConnectionError::Connection)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let c = Connection(conn.0.clone());
        self.run_blocking(move |m| {
            m.is_valid(&mut *c.inner())?;
            Ok(())
        })
        .await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // Diesel returns this value internally. We have no way of calling the
        // inner method without blocking as this method is not async, but `bb8`
        // indicates that this method is not mandatory.
        false
    }
}
