//! An async-safe connection pool for Diesel.

use crate::{Connection, ConnectionError, PoolError, PoolResult};
use async_trait::async_trait;
use diesel::r2d2::{self, ManageConnection, R2D2Connection};
use std::sync::{Arc, Mutex};
use tokio::task;

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
///     // You can acquire connections to the pool manually...
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute_async(&*pool.get().await.unwrap())
///         .await
///         .unwrap();
///
///     // ... Or just issue them to the pool directly.
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute_async(&pool)
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
            .map_err(ConnectionError::Checkout)
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

#[async_trait]
impl<Conn> crate::AsyncSimpleConnection<Conn, PoolError> for bb8::Pool<ConnectionManager<Conn>>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> PoolResult<()> {
        let self_ = self.clone();
        let query = query.to_string();
        let conn = self_.get_owned().await.map_err(PoolError::from)?;
        task::spawn_blocking(move || conn.inner().batch_execute(&query))
            .await
            .unwrap() // Propagate panics
            .map_err(PoolError::from)
    }
}

#[async_trait]
impl<Conn> crate::AsyncConnection<Conn, PoolError> for bb8::Pool<ConnectionManager<Conn>>
where
    Conn: 'static + R2D2Connection,
    bb8::Pool<ConnectionManager<Conn>>: crate::AsyncSimpleConnection<Conn, PoolError>,
{
    #[inline]
    async fn run<R, E, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<PoolError> + Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.map_err(PoolError::from)?;
        task::spawn_blocking(move || f(&mut *conn.inner()))
            .await
            .unwrap() // Propagate panics
    }
}
