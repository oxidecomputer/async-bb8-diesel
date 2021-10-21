//! An async wrapper around a [`diesel::Connection`].

use crate::{ConnectionError, ConnectionResult};
use async_trait::async_trait;
use diesel::{r2d2::R2D2Connection, QueryResult};
use std::sync::{Arc, Mutex};
use tokio::task;

/// An async-safe analogue of any connection that implements
/// [`diesel::Connection`].
///
/// These connections are created by [`ConnectionManager`].
///
/// All blocking methods within this type delegate to
/// [`tokio::task::spawn_blocking`], meaning they won't block
/// any asynchronous work or threads.
pub struct Connection<C>(pub(crate) Arc<Mutex<C>>);

impl<C> Connection<C> {
    pub fn new(c: C) -> Self {
        Self(Arc::new(Mutex::new(c)))
    }

    // Accesses the underlying connection.
    //
    // As this is a blocking mutex, it's recommended to avoid invoking
    // this function from an asynchronous context.
    pub(crate) fn inner(&self) -> std::sync::MutexGuard<'_, C> {
        self.0.lock().unwrap()
    }
}

#[async_trait]
impl<Conn> crate::AsyncSimpleConnection<Conn, ConnectionError> for Connection<Conn>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> ConnectionResult<()> {
        let diesel_conn = Connection(self.0.clone());
        let query = query.to_string();
        task::spawn_blocking(move || diesel_conn.inner().batch_execute(&query))
            .await
            .unwrap() // Propagate panics
            .map_err(ConnectionError::from)
    }
}

#[async_trait]
impl<Conn> crate::AsyncConnection<Conn, ConnectionError> for Connection<Conn>
where
    Conn: 'static + R2D2Connection,
    Connection<Conn>: crate::AsyncSimpleConnection<Conn, ConnectionError>,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> ConnectionResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let diesel_conn = Connection(self.0.clone());
        task::spawn_blocking(move || f(&mut *diesel_conn.inner()))
            .await
            .unwrap() // Propagate panics
            .map_err(ConnectionError::from)
    }
}
