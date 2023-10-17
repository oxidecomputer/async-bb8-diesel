//! An async wrapper around a [`diesel::Connection`].

use crate::async_traits::PostgresConn;

use async_trait::async_trait;
use diesel::r2d2::R2D2Connection;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::task;

/// An async-safe analogue of any connection that implements
/// [`diesel::Connection`].
///
/// These connections are created by [`crate::ConnectionManager`].
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
    pub(crate) fn inner(&self) -> MutexGuard<'_, C> {
        self.0.lock().unwrap()
    }
}

#[async_trait]
impl<Conn> crate::AsyncSimpleConnection<Conn> for Connection<Conn>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> Result<(), diesel::result::Error> {
        let diesel_conn = Connection(self.0.clone());
        let query = query.to_string();
        task::spawn_blocking(move || diesel_conn.inner().batch_execute(&query))
            .await
            .unwrap() // Propagate panics
    }
}

#[async_trait]
impl<Conn> crate::AsyncConnection<Conn> for Connection<Conn>
where
    Conn: 'static + R2D2Connection + PostgresConn,
    Connection<Conn>: crate::AsyncSimpleConnection<Conn>,
{
    type OwnedConnection = Connection<Conn>;

    async fn get_owned_connection(&self) -> Self::OwnedConnection {
        Connection(self.0.clone())
    }

    fn as_sync_conn(owned: &Self::OwnedConnection) -> MutexGuard<'_, Conn> {
        owned.inner()
    }

    fn as_async_conn(owned: &Self::OwnedConnection) -> &Connection<Conn> {
        owned
    }
}
