//! An async wrapper around a [`diesel::Connection`].

use crate::async_traits::AsyncConnection;
use async_trait::async_trait;
use diesel::r2d2::R2D2Connection;
use diesel::result::Error as DieselError;
use std::sync::atomic::{AtomicBool, Ordering};
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
pub struct Connection<C>(Arc<ConnectionInner<C>>);

pub struct ConnectionInner<C> {
    pub(crate) inner: Mutex<C>,
    pub(crate) broken: AtomicBool,
}

impl<C> Connection<C> {
    pub fn new(c: C) -> Self {
        Self(Arc::new(ConnectionInner {
            inner: Mutex::new(c),
            broken: AtomicBool::new(false),
        }))
    }

    pub(crate) fn clone(&self) -> Self {
        Self(self.0.clone())
    }

    pub(crate) fn mark_broken(&self) {
        self.0.broken.store(true, Ordering::SeqCst);
    }

    // Accesses the underlying connection.
    //
    // As this is a blocking mutex, it's recommended to avoid invoking
    // this function from an asynchronous context.
    pub(crate) fn inner(&self) -> MutexGuard<'_, C> {
        self.0.inner.lock().unwrap()
    }
}

#[async_trait]
impl<Conn> crate::AsyncSimpleConnection<Conn> for Connection<Conn>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> Result<(), diesel::result::Error> {
        if self.is_broken_from_txn() {
            return Err(DieselError::BrokenTransactionManager);
        }

        let diesel_conn = self.clone();
        let query = query.to_string();
        task::spawn_blocking(move || diesel_conn.inner().batch_execute(&query))
            .await
            .unwrap() // Propagate panics
    }
}

#[async_trait]
impl<Conn> crate::AsyncR2D2Connection<Conn> for Connection<Conn> where Conn: 'static + R2D2Connection
{}

#[async_trait]
impl<Conn> crate::AsyncConnection<Conn> for Connection<Conn>
where
    Conn: 'static + R2D2Connection,
    Connection<Conn>: crate::AsyncSimpleConnection<Conn>,
{
    fn get_owned_connection(&self) -> Self {
        self.clone()
    }

    // Accesses the connection synchronously, protected by a mutex.
    //
    // Avoid calling from asynchronous contexts.
    fn as_sync_conn(&self) -> MutexGuard<'_, Conn> {
        self.inner()
    }

    fn as_async_conn(&self) -> &Connection<Conn> {
        self
    }

    fn is_broken_from_txn(&self) -> bool {
        self.0.broken.load(Ordering::SeqCst)
    }
}
