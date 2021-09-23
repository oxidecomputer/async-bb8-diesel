//! An async wrapper around a [`diesel::Connection`].

use std::sync::{Arc, Mutex};

/// An async-safe analogue of any connection that implements
/// [`diesel::Connection`].
///
/// These connections are created by [`DieselConnectionManager`].
///
/// All blocking methods within this type delegate to
/// [`tokio::task::spawn_blocking`], meaning they won't block
/// any asynchronous work or threads.
pub struct DieselConnection<C>(pub(crate) Arc<Mutex<C>>);

impl<C> DieselConnection<C> {
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
