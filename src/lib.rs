//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

use async_trait::async_trait;
use diesel::{
    connection::{Connection, SimpleConnection},
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    r2d2::{self, ManageConnection, R2D2Connection},
    QueryResult,
};
use std::sync::{Arc, Mutex};
use thiserror::Error;
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
///     let mgr = async_bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
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
pub struct DieselConnectionManager<T> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
}

impl<T: Send + 'static> DieselConnectionManager<T> {
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
impl<T> bb8::ManageConnection for DieselConnectionManager<T>
where
    T: R2D2Connection + Send + 'static,
{
    type Connection = DieselConnection<T>;
    type Error = ConnectionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(DieselConnection::new)
            .map_err(ConnectionError::Checkout)
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        let c = DieselConnection(conn.0.clone());
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
    fn inner(&self) -> std::sync::MutexGuard<'_, C> {
        self.0.lock().unwrap()
    }
}

/// Syntactic sugar around a Result returning an [`ConnectionError`].
pub type ConnectionResult<R> = Result<R, ConnectionError>;

/// Errors returned directly from DieselConnection.
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

/// An async variant of [`diesel::connection::SimpleConnection`].
#[async_trait]
pub trait AsyncSimpleConnection<Conn, E>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> Result<(), E>;
}

// We allow callers to treat the entire pool as a connection, if they want.
//
// This provides an implementation that automatically checks out a single
// connection and acts upon it.
#[async_trait]
impl<Conn> AsyncSimpleConnection<Conn, PoolError> for bb8::Pool<DieselConnectionManager<Conn>>
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

// Additionally, we allow callers to act upon manually checked out connections.
#[async_trait]
impl<Conn> AsyncSimpleConnection<Conn, ConnectionError> for DieselConnection<Conn>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> ConnectionResult<()> {
        let diesel_conn = DieselConnection(self.0.clone());
        let query = query.to_string();
        task::spawn_blocking(move || diesel_conn.inner().batch_execute(&query))
            .await
            .unwrap() // Propagate panics
            .map_err(ConnectionError::from)
    }
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn, E>: AsyncSimpleConnection<Conn, E>
where
    Conn: 'static + Connection,
{
    async fn run<R, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static;

    async fn transaction<R, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static;
}

#[async_trait]
impl<Conn> AsyncConnection<Conn, PoolError> for bb8::Pool<DieselConnectionManager<Conn>>
where
    Conn: 'static + R2D2Connection,
    bb8::Pool<DieselConnectionManager<Conn>>: AsyncSimpleConnection<Conn, PoolError>,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> PoolResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.map_err(PoolError::from)?;
        task::spawn_blocking(move || f(&mut *conn.inner()))
            .await
            .unwrap() // Propagate panics
            .map_err(PoolError::from)
    }

    #[inline]
    async fn transaction<R, Func>(&self, f: Func) -> PoolResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.map_err(PoolError::from)?;
        task::spawn_blocking(move || {
            let mut conn = conn.inner();
            conn.transaction(|c| f(c))
        })
        .await
        .unwrap() // Propagate panics
        .map_err(PoolError::from)
    }
}

#[async_trait]
impl<Conn> AsyncConnection<Conn, ConnectionError> for DieselConnection<Conn>
where
    Conn: 'static + R2D2Connection,
    DieselConnection<Conn>: AsyncSimpleConnection<Conn, ConnectionError>,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> ConnectionResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let diesel_conn = DieselConnection(self.0.clone());
        task::spawn_blocking(move || f(&mut *diesel_conn.inner()))
            .await
            .unwrap() // Propagate panics
            .map_err(ConnectionError::from)
    }

    #[inline]
    async fn transaction<R, Func>(&self, f: Func) -> ConnectionResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let diesel_conn = DieselConnection(self.0.clone());
        task::spawn_blocking(move || {
            let mut conn = diesel_conn.inner();
            conn.transaction(|c| f(c))
        })
        .await
        .unwrap() // Propagate panics
        .map_err(ConnectionError::from)
    }
}

/// An async variant of [`diesel::query_dsl::RunQueryDsl`].
#[async_trait]
pub trait AsyncRunQueryDsl<Conn, AsyncConn, E>
where
    Conn: 'static + Connection,
{
    async fn execute_async(self, asc: &AsyncConn) -> Result<usize, E>
    where
        Self: ExecuteDsl<Conn>;

    async fn load_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn get_result_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn get_results_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn first_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>;
}

#[async_trait]
impl<T, AsyncConn, Conn, E> AsyncRunQueryDsl<Conn, AsyncConn, E> for T
where
    T: 'static + Send + RunQueryDsl<Conn>,
    Conn: 'static + Connection,
    AsyncConn: Send + Sync + AsyncConnection<Conn, E>,
{
    async fn execute_async(self, asc: &AsyncConn) -> Result<usize, E>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(conn)).await
    }

    async fn load_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(conn)).await
    }

    async fn get_result_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(conn)).await
    }

    async fn get_results_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(conn)).await
    }

    async fn first_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(conn)).await
    }
}

#[async_trait]
pub trait AsyncSaveChangesDsl<Conn, AsyncConn, E>
where
    Conn: 'static + Connection,
{
    async fn save_changes_async<Output>(self, asc: &AsyncConn) -> Result<Output, E>
    where
        Self: Sized,
        Conn: diesel::query_dsl::UpdateAndFetchResults<Self, Output>,
        Output: Send + 'static;
}

#[async_trait]
impl<T, AsyncConn, Conn, E> AsyncSaveChangesDsl<Conn, AsyncConn, E> for T
where
    T: 'static + Send + Sync + diesel::SaveChangesDsl<Conn>,
    Conn: 'static + Connection,
    AsyncConn: Send + Sync + AsyncConnection<Conn, E>,
{
    async fn save_changes_async<Output>(self, asc: &AsyncConn) -> Result<Output, E>
    where
        Conn: diesel::query_dsl::UpdateAndFetchResults<Self, Output>,
        Output: Send + 'static,
    {
        asc.run(|conn| self.save_changes(conn)).await
    }
}
