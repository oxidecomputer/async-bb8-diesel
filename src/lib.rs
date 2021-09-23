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
    r2d2::R2D2Connection,
    QueryResult,
};
use tokio::task;

mod diesel_connection;
mod diesel_connection_manager;
mod error;

pub use diesel_connection::DieselConnection;
pub use diesel_connection_manager::DieselConnectionManager;
pub use error::{ConnectionError, ConnectionResult, PoolError, PoolResult};

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
