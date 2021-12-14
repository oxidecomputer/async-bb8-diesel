//! Async versions of traits for issuing Diesel queries.

use async_trait::async_trait;
use diesel::{
    connection::{Connection as DieselConnection, SimpleConnection},
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    result::Error as DieselError,
};

/// An async variant of [`diesel::connection::SimpleConnection`].
#[async_trait]
pub trait AsyncSimpleConnection<Conn, E>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> Result<(), E>;
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn, E>: AsyncSimpleConnection<Conn, E>
where
    Conn: 'static + DieselConnection,
{
    async fn run<R, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, DieselError> + Send + 'static;

    async fn transaction<R, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, DieselError> + Send + 'static,
    {
        self.run(|conn| conn.transaction(|c| f(c))).await
    }
}

/// An async variant of [`diesel::query_dsl::RunQueryDsl`].
#[async_trait]
pub trait AsyncRunQueryDsl<Conn, AsyncConn, E>
where
    Conn: 'static + DieselConnection,
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
    Conn: 'static + DieselConnection,
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
    Conn: 'static + DieselConnection,
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
    Conn: 'static + DieselConnection,
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
