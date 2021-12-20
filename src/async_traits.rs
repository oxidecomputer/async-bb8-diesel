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
pub trait AsyncSimpleConnection<Conn, ConnErr>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> Result<(), ConnErr>;
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn, ConnErr>: AsyncSimpleConnection<Conn, ConnErr>
where
    Conn: 'static + DieselConnection,
{
    /// Runs the function `f` in an context where blocking is safe.
    ///
    /// Any error may be propagated through `f`, as long as that
    /// error type may be constructed from `ConnErr` (as that error
    /// type may also be generated).
    async fn run<R, E, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<ConnErr> + Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static;

    async fn transaction<R, E, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DieselError> + From<ConnErr> + Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
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
    E: From<DieselError> + Send + 'static,
{
    async fn execute_async(self, asc: &AsyncConn) -> Result<usize, E>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(conn).map_err(E::from)).await
    }

    async fn load_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(conn).map_err(E::from)).await
    }

    async fn get_result_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(conn).map_err(E::from)).await
    }

    async fn get_results_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, E>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(conn).map_err(E::from))
            .await
    }

    async fn first_async<U>(self, asc: &AsyncConn) -> Result<U, E>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(conn).map_err(E::from)).await
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
    E: 'static + Send + From<DieselError>,
{
    async fn save_changes_async<Output>(self, asc: &AsyncConn) -> Result<Output, E>
    where
        Conn: diesel::query_dsl::UpdateAndFetchResults<Self, Output>,
        Output: Send + 'static,
    {
        asc.run(|conn| self.save_changes(conn).map_err(E::from))
            .await
    }
}
