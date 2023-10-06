//! Async versions of traits for issuing Diesel queries.

use crate::connection::Connection as SingleConnection;
use async_trait::async_trait;
use diesel::{
    connection::{Connection as DieselConnection, SimpleConnection, TransactionManager},
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    result::Error as DieselError,
};
use std::future::Future;
use std::sync::Arc;
use std::sync::MutexGuard;
use tokio::task::spawn_blocking;

/// An async variant of [`diesel::connection::SimpleConnection`].
#[async_trait]
pub trait AsyncSimpleConnection<Conn>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> Result<(), DieselError>;
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn>: AsyncSimpleConnection<Conn>
where
    Conn: 'static + DieselConnection,
    Self: Send,
{
    type OwnedConnection: Sync + Send + 'static;

    #[doc(hidden)]
    async fn get_owned_connection(&self) -> Self::OwnedConnection;
    #[doc(hidden)]
    fn as_sync_conn(owned: &Self::OwnedConnection) -> MutexGuard<'_, Conn>;
    #[doc(hidden)]
    fn as_async_conn(owned: &Self::OwnedConnection) -> &SingleConnection<Conn>;

    /// Runs the function `f` in an context where blocking is safe.
    async fn run<R, E, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        let connection = self.get_owned_connection().await;
        Self::run_with_connection(connection, f).await
    }

    #[doc(hidden)]
    async fn run_with_connection<R, E, Func>(
        connection: Self::OwnedConnection,
        f: Func,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        spawn_blocking(move || f(&mut *Self::as_sync_conn(&connection)))
            .await
            .unwrap() // Propagate panics
    }

    #[doc(hidden)]
    async fn run_with_shared_connection<R, E, Func>(
        connection: Arc<Self::OwnedConnection>,
        f: Func,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        spawn_blocking(move || f(&mut *Self::as_sync_conn(&connection)))
            .await
            .unwrap() // Propagate panics
    }

    async fn transaction_async<R, E, Func, Fut, 'a>(&'a self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DieselError> + Send + 'static,
        Fut: Future<Output = Result<R, E>> + Send,
        Func: FnOnce(SingleConnection<Conn>) -> Fut + Send,
    {
        // Check out a connection once, and use it for the duration of the
        // operation.
        let conn = Arc::new(self.get_owned_connection().await);

        // This function mimics the implementation of:
        // https://docs.diesel.rs/master/diesel/connection/trait.TransactionManager.html#method.transaction
        //
        // However, it modifies all callsites to instead issue
        // known-to-be-synchronous operations from an asynchronous context.
        Self::run_with_shared_connection(conn.clone(), |conn| {
            Conn::TransactionManager::begin_transaction(conn).map_err(E::from)
        })
        .await?;

        // TODO: The ideal interface would pass the "async_conn" object to the
        // underlying function "f" by reference.
        //
        // This would prevent the user-supplied closure + future from using the
        // connection *beyond* the duration of the transaction, which would be
        // bad.
        //
        // However, I'm struggling to get these lifetimes to work properly. If
        // you can figure out a way to convince that the reference lives long
        // enough to be referenceable by a Future, but short enough that we can
        // guarantee it doesn't live persist after this function returns, feel
        // free to make that change.
        let async_conn = SingleConnection(Self::as_async_conn(&conn).0.clone());
        match f(async_conn).await {
            Ok(value) => {
                Self::run_with_shared_connection(conn.clone(), |conn| {
                    Conn::TransactionManager::commit_transaction(conn).map_err(E::from)
                })
                .await?;
                Ok(value)
            }
            Err(user_error) => {
                match Self::run_with_shared_connection(conn.clone(), |conn| {
                    Conn::TransactionManager::rollback_transaction(conn).map_err(E::from)
                })
                .await
                {
                    Ok(()) => Err(user_error),
                    Err(err) => Err(err.into()),
                }
            }
        }
    }
}

/// An async variant of [`diesel::query_dsl::RunQueryDsl`].
#[async_trait]
pub trait AsyncRunQueryDsl<Conn, AsyncConn>
where
    Conn: 'static + DieselConnection,
{
    async fn execute_async(self, asc: &AsyncConn) -> Result<usize, DieselError>
    where
        Self: ExecuteDsl<Conn>;

    async fn load_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>;

    async fn get_result_async<U>(self, asc: &AsyncConn) -> Result<U, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>;

    async fn get_results_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>;

    async fn first_async<U>(self, asc: &AsyncConn) -> Result<U, DieselError>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<'static, Conn, U>;
}

#[async_trait]
impl<T, AsyncConn, Conn> AsyncRunQueryDsl<Conn, AsyncConn> for T
where
    T: 'static + Send + RunQueryDsl<Conn>,
    Conn: 'static + DieselConnection,
    AsyncConn: Send + Sync + AsyncConnection<Conn>,
{
    async fn execute_async(self, asc: &AsyncConn) -> Result<usize, DieselError>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(conn)).await
    }

    async fn load_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>,
    {
        asc.run(|conn| self.load(conn)).await
    }

    async fn get_result_async<U>(self, asc: &AsyncConn) -> Result<U, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>,
    {
        asc.run(|conn| self.get_result(conn)).await
    }

    async fn get_results_async<U>(self, asc: &AsyncConn) -> Result<Vec<U>, DieselError>
    where
        U: Send + 'static,
        Self: LoadQuery<'static, Conn, U>,
    {
        asc.run(|conn| self.get_results(conn)).await
    }

    async fn first_async<U>(self, asc: &AsyncConn) -> Result<U, DieselError>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<'static, Conn, U>,
    {
        asc.run(|conn| self.first(conn)).await
    }
}

#[async_trait]
pub trait AsyncSaveChangesDsl<Conn, AsyncConn>
where
    Conn: 'static + DieselConnection,
{
    async fn save_changes_async<Output>(self, asc: &AsyncConn) -> Result<Output, DieselError>
    where
        Self: Sized,
        Conn: diesel::query_dsl::UpdateAndFetchResults<Self, Output>,
        Output: Send + 'static;
}

#[async_trait]
impl<T, AsyncConn, Conn> AsyncSaveChangesDsl<Conn, AsyncConn> for T
where
    T: 'static + Send + Sync + diesel::SaveChangesDsl<Conn>,
    Conn: 'static + DieselConnection,
    AsyncConn: Send + Sync + AsyncConnection<Conn>,
{
    async fn save_changes_async<Output>(self, asc: &AsyncConn) -> Result<Output, DieselError>
    where
        Conn: diesel::query_dsl::UpdateAndFetchResults<Self, Output>,
        Output: Send + 'static,
    {
        asc.run(|conn| self.save_changes(conn)).await
    }
}
