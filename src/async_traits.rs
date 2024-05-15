//! Async versions of traits for issuing Diesel queries.

use crate::connection::Connection;
use async_trait::async_trait;
use diesel::{
    connection::{
        Connection as DieselConnection, SimpleConnection, TransactionManager,
        TransactionManagerStatus,
    },
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    r2d2::R2D2Connection,
    result::Error as DieselError,
};
use futures::future::BoxFuture;
use futures::future::FutureExt;
use std::any::Any;
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

#[cfg(feature = "cockroach")]
fn retryable_error(err: &DieselError) -> bool {
    use diesel::result::DatabaseErrorKind::SerializationFailure;
    match err {
        DieselError::DatabaseError(SerializationFailure, _boxed_error_information) => true,
        _ => false,
    }
}

/// An async variant of [`diesel::r2d2::R2D2Connection`].
#[async_trait]
pub trait AsyncR2D2Connection<Conn>: AsyncConnection<Conn>
where
    Conn: 'static + DieselConnection + R2D2Connection,
    Self: Send + Sized + 'static,
{
    async fn ping_async(&mut self) -> diesel::result::QueryResult<()> {
        self.as_async_conn().run(|conn| conn.ping()).await
    }

    async fn is_broken_async(&mut self) -> bool {
        self.as_async_conn()
            .run(|conn| Ok::<bool, ()>(conn.is_broken()))
            .await
            .unwrap()
    }
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn>: AsyncSimpleConnection<Conn>
where
    Conn: 'static + DieselConnection,
    Self: Send + Sized + 'static,
{
    #[doc(hidden)]
    fn get_owned_connection(&self) -> Self;
    #[doc(hidden)]
    fn as_sync_conn(&self) -> MutexGuard<'_, Conn>;
    #[doc(hidden)]
    fn as_async_conn(&self) -> &Connection<Conn>;

    /// Runs the function `f` in an context where blocking is safe.
    async fn run<R, E, Func>(&self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        let connection = self.get_owned_connection();
        connection.run_with_connection(f).await
    }

    #[doc(hidden)]
    async fn run_with_connection<R, E, Func>(self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        spawn_blocking(move || f(&mut *self.as_sync_conn()))
            .await
            .unwrap() // Propagate panics
    }

    #[doc(hidden)]
    async fn run_with_shared_connection<R, E, Func>(self: &Arc<Self>, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        Func: FnOnce(&mut Conn) -> Result<R, E> + Send + 'static,
    {
        let conn = self.clone();
        spawn_blocking(move || f(&mut *conn.as_sync_conn()))
            .await
            .unwrap() // Propagate panics
    }

    #[doc(hidden)]
    async fn transaction_depth(&self) -> Result<u32, DieselError> {
        let conn = self.get_owned_connection();

        Self::run_with_connection(conn, |conn| {
            match Conn::TransactionManager::transaction_manager_status_mut(&mut *conn) {
                TransactionManagerStatus::Valid(status) => {
                    Ok(status.transaction_depth().map(|d| d.into()).unwrap_or(0))
                }
                TransactionManagerStatus::InError => Err(DieselError::BrokenTransactionManager),
            }
        })
        .await
    }

    // Diesel's "begin_transaction" chooses whether to issue "BEGIN" or a
    // "SAVEPOINT" depending on the transaction depth.
    //
    // This method is a wrapper around that call, with validation that
    // we're actually issuing the BEGIN statement here.
    #[doc(hidden)]
    async fn start_transaction(self: &Arc<Self>) -> Result<(), DieselError> {
        if self.transaction_depth().await? != 0 {
            return Err(DieselError::AlreadyInTransaction);
        }
        self.run_with_shared_connection(|conn| Conn::TransactionManager::begin_transaction(conn))
            .await?;
        Ok(())
    }

    // Diesel's "begin_transaction" chooses whether to issue "BEGIN" or a
    // "SAVEPOINT" depending on the transaction depth.
    //
    // This method is a wrapper around that call, with validation that
    // we're actually issuing our first SAVEPOINT here.
    #[doc(hidden)]
    async fn add_retry_savepoint(self: &Arc<Self>) -> Result<(), DieselError> {
        match self.transaction_depth().await? {
            0 => return Err(DieselError::NotInTransaction),
            1 => (),
            _ => return Err(DieselError::AlreadyInTransaction),
        };

        self.run_with_shared_connection(|conn| Conn::TransactionManager::begin_transaction(conn))
            .await?;
        Ok(())
    }

    #[doc(hidden)]
    async fn commit_transaction(self: &Arc<Self>) -> Result<(), DieselError> {
        self.run_with_shared_connection(|conn| Conn::TransactionManager::commit_transaction(conn))
            .await?;
        Ok(())
    }

    #[doc(hidden)]
    async fn rollback_transaction(self: &Arc<Self>) -> Result<(), DieselError> {
        self.run_with_shared_connection(|conn| {
            Conn::TransactionManager::rollback_transaction(conn)
        })
        .await?;
        Ok(())
    }

    /// Issues a function `f` as a transaction.
    ///
    /// If it fails, asynchronously calls `retry` to decide if to retry.
    ///
    /// This function throws an error if it is called from within an existing
    /// transaction.
    #[cfg(feature = "cockroach")]
    async fn transaction_async_with_retry<R, Func, Fut, RetryFut, RetryFunc, 'a>(
        &'a self,
        f: Func,
        retry: RetryFunc,
    ) -> Result<R, DieselError>
    where
        R: Any + Send + 'static,
        Fut: FutureExt<Output = Result<R, DieselError>> + Send,
        Func: (Fn(Connection<Conn>) -> Fut) + Send + Sync,
        RetryFut: FutureExt<Output = bool> + Send,
        RetryFunc: Fn() -> RetryFut + Send + Sync,
    {
        // This function sure has a bunch of generic parameters, which can cause
        // a lot of code to be generated, and can slow down compile-time.
        //
        // This API intends to provide a convenient, generic, shim over the
        // dynamic "transaction_async_with_retry_inner" function below, which
        // should avoid being generic. The goal here is to instantiate only one
        // significant "body" of this function, while retaining flexibility for
        // clients using this library.

        // Box the functions, and box the return value.
        let f = |conn| {
            f(conn)
                .map(|result| result.map(|r| Box::new(r) as Box<dyn Any + Send>))
                .boxed()
        };
        let retry = || retry().boxed();

        // Call the dynamically dispatched function, then retrieve the return
        // value out of a Box.
        self.transaction_async_with_retry_inner(&f, &retry)
            .await
            .map(|v| *v.downcast::<R>().expect("Should be an 'R' type"))
    }

    // NOTE: This function intentionally avoids all generics!
    #[cfg(feature = "cockroach")]
    async fn transaction_async_with_retry_inner(
        &self,
        f: &(dyn Fn(Connection<Conn>) -> BoxFuture<'_, Result<Box<dyn Any + Send>, DieselError>>
              + Send
              + Sync),
        retry: &(dyn Fn() -> BoxFuture<'_, bool> + Send + Sync),
    ) -> Result<Box<dyn Any + Send>, DieselError> {
        // Check out a connection once, and use it for the duration of the
        // operation.
        let conn = Arc::new(self.get_owned_connection());

        // Refer to CockroachDB's guide on advanced client-side transaction
        // retries for the full context:
        // https://www.cockroachlabs.com/docs/v23.1/advanced-client-side-transaction-retries
        //
        // In short, they expect a particular name for this savepoint, but
        // Diesel has Opinions on savepoint names, so we use this session
        // variable to identify that any name is valid.
        //
        // TODO: It may be preferable to set this once per connection -- but
        // that'll require more interaction with how sessions with the database
        // are constructed.
        Self::start_transaction(&conn).await?;
        conn.run_with_shared_connection(|conn| {
            conn.batch_execute("SET LOCAL force_savepoint_restart = true")
        })
        .await?;

        loop {
            // Add a SAVEPOINT to which we can later return.
            Self::add_retry_savepoint(&conn).await?;

            let async_conn = Connection(Self::as_async_conn(&conn).0.clone());
            match f(async_conn).await {
                Ok(value) => {
                    // The user-level operation succeeded: try to commit the
                    // transaction by RELEASE-ing the retry savepoint.
                    if let Err(err) = Self::commit_transaction(&conn).await {
                        // Diesel's implementation of "commit_transaction"
                        // calls "rollback_transaction" in the error path.
                        //
                        // We're still in the transaction, but we at least
                        // tried to ROLLBACK to our savepoint.
                        if !retryable_error(&err) || !retry().await {
                            // Bail: ROLLBACK the initial BEGIN statement too.
                            let _ = Self::rollback_transaction(&conn).await;
                            return Err(err);
                        }
                        // ROLLBACK happened, we want to retry.
                        continue;
                    }

                    // Commit the top-level transaction too.
                    Self::commit_transaction(&conn).await?;
                    return Ok(value);
                }
                Err(user_error) => {
                    // The user-level operation failed: ROLLBACK to the retry
                    // savepoint.
                    if let Err(first_rollback_err) = Self::rollback_transaction(&conn).await {
                        // If we fail while rolling back, prioritize returning
                        // the ROLLBACK error over the user errors.
                        return match Self::rollback_transaction(&conn).await {
                            Ok(()) => Err(first_rollback_err),
                            Err(second_rollback_err) => Err(second_rollback_err),
                        };
                    }

                    // We rolled back to the retry savepoint, and now want to
                    // retry.
                    if retryable_error(&user_error) && retry().await {
                        continue;
                    }

                    // If we aren't retrying, ROLLBACK the BEGIN statement too.
                    return match Self::rollback_transaction(&conn).await {
                        Ok(()) => Err(user_error),
                        Err(err) => Err(err),
                    };
                }
            }
        }
    }

    async fn transaction_async<R, E, Func, Fut, 'a>(&'a self, f: Func) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DieselError> + Send + 'static,
        Fut: Future<Output = Result<R, E>> + Send,
        Func: FnOnce(Connection<Conn>) -> Fut + Send,
    {
        // This function sure has a bunch of generic parameters, which can cause
        // a lot of code to be generated, and can slow down compile-time.
        //
        // This API intends to provide a convenient, generic, shim over the
        // dynamic "transaction_async_with_retry_inner" function below, which
        // should avoid being generic. The goal here is to instantiate only one
        // significant "body" of this function, while retaining flexibility for
        // clients using this library.

        let f = Box::new(move |conn| {
            f(conn)
                .map(|result| result.map(|r| Box::new(r) as Box<dyn Any + Send>))
                .boxed()
        });

        self.transaction_async_inner(f)
            .await
            .map(|v| *v.downcast::<R>().expect("Should be an 'R' type"))
    }

    // NOTE: This function intentionally avoids as many generic parameters as possible
    async fn transaction_async_inner<'a, E>(
        &'a self,
        f: Box<
            dyn FnOnce(Connection<Conn>) -> BoxFuture<'a, Result<Box<dyn Any + Send>, E>>
                + Send
                + 'a,
        >,
    ) -> Result<Box<dyn Any + Send>, E>
    where
        E: From<DieselError> + Send + 'static,
    {
        // Check out a connection once, and use it for the duration of the
        // operation.
        let conn = Arc::new(self.get_owned_connection());

        // This function mimics the implementation of:
        // https://docs.diesel.rs/master/diesel/connection/trait.TransactionManager.html#method.transaction
        //
        // However, it modifies all callsites to instead issue
        // known-to-be-synchronous operations from an asynchronous context.
        conn.run_with_shared_connection(|conn| {
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
        let async_conn = Connection(Self::as_async_conn(&conn).0.clone());
        match f(async_conn).await {
            Ok(value) => {
                conn.run_with_shared_connection(|conn| {
                    Conn::TransactionManager::commit_transaction(conn).map_err(E::from)
                })
                .await?;
                Ok(value)
            }
            Err(user_error) => {
                match conn
                    .run_with_shared_connection(|conn| {
                        Conn::TransactionManager::rollback_transaction(conn).map_err(E::from)
                    })
                    .await
                {
                    Ok(()) => Err(user_error),
                    Err(err) => Err(err),
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
