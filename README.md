# async-bb8-diesel

This crate provides an interface for asynchronously accessing a bb8 connection
pool atop [Diesel](https://github.com/diesel-rs/diesel).

This is accomplished by implementing an async version
of Diesel's "RunQueryDsl" trait, aptly named "AsyncRunQueryDsl",
which operates on an async-compatible connection. When called
from an async context, these operations transfer the query
to a blocking tokio thread, where it may be executed.

NOTE: This crate pre-dated [diesel-async](https://docs.rs/diesel-async/).
For new code, consider using that interface directly.

# Pre-requisites

- A willingness to tolerate some instability. This crate effectively originated
  as a stop-gap until more native asynchronous support existed within Diesel.

# Comparisons with existing crates

This crate was heavily inspired by both
[tokio-diesel](https://github.com/mehcode/tokio-diesel) and
[bb8-diesel](https://github.com/overdrivenpotato/bb8-diesel), but serves a
slightly different purpose.

## What do those crates do?

Both of those crates rely heavily on the
[`tokio::block_in_place`](https://docs.rs/tokio/1.10.1/tokio/task/fn.block_in_place.html)
function to actually execute synchronous Diesel queries.

Their flow is effectively:
- A query is issued (in the case of tokio-diesel, it's async. In the case
of bb8-diesel, it's synchronous - but you're using bb8, so presumably
calling from an asynchronous task).
- The query and connection to the DB are moved into the `block_in_place` call.
- Diesel's native synchronous API is used within `block_in_place`.

These crates have some advantages by taking this approach:
- The tokio executor knows not to schedule additional async tasks for the
  duration of the `block_in_place` call.
- The callback used within `block_in_place` doesn't need to be `Send` - it
  executes synchronously within the otherwise asynchronous task.

However, they also have some downsides:
- The call to `block_in_place` effectively pauses an async thread for the
  duration of the call. This *requires* a multi-threaded runtime, and reduces
  efficacy of one of these threads for the duration of the call.
- The call to `block_in_place` starves all other asynchronous code running in
  the same task.

This starvation results in some subtle inhibition of other futures, such as in
the following example, where a timeout would be ignored if a long-running
database operation was issued from the same task as a timeout.

```rust
tokio::select! {
  // Calls "tokio::block_in_place", doing a synchronous Diesel operation
  // on the calling thread...
  _ = perform_database_operation() => {},
  // ... meaning this asynchronous timeout cannot complete!
  _ = sleep_until(timeout) = {},
}
```

## What does this crate do?

This crate attempts to avoid calls to `block_in_place` - which would block the
calling thread - and prefers to use
[`tokio::spawn_blocking`](https://docs.rs/tokio/1.10.1/tokio/task/fn.spawn_blocking.html)
function. This function moves the requested operation to an entirely distinct
thread where blocking is acceptable, but does *not* prevent the current task
from executing other asynchronous work.

This isn't entirely free - as this work now needs to be transferred to a new
thread, it imposes a "Send + 'static" constraint on the queries which are
constructed.

## Which one is right for me?

- If you care about preserving typically expected semantics for asynchronous
  operations, we recommend this crate.
- If you don't - maybe you have an asynchronous workload, but you *know*
  when you can block those threads - you can use either of the
  tokio-diesel or bb8-diesel crates, depending on whether or not you
  want access to the asynchronous thread pool.
