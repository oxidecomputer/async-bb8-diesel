# Tests

This directory includes some integration tests, specifically with the
CockroachDB database, which uses a PostgreSQL protocol.

Tests expect that the CockroachDB binary exist, and fail without it.

To download a local copy of CockroachDB:

```bash
./tools/ci_download_cockroachdb
```

This downloads a binary to `./out/cockroachdb/bin/cockoach`. If you'd like
to use your own binary here, replace this binary with your own executable.
