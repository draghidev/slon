# Slon Platform Fortunes

This standalone Platform-style Fortunes application exposes `GET /fortunes`. It reads every
row from `fortune`, adds the standard request-time fortune, sorts by message, and renders the
standard HTML response with RazorSlices HTML encoding.

## Selection

Set all of these environment variables before starting the app:

| Variable | Values |
| --- | --- |
| `DATABASE` | `postgresql` |
| `DRIVER` | `slon` or `npgsql` |
| `CONNECTION_STRING` | PostgreSQL connection string |
| `DATABASE_CONNECTIONS` | Positive fixed pool size |
| `SLON_POOL_MODE` | `raw` (default) or `connection` |
| `SLON_CONSUMPTION_MODE` | `stream` (default) or `collect` |

Invalid, unsupported, or missing selections fail application startup with an explicit error.
The Crank config defaults `branchOrCommit` to `main`; override it when benchmarking an
unmerged branch.

## Driver strategies

Slon uses its experimental lower layer directly in both modes, and creates a fresh
`ReaderDrivenCommandFlow` per request. `raw` opens `DATABASE_CONNECTIONS` protocols and places
flows by atomic round-robin. `connection` wraps the same protocols in `ConnectionPool<T>` through
the lower-layer `IPoolConnection<T>` seam, exercising production placement without adding ADO.
Every wire receives the same prepared statement before it becomes schedulable.
`SLON_CONSUMPTION_MODE` independently selects nested streaming enumeration or one-await collection.
Both Slon modes disable zero-byte reads to match Apex's ordinary BCL transport shape.

Npgsql uses a slim data source and a command bound to each leased connection. Both drivers
materialize messages as strings, append and ordinally sort the same model, and render the same
RazorSlices string template for a fair comparison.

The Crank configuration uses two fewer Slon connections than database cores and 256 Npgsql
connections.
