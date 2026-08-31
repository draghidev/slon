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
Every wire receives the same prepared statement before it becomes schedulable. In `stream` mode,
the response retains UTF-8 field memory through rendering, avoiding per-row strings and byte arrays.
`collect` exercises the one-await collector and materializes strings before rendering. Both Slon
pool modes disable zero-byte reads to match Apex's ordinary BCL transport shape.

Npgsql uses a slim data source and a command bound to each leased connection. Every strategy
appends and ordinally sorts the same logical model and renders through the same RazorSlices UTF-8
template.

The Crank configuration uses two fewer Slon connections than database cores and 256 Npgsql
connections.
