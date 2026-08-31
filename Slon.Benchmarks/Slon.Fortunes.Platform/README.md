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

Invalid, unsupported, or missing selections fail application startup with an explicit error.
The Crank config defaults `branchOrCommit` to `main`; override it when benchmarking an
unmerged branch.

## Driver strategies

Slon uses its experimental lower layer through `ConnectionPool<T>` and creates a fresh
`ReaderDrivenCommandFlow` per request. Every wire receives the same prepared statement before it
becomes schedulable. Streaming consumption retains UTF-8 field memory through rendering, avoiding
per-row strings and byte arrays. Zero-byte reads are disabled to match Apex's ordinary BCL transport
shape.

Npgsql uses a slim data source and a command bound to each leased connection. Every strategy
appends and ordinally sorts the same logical model and renders through the same RazorSlices UTF-8
template.

The Crank configuration uses two fewer Slon connections than database cores and 256 Npgsql
connections.
