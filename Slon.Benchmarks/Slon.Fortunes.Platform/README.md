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

Slon uses its experimental lower layer directly. A benchmark-local fixed pool opens
`DATABASE_CONNECTIONS` `PgClientProtocol` instances, prepares the statement once on every wire,
and places flows by atomic round-robin. This deliberately simple outer pool isolates Slon's
protocol/flow baseline. It does not exercise the richer production `SlonDataSource` placement
policy.
The raw Slon arm disables zero-byte reads to match Apex's ordinary BCL transport shape.

Npgsql uses a slim data source and a command bound to each leased connection. Both drivers
materialize messages as strings, append and ordinally sort the same model, and render the same
RazorSlices string template for a fair comparison.

The Crank configuration uses two fewer Slon connections than database cores and 256 Npgsql
connections.
