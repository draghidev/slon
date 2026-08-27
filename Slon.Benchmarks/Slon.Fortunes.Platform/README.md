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
| `SLON_PIPELINING` | Positive per-wire in-flight limit for Slon |

Invalid, unsupported, or missing selections fail application startup with an explicit error.
The Crank config defaults `branchOrCommit` to `main`; override it when benchmarking an
unmerged branch.

## Driver strategies

Slon uses a fixed-size `SlonDataSource` and one data-source-bound command prepared at startup.
The prepared command is reused concurrently and pipelines requests across the configured
connections. Npgsql uses a slim data source and a command bound to each leased connection.
Both drivers materialize messages with `GetString`, append and ordinally sort the same string
model, and render the same RazorSlices string template for a fair comparison.

The Crank configuration uses two fewer Slon connections than database cores and 256 Npgsql
connections. `SLON_PIPELINING` is set to 64 for the low-level Platform benchmark.
