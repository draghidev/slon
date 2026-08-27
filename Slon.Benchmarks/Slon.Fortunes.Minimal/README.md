# Slon Minimal APIs Fortunes

This standalone Minimal APIs benchmark exposes `GET /fortunes`. Every request loads all
`fortune` rows, appends the standard request-time fortune, sorts by message, and renders the
HTML response with RazorSlices HTML encoding.

## Selection

Set the following configuration values as environment variables or equivalent .NET configuration:

| Setting | Values |
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
connections; Npgsql needs the additional in-flight operations to hide network and query latency.
`SLON_PIPELINING` is set to 16.
