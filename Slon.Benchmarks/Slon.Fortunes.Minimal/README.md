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
| `SLON_FLOW_POOL_CAPACITY` | Retained `CommandFlow` count; omitted or `0` disables flow pooling |

Invalid, unsupported, or missing selections fail application startup with an explicit error.
The Crank config currently composes `sebros/slon-benchmarks` with Draghi's
`experiment/observation-frontier`; override either revision after those experiments land.

## Driver strategies

Slon uses its experimental lower layer through `ConnectionPool<T>`. Setting
`SLON_FLOW_POOL_CAPACITY` reuses `CommandFlow` instances after framework retirement; leaving it
unset creates a fresh flow per request. Every wire receives the same prepared statement before it becomes
schedulable. Results are consumed through `CommandResult.CollectAsync`; result retention keeps
borrowed UTF-8 fields valid through Razor rendering without per-row strings or byte arrays. Zero-byte
reads are disabled to match Apex's ordinary BCL transport shape.

Npgsql uses a slim data source and a command bound to each leased connection. Both drivers append,
ordinally sort, and render the same UTF-8 model; Npgsql returns each field as an allocated byte array
because its reader does not retain row storage through rendering.

The Crank configuration retains up to 1024 Slon flows, uses two fewer Slon connections than
database cores, and uses 256 Npgsql connections; Npgsql needs the additional in-flight operations
to hide network and query latency. Set `slonFlowPoolCapacity=0` for the unpooled comparison.
