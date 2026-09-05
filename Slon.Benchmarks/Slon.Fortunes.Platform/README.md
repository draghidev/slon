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
| `SLON_FLOW_POOL_CAPACITY` | Retained `CommandFlow` count; omitted or `0` disables flow pooling |
| `TEMPLATING` | `razor` (default) or `raw` |

Invalid, unsupported, or missing selections fail application startup with an explicit error.
The Crank config currently composes `sebros/slon-benchmarks` with Draghi's
`experiment/observation-frontier`; override either revision after those experiments land.

## Driver strategies

Slon uses its experimental lower layer through `ConnectionPool<T>`. Setting
`SLON_FLOW_POOL_CAPACITY` reuses `CommandFlow` instances after framework retirement; leaving it
unset creates a fresh flow per request. Every wire receives the same prepared statement before it becomes
schedulable. `CommandResult.CollectAsync` is the row-buffering barrier, while result buffering retains
UTF-8 field memory through rendering without per-row strings or byte arrays. Zero-byte reads are
disabled to match Apex's ordinary BCL transport shape.

`TEMPLATING=raw` writes the same encoded HTML directly into the response buffer. It isolates the
driver and pool cost from RazorSlices overhead without changing query or row-consumption behavior.

Npgsql uses a slim data source and a command bound to each leased connection. Every strategy
appends and ordinally sorts the same logical model and renders through the same RazorSlices UTF-8
template.

The Crank configuration retains up to 1024 Slon flows, uses two fewer Slon connections than
database cores, and uses 256 Npgsql connections. Set `slonFlowPoolCapacity=0` for the unpooled
comparison.
