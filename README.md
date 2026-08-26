# Slon

_Slon (Slovene: elephant)_

Slon is a PostgreSQL driver for .NET built around continuous pipelining. It provides a low-level
protocol API and a layered ADO.NET implementation over the same pooled, multiplexed engine.

## Status

Slon targets .NET 10 and is under active pre-release hardening. The source and public API are not
yet a compatibility promise, and no production package has been published.

## Why Slon

A PostgreSQL connection has one ordered wire, but producing requests and consuming responses are
independent sources of progress. Serial request/response execution leaves capacity unused, while a
send-then-receive batching pump creates cliffs around backpressure, streaming, cancellation, and
interactive work.

Slon keeps admitted operations independently live while preserving their FIFO ownership of the
connection. A blocked write does not prevent an earlier response from being consumed, synchronous
callers can drive directly into their handoff, and streaming work remains part of the pipeline
rather than becoming a global barrier.

Failure is handled at the operation's ordered position. When framing remains trustworthy, a
recovery flow can repair PostgreSQL state before already in-flight successors continue on the same
connection. This also lets custom low-level flows share a pipeline without each implementing the
driver's resynchronization protocol.

## Architecture

- `Slon.Pg` exposes commands, rows, parameters, type metadata, and the serializer substrate.
- `Slon.Pg.Protocol` owns PostgreSQL framing, startup, cancellation, flow execution, and recovery.
- The root `Slon` namespace provides the ADO.NET data source, connection, command, batch,
  transaction, reader, and parameter surface.
- Pooling places commands onto live protocols without requiring a leased connection for stateless
  work. Explicit connection scopes remain available for transactions and session state.
- [Draghi.Pipelining](https://github.com/draghidev/pipelining) supplies the two-frontier lifecycle
  engine that coordinates execution, ordered activation, completion, and substitution.

The transport is built on `System.IO.Pipelines` over streams. The protocol and ADO layers share the
same flow machinery rather than maintaining separate execution models.

## Build

Slon currently consumes Draghi as a sibling source checkout:

```text
git clone https://github.com/draghidev/pipelining.git Draghi
git clone https://github.com/draghidev/slon.git Slon
dotnet build Slon/Slon.slnx -c Release
```

## Test

The reproducible test entrypoint starts PostgreSQL 17 through Docker Compose, including SCRAM,
MD5, and cleartext authentication roles:

```text
cd Slon
./test.sh
```

Set `SLON_TEST_PORT` to change the default mapped port `55432`. The test projects also accept
`SLON_TEST_HOST` and `SLON_TEST_PORT` when using an existing PostgreSQL server. Stress suites use
`SLON_STRESS_ITERATIONS`, with `SLON_UNCAPPED=1` reserved for deliberate deep soaks.

## Production-shaped workload

`Slon.ProductionWorkload` exercises mixed ADO.NET usage, transport jitter, cancellation, SQL
errors, backend termination, and recovery over a small multiplexed pool. It is a hardening harness,
not a throughput benchmark. Its own README documents the available controls.

## License

Copyright (c) 2026 Nino Floris. Licensed under the [MIT License](LICENSE).
