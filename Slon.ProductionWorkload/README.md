# Slon production workload

This is a manual, production-shaped workload rather than another schedule-specific test. It mixes:

- commands created directly from a data source, plus multiplexed synchronous and asynchronous batches;
- error-barrier batches which continue through a failed middle command;
- leased connections and synchronously or asynchronously committed, rolled-back, and disposed transactions;
- partially consumed readers and synchronous ADO calls;
- expected SQL errors, command cancellation, and backend termination;
- seeded TCP fragmentation and delay on both ordinary and CancelRequest connections.

Backend termination is deliberately selected independently by each worker. It collateralizes unrelated operations at
arbitrary ADO boundaries, which is useful for finding exception-projection and cleanup holes that isolated tests miss.

The workload creates uniquely named state and audit tables. Every committed transaction increments the state and
inserts an audit row in the same transaction; every rollback attempts both changes before rolling back. A successful
run verifies that the state equals the committed audit count, no rollback marker survived, and the pool remains usable.

Run the default 100,000-operation workload against the local test server:

```sh
dotnet run --project Slon/Slon.ProductionWorkload -c Release
```

Use `--help` for command-line options. Every option also has a `SLON_WORKLOAD_*` environment-variable form. The seed is
printed at startup and in the final report.
