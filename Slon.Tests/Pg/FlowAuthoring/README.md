# PostgreSQL flow authoring contracts

These tests are the executable lifecycle specification for `PgClientFlow`
implementations. They exercise behavior shared by flow types; SQL semantics and
result-shape tests remain with the implementation.

## Profiles

- `IAutonomousFlowContract` applies when the flow owns its complete pipeline
  task and needs no external consumer to reach RFQ.
- `IConsumerDrivenFlowContract` applies when an external sync or async consumer
  owns result advancement and can abandon that ownership.

A flow exposing both kinds of entry point should register an adapter for every
profile it supports. Profiles are separate interfaces so an implementation
cannot silently opt out of individual required scenarios with capability flags.

## Registering a flow

Implement the relevant adapter in this directory and add its singleton to the
profile's `Implementations` data source. For a consumer-driven flow the complete
adapter is:

```csharp
sealed class MyFlowContract : IConsumerDrivenFlowContract
{
    public string Name => nameof(MyFlow);
    public PgClientFlow Create(bool async, params Command[] commands) =>
        new MyFlow(async, commands);
    public IEnumerator<CommandResult> GetEnumerator(PgClientFlow flow) =>
        ((MyFlow)flow).GetEnumerator();
    public IAsyncEnumerator<CommandResult> GetAsyncEnumerator(PgClientFlow flow) =>
        ((MyFlow)flow).GetAsyncEnumerator();
}
```

The interface conversions may box a value-type enumerator. This suite verifies
correctness rather than allocation behavior; implementation-specific benchmarks
continue to call concrete enumerators directly.

## Contract rules

The suite treats submission, activation, consumer progress, and protocol
lifecycle as independent axes. In particular:

- the pipeline task retains wire/decoder tenure through RFQ;
- consumer abandonment transfers read ownership and leaves the wire reusable;
- forceful termination completes a flow even if no consumer attaches;
- sync execution blocks only its caller and does not require a handoff for an
  autonomous flow;
- an async consumer never runs on the pipeline executor strand;
- release occurs before completion makes a flow reusable.

Contract scenarios must use explicit scheduler gates, in-memory transports, or
`FakeTimeProvider` when ordering matters. Do not use wall-clock delays or
timeouts as correctness oracles.
