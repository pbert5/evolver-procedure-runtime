# Procedure sink contract (frozen locally)

This is the local durable freeze for issue #30 because the GitHub mutation is
unavailable. The contract is intentionally declarative: hosts route requests
to trusted implementations; `procedure.sinks` performs no I/O and accepts no
executable extension point.

## Registered identifiers

The initial registry contains exactly:

- `edge.calibration_run.observation`
- `central.calibration_session.observation`
- `procedure.checkpoint.export`

The first two are persistent observation sinks. The third is a persistent
checkpoint-export sink.

## Payloads and effects

Every request has a trusted sink ID, data-only mapping payload, and explicit
idempotency key. Observation requests may carry an ephemeral `SessionBinding`.
Checkpoint requests carry a host-supplied `CheckpointDestination`; that value
is an opaque trusted identifier, not a path, URL, SQL statement, or command.
Session binding, checkpoint destination, and persistent sink identity are
separate typed concepts and are never interchangeable.

## Idempotency and mutation outcomes

The host supplies idempotency keys and owns delivery. This contract performs
no hidden retries. An ambiguous mutation outcome is returned to the host as
`MutationOutcome(status="ambiguous")` and requires host policy/reconciliation.
