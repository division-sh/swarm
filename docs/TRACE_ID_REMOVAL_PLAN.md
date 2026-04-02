# Trace ID Removal Plan

`trace_id` is now redundant with the combination of:

- `run_id` for execution identity
- `source_event_id` for causal lineage

This document tracks the removal stream so the runtime, tools, and specs converge on one model instead of carrying both indefinitely.

## Target model

- `run_id` is the only execution identity.
- Causality is reconstructed from `source_event_id` and parent links.
- `trace_id` is first deprecated, then removed.

## Execution order

1. Deprecate `trace_id` in the platform spec and internal docs.
2. Move operator tooling to `run_id` first.
3. Replace trace-shaped store helpers and cancellation/reporting APIs.
4. Remove transport and correlation dependence on `trace_id`.
5. Stop persisting `trace_id` in active paths.
6. Drop schema columns and indexes.

## Detailed steps

### 1. Spec and doc deprecation

- Mark `trace_id` as deprecated in the platform spec.
- Clarify that `run_id` is the authoritative execution key.
- Clarify that lineage queries should use `source_event_id`.
- Update internal runtime docs to treat `trace_id` as legacy compatibility only.

### 2. Operator tooling

- Make `swarm status` purely `run_id`-first.
- Keep trace fallback only for legacy logs produced before the run-scoped logging tranche.
- Add causal/event-subtree tooling based on `event_id` rather than trace traversal.

### 3. Store and observability helpers

- Replace `trace`-centric reporting helpers with:
  - run report
  - causal subtree report from `event_id`
  - run cancellation by `run_id`
- Remove `trace_cancel.go` and any trace-only APIs after replacements exist.

### 4. Runtime correlation and transport

- Stop treating `trace_id` as a primary correlation field in event correlation.
- Remove `X-SWARM-Trace-Id` and `trace_id` query propagation from MCP/CLI transport.
- Keep compatibility reads only during the migration window.

### 5. Persistence

- Stop writing `trace_id` into new runtime logs, events, and turns once all call sites have moved.
- Keep schema columns temporarily for compatibility and historical reads.

### 6. Schema cleanup

- Remove:
  - `events.trace_id`
  - `agent_turns.trace_id`
  - related indexes
- Delete legacy tests and fixtures that still assert trace propagation.

## Completion criteria

The stream is complete when all of the following are true:

- No active runtime behavior requires `trace_id`.
- Operator tooling is run-shaped or event-lineage-shaped.
- New writes no longer stamp `trace_id`.
- Only compatibility readers mention `trace_id`.
- Schema and transport fields are removed.
