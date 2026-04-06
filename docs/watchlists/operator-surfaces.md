# Operator Surfaces Watchlist

Canonical home for operator-facing truth: dashboard, CLI, builder, API/read-models, observability, and run-scoped query ownership.

## Active Issues

- `#129` Introduce one canonical operator projection for agent health and backlog.
- `#130` Preserve retryable-vs-terminal receipt outcome in canonical operator surfaces.
- `#131` Define a shared operator observability contract across CLI, dashboard, and builder.
- `#134` Stop dashboard conversation and turn APIs from widening typed persisted data into loose carriers.
- `#138` Canonicalize native capability visibility and recording in CLI execution.
- `#148` Make `run_id` canonical across replay, diagnostics, API/dashboard scoping, and operator surfaces.

## Reserve Backlog

- `17.` Separate live session state from conversation/turn audit state.
  - follow-on priority: issue `#166`
- `31.` Remove remaining raw-response summary inference from agent/status readers.
  - follow-on priority: issue `#167`
- `32.` Add conformance checks for missing canonical summary/runtime-log/mutation rows.
  - follow-on priority: issue `#168`
- `Lower Priority 4.` Formalize an operator-facing observability contract across status/debug/conversation APIs.
  - already covered by issue `#131`
