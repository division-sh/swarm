# Event Identity And Routing Implementation Plan

## Goal

Implement the updated platform spec for canonical event identity and pin-based cross-flow routing without relying on ad hoc name-resolution logic spread across the runtime.

## Principles

- One shared event-identity resolver module.
- Local handler keys stay local.
- The bus stores and routes external names.
- Cross-flow routing defaults to pin-based auto-wiring.
- Explicit scoped subscriptions remain an escape hatch.
- Ambiguous auto-wiring fails boot.

## Work Plan

1. Extract a shared event-identity module.
- Centralize:
  - localize external event name to local handler key
  - externalize local event name to bus-visible name
  - resolve route patterns
  - wildcard/pattern matching helpers as needed

2. Refactor existing runtime users onto the shared resolver.
- `internal/runtime/contracts`
- `internal/runtime/bus`
- `internal/runtime/pipeline`
- `internal/runtime/bootverify`

3. Implement pin-based cross-flow auto-wiring at boot.
- Build cross-flow routes from matching `pins.outputs.events` to `pins.inputs.events`.
- Preserve explicit scoped subscriptions as an escape hatch.

4. Define precedence and ambiguity rules in code.
- Intra-flow local subscriptions remain local.
- Auto-wired cross-flow routes are default.
- Explicit scoped subscriptions override/augment where declared.
- Ambiguous pin matches fail boot unless explicitly disambiguated.

5. Tighten handler lookup.
- Any routed external event delivered to a node must be localized via the shared resolver before handler lookup.
- This must work for:
  - auto-wired pin routes
  - explicit scoped subscriptions
  - root events

6. Tighten boot verification.
- Validate ambiguous auto-wiring.
- Validate that auto-wired routes land on resolvable local handlers.
- Validate explicit scoped escape hatches against known source outputs where possible.

7. Add focused regression tests.
- Cross-flow qualified event localizes to local handler key.
- Pin-based auto-wiring from source output pin to target input pin.
- Ambiguous auto-wiring fails boot.
- Escape-hatch scoped subscription still works.
- Root events still route correctly.

8. Add end-to-end fixtures.
- Auto-wiring-only flow composition fixture.
- Escape-hatch fixture.
- Assert emitted event, downstream handler execution, and state advance.

9. Improve observability.
- Record:
  - incoming routed event name
  - localized handler key
  - source flow
  - target flow/node
  - whether route came from auto-wiring or explicit scoped subscription

## Current Status

Completed:

- shared event-identity module exists in [eventidentity.go](../internal/runtime/core/eventidentity/eventidentity.go)
- centralized helpers now cover:
  - normalization
  - leaf-name extraction
  - route-segment splitting
  - wildcard/pattern matching
  - localize-for-flow
  - externalize-for-flow
  - descendant externalization
- runtime users moved onto the shared helper across:
  - contracts
  - bus
  - pipeline
  - bootverify
- pin-based cross-flow auto-wiring is implemented in route derivation
- ambiguous cross-flow pin matches fail boot unless an explicit scoped escape hatch exists
- routed-event handler lookup localizes external names back to local handler keys
- publish diagnostics now record:
  - routed recipients
  - matched route pattern
  - route source
  - localized target event name
- turn blocks now persist publish diagnostics for flight-recorder/debug use

Still remaining:

- one final live validation pass after restart against the latest Empire contracts
- broader end-to-end fixture coverage for:
  - auto-wiring-only routing
  - explicit scoped escape-hatch routing
- optional final cleanup of lower-value helper sites that inspect event names but do not own routing semantics

## Recommended Order

1. Shared resolver extraction
2. Contracts + handler lookup refactor
3. Route derivation refactor
4. Auto-wiring implementation
5. Boot ambiguity validation
6. Focused tests
7. End-to-end fixtures
8. Observability additions

## Definition Of Done

- No subsystem has its own private event-name localization logic.
- Cross-flow routing works without manual scoped subscriptions in the normal case.
- Explicit scoped subscriptions still work as escape hatches.
- Ambiguous auto-wiring fails boot.
- Routed events never silently miss handlers because of local vs external name mismatch.
