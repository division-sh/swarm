# Catalog Fixture Spec Gaps Handoff

## Purpose

This note captures spec-vs-runtime gaps surfaced while fixing and promoting the catalog fixtures.

Scope:
- spec clarification only
- no runtime changes proposed here

Current baseline:
- `go test ./... -count=1` is green
- `cataloge2e` is green
- the remaining fixture backlog is now mostly concrete YAML cleanup, not open-ended architecture work

## Summary

The findings split into 3 buckets:

1. likely spec/runtime mismatches
2. true spec ambiguity
3. runtime behavior that needs sharper wording in the spec

## 1. Likely Spec/Runtime Mismatches

These are written as required in the spec, but the live loader/runtime does not appear to enforce them, and many passing fixtures omit them.

### `schema.yaml` `name` / `namespace`

Observed behavior:
- passing fixtures frequently omit explicit `name` and `namespace`
- the runtime still boots and executes them

Implication:
- either the spec is too strict
- or the loader is implicitly deriving these values and the spec should say so

Recommended clarification:
- mark them as optional when derivable from package context, or
- explicitly document the derivation/defaulting rule

### Agent `emit_events`

Observed behavior:
- many passing fixtures omit `emit_events` when an agent emits nothing
- the runtime accepts this

Implication:
- the spec should not present `emit_events` as unconditionally required if an empty omission is valid

Recommended clarification:
- make `emit_events` optional when empty, or
- specify that omission means `[]`

### Agent `id`

Observed behavior:
- fixtures use the YAML map key as the effective agent ID
- explicit `id:` fields are typically absent

Implication:
- the runtime’s real contract is “agent key is the id”

Recommended clarification:
- make `id` optional and derived from the map key, or
- explicitly state that the YAML object key is the canonical agent identifier

## 2. True Spec Ambiguity

### `data_accumulation` format

Observed behavior:
- the team found conflicting descriptions in the spec
- the runtime and fixtures have had to support multiple shapes/dialects

Implication:
- this creates unnecessary fixture churn and compatibility logic

Recommended clarification:
- choose one canonical `data_accumulation` shape
- explicitly mark any older shape as legacy compatibility only
- provide one minimal valid example and one complex valid example

## 3. Behavior That Needs Sharper Wording

### Transitions from terminal states

Observed behavior:
- the runtime now blocks transitions out of terminal states by default
- this surfaced in fixtures like `test-compose-clear-gates-reenter`

Important nuance:
- this is narrower than “backward transitions are forbidden”
- the actual live rule is closer to:
  - terminal-state exits are blocked unless explicitly allowed

Recommended clarification:
- specify whether terminal states are absorbing by default
- define the opt-in mechanism for reopening/re-entering if that is allowed
- avoid framing it as a generic prohibition on all backward transitions unless that is truly intended

## Recommendation to Spec Writer

Please resolve these in the spec in this order:

1. Define loader-derived defaults
- `schema.name`
- `schema.namespace`
- agent `id`
- agent `emit_events` when omitted

2. Resolve the `data_accumulation` dialect conflict
- pick one canonical shape
- label the other as legacy if it must remain supported

3. Clarify terminal-state exit semantics
- terminal states absorbing by default or not
- how reopening is declared when intended

## Why This Matters

These gaps caused:
- repeated fixture drift
- unnecessary runtime compatibility shims
- confusion over whether a failing fixture reflected a runtime bug, a stale fixture, or a spec mismatch

Clarifying them will make future fixture work much cheaper and reduce false-positive audit churn.
