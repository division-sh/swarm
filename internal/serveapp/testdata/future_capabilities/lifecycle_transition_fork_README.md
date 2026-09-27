# Root Fork Proof Contract

Binding disposition: https://github.com/division-sh/swarm/issues/2415#issuecomment-5654548365.

`lifecycle_transition_fork_original_test.go.txt` is the byte-identical original
`internal/serveapp/lifecycle_transition_fork_test.go` at `edae2a094`.
SHA256: `4204b142a6927d8d45babede97d77ba42fe2c986be16b82fc2aeb9e3c78831de`.
`TestRetainedRootForkOriginalArtifactPreserved` enforces that identity. This is
not a passing test, unexplained skip, or expected-failure wrapper.

## Assertion Mapping

- `frozen_gate`: original final consumer, approved result, compiled gate cause,
  child history, parent pending card and parent state isolation remain the
  complete future selected-deferred-control oracle under #642. Copying a frozen
  card is not permission to execute it. Current coverage is
  `TestServedCompiledFrozenGateForkControlRefusalOnBothStores`: actual fork and
  copied card, frozen definition/context/outcomes, exact child anchor/source,
  repeated exact nonretryable refusal, all-application-table equality, parent
  card isolation and exact fork idempotency. No domain or completion table is
  omitted from the control-request snapshots.
- `open_loop`: the retained original absent-source assumption no longer describes
  public event admission: a public root input now records root routing provenance.
  Its declared loop field must project to the fork-local revision. Current
  supported-surface coverage is `TestServedCompiledRootLoopForkProjectsRevisionOnBothStores`:
  real loop/pause/frontier/fork, exact source and projected child payloads,
  fresh child generation, delivered transition, idempotency, and parent isolation.
  `TestOriginalLoopCarriageAbsentSourceGrantsNoOwnership` retains the separate
  absent-source negative; an absent source cannot infer a generation from a
  receiver or matching field name.
- The later `loop.close` final-consumer, closed activation and exact compiled
  cause assertions remain preserved in the original artifact. They additionally
  require selected deferred-control support under #642, even when supplied a
  valid child revision. This is distinct from the root-owned projection above.
- Supported source-owned generation execution remains independently covered by
  `TestServedForkLoopGenerationStateEffectBothStores` and the retained four-scope
  static-fork positives. They are not substitutes for the current gate refusal.

## Reproduction

From a checkout, set `ROOT` to its absolute path. Use this Go overlay (substitute
`ROOT` in both paths) to replace only the current-policy test file with the exact
original source. Shared helpers are included; no production file is replaced:

```json
{"Replace":{"ROOT/internal/serveapp/lifecycle_transition_fork_test.go":"ROOT/internal/serveapp/testdata/future_capabilities/lifecycle_transition_fork_original_test.go.txt"}}
```

```sh
go test -overlay /absolute/path/root-fork-original-overlay.json ./internal/serveapp -run '^TestServedCompiledTransitionForkIsolationOnBothStores$' -count=1 -timeout=3m
```

Expected current result: RED on both databases, at the two policy boundaries
above. The lead independently reproduced this at `1224c99b3` in 34.876s. No
future success, external-process proof or deferred-execution closure is claimed.
