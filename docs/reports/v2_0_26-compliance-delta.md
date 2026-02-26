# EmpireAI v2.0.26 Compliance Delta Report

Generated: 2026-02-26 (UTC)

## Validation executed
- `python3 scripts/verify_wiring.py` -> `PASS=382 FAIL=0 WARN=0`
- `go run ./scripts/runtime_payload_audit.go` -> `runtime events: 33, findings: 0`
- `go test ./internal/runtime ./internal/dashboard ./cmd/empire -count=1` -> pass
- `go test ./... -count=1` -> pass

## Summary
Core runtime wiring and payload contracts are currently healthy. Remaining non-compliance is concentrated in a small set of implementation drifts against v2.0.26 behavioral requirements.

- Critical gaps: 0
- High gaps: 2
- Medium gaps: 1
- Low gaps: 1

## Implementation gaps

| Severity | Gap | Evidence | Spec expectation | Required fix |
|---|---|---|---|---|
| High | `local_services` scan dispatch fanout is incomplete (1 scanner only) | `internal/runtime/pipeline_coordinator.go:1205` publishes only `scanner.google_maps.scan_assigned`; `internal/runtime/pipeline_coordinator.go:3639` (`expectedAgents`) returns `1` for `local_services` | v2.0.26 defines local-services scan completion set as 5 scanner types and expected agents aligned to that (`docs/specs/empireai-v2_0_26.md:1975`, `docs/specs/empireai-v2_0_26.md:1986`, `docs/specs/empireai-v2_0_26.md:1988`) | Emit all 5 `scanner.{type}.scan_assigned` events for `local_services` and set expected count to 5 (or derived equivalent), keeping accumulator/completion logic consistent. |
| High | Inbound dedup cleanup retention is 24h, not 7 days | `cmd/empire/main.go:4364` uses `cutoff := time.Now().Add(-24 * time.Hour)` in `inboundCleanupLoop` | v2.0.26 aligns inbound dedup retention to 7 days (`docs/specs/empireai-v2_0_26.md:11416`, `docs/specs/empireai-v2_0_26.md:11418`) | Change cleanup cutoff to 7 days and keep batched purge logic. |
| Medium | `empire init` default template version is stale (`2.0.14`) | `cmd/empire/init.go:45` default flag value `"2.0.14"`; tests assert same in `cmd/empire/init_test.go:10` | Current release baseline should default to current template line (v2.0.26) unless explicitly overridden | Update default `--template-version` to `2.0.26`, then update/init tests accordingly. |
| Low | Runtime pause/dead-letter events embed stale `spec_version` (`v2.0.23`) | `internal/runtime/manager.go:2005`, `internal/runtime/manager.go:2101` | Version metadata should reflect active spec/runtime release | Replace hardcoded value with current version constant (single source), e.g. `v2.0.26`. |

## Notes
- No prompt/tool/schema wiring gaps were detected by current strict verifier.
- No runtime-emitted payload completeness gaps were detected by the payload audit.
- The gaps above are behavioral/version-alignment drifts rather than architecture-level breakages.

## Recommended remediation order
1. Fix `local_services` fanout/expected-count mismatch.
2. Fix inbound retention to 7 days.
3. Bump default init template version and associated tests.
4. Replace stale hardcoded `spec_version` literals with one runtime constant.
