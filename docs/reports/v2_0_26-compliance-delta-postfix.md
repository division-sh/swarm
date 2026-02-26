# EmpireAI v2.0.26 Compliance Delta - Post-Fix Validation

Generated: 2026-02-26 (UTC)

## Scope
This validates closure of the previously identified implementation gaps from `docs/reports/v2_0_26-compliance-delta.md`.

## Previously flagged gaps and status

| Gap | Status | Evidence |
|---|---|---|
| `local_services` fanout was 1 scanner only | RESOLVED | `internal/runtime/pipeline_coordinator.go` now emits all 5 scanner assignments (`scanner.google_maps`, `scanner.instagram`, `scanner.reviews`, `scanner.directories`, `scanner.job_boards`) and `expectedAgents(local_services)=5` via `localServicesScannerExpected`. |
| Inbound dedup cleanup retention 24h vs 7d | RESOLVED | `cmd/empire/main.go` now uses `inboundRetentionCutoff()` with `now.Add(-7 * 24 * time.Hour)`. |
| `empire init` default template version stale (`2.0.14`) | RESOLVED | `cmd/empire/init.go` default `--template-version` updated to `2.0.26`; default assertion updated in `cmd/empire/init_test.go`. |
| Runtime hardcoded `spec_version` stale (`v2.0.23`) | RESOLVED | `internal/runtime/manager.go` now uses `runtimeSpecVersion = "v2.0.26"` for pause/dead-letter event payloads. |

## Additional hardening included
- Added local-services pipeline tests to lock fanout/completion behavior:
  - `internal/runtime/pipeline_coordinator_local_services_test.go`
- Added inbound cleanup retention unit test:
  - `cmd/empire/main_inbound_cleanup_test.go`

## Validation executed
- `go test ./internal/runtime -run "TestFactoryPipelineCoordinator_LocalServices|TestFactoryPipelineCoordinator_RecordsConsumedTransition|TestFactoryPipelineCoordinator_FinalizeScoring_" -count=1 -v` -> pass
- `go test ./cmd/empire -run "TestInboundRetentionCutoffUsesSevenDays|TestParseInitOptions_Defaults|TestParseInitOptions_Overrides" -count=1 -v` -> pass
- `python3 scripts/verify_wiring.py` -> `PASS=386 FAIL=0 WARN=0`
- `go run ./scripts/runtime_payload_audit.go` -> `runtime events: 37, findings: 0`
- `go test ./... -count=1` -> pass

## Conclusion
All four previously identified v2.0.26 implementation gaps are closed in the current code state.
