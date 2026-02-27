# v2.0.37 Live Test Report (System Node + Deferred Event Path)

Generated: 2026-02-27 (UTC)  
Environment: `docker compose` (`empireai-orchestrator` + `empireai-postgres`)  
Directive used for live flow: `local services in argentina`

## Scope

Executed the 3 requested layers:

1. Gate manifest (contracts + wiring + system-node gates)
2. Deferred-event bug verification (`source.scraped` -> deferred `vertical.discovered` -> `scoring.requested`)
3. End-to-end live factory path (`system.directive` through scoring output)

## Important DB note

Host `psql` was pointing to local Homebrew Postgres (`PostgreSQL 17.7`) rather than compose Postgres (`PostgreSQL 16.11`).  
All live SQL evidence below was collected via:

```bash
docker compose exec -T postgres psql -U postgres -d empireai ...
```

## Layer 1: Gate Manifest Results

### Gate status summary

| Gate | Result |
|---|---|
| `contracts-yaml-parse` | PASS |
| `ddl-fresh-install` | PASS |
| `ddl-table-count` | PASS |
| `ddl-runtime-tables-match-structs` | PASS |
| `wiring-verifier-clean` | PASS (`PASS=564 FAIL=0 WARN=0`) |
| `wiring-no-glob-subs` | PASS |
| `wiring-no-sc-ghosts` | PASS |
| `wiring-no-uuid-agents` | PASS |
| `wiring-delivery-channel-complete` | PASS |
| `xval-emit-events-in-catalog` | PASS |
| `xval-subscriptions-in-catalog` | PASS |
| `xval-catalog-emitters-exist` | PASS |
| `agents-all-have-prompts` | PASS |
| `agents-naming-convention` | PASS |
| `system-nodes-yaml-parse` | PASS |
| `xval-system-node-events-in-catalog` | PASS |
| `system-node-scoring-parity` | PASS |
| `system-node-replay-idempotency` | PASS |
| `system-node-dead-letter` | PASS |
| `integration-event-roundtrip` | UNVERIFIED (`no tests to run`) |
| `integration-crash-recovery` | UNVERIFIED (`no tests to run`) |
| `integration-dual-assessment` | UNVERIFIED (`no tests to run`) |
| `ddl-schema-diff` | UNVERIFIED (`empire db diff` CLI not available) |

### Notes on the 3 system-node integration gates

- `TestScoringNodeParity`: PASS (simple case validated)
- `TestScoringNodeIdempotency`: PASS (replay safety validated)
- `TestScoringNodeDeadLetter`: PASS (dead-letter emission path validated in integration test)

## Layer 2: Deferred Event Bug Verification (live)

### Scenario

- Triggered live directive via API:

```bash
POST /api/directive
{"directive_text":"local services in argentina"}
```

Response:

```json
{"event_id":"0d4e0f05-a709-4864-8c28-d22b8b9ccc2c","ok":true}
```

### Required proof

Previously broken path: deferred `vertical.discovered` from `source.scraped` was not consumed by scoring logic.  
Expected in v2.0.37: `source.scraped` -> `vertical.discovered` (pipeline coordinator) -> `scoring.requested` (ScoringNode).

### Live evidence

From `runtime_log` (same campaign):

- `source.scraped` published by `scanner-agent`
- `vertical.discovered` published by `pipeline-coordinator`
- `scoring.requested` published by `scoring-node`

Example sequence:

- `03:54:17.325` `source.scraped` (`scanner-agent`)
- `03:54:17.326` `vertical.discovered` (`pipeline-coordinator`)
- `03:54:17.329` `scoring.requested` (`scoring-node`)

### Count parity check

For this campaign window:

- `vertical.discovered = 33`
- `scoring.requested = 33`

No discovered event was dropped before scoring request generation.

### Lag check (`vertical.discovered` -> `scoring.requested`)

Matched verticals: `33`

- `avg_lag_ms = 5.35`
- `max_lag_ms = 9.22`
- `min_lag_ms = 3.15`

Conclusion: async ScoringNode handoff is present and low-latency (milliseconds).

## Layer 3: End-to-End Live Factory Flow (directive -> scoring output)

### Observed live chain

1. `system.directive` (human)
2. `scan.requested` (scan-campaign-manager)
3. scanner assignments (`scanner.*.scan_assigned`)
4. scanner outputs (`source.scraped`)
5. discovery accumulation (`vertical.discovered`)
6. ScoringNode trigger (`scoring.requested`)
7. analysis output (`score.dimension_complete`)
8. scoring result emitted (`vertical.scored`)
9. marginal classification emitted (`vertical.marginal`)
10. campaign closed (`scan.completed` -> `campaign.completed`)

### Concrete scoring output

For vertical `f42dc0c7-c7d6-4e71-b43e-e15ad202b0a0`:

- `03:55:01.577` `score.dimension_complete` (`analysis-agent`)
- `03:55:01.583` `vertical.scored` (`scoring-node`)
- `03:55:01.589` `vertical.marginal` (`scoring-node`)

### Pipeline transitions evidence

`pipeline_transitions` confirms deterministic transitions/events:

- `scan.requested` consumed -> scanner assignments emitted
- repeated `source.scraped` consumed -> `vertical.discovered` emitted
- `vertical.scored` consumed (policy-consumed non-shortlist path)
- final scanner completion -> `scan.completed`
- `scan.completed` consumed by campaign manager -> `campaign.completed`

## Findings / Observations

1. Deferred-event bug is fixed in live runtime.
2. ScoringNode is actively producing `scoring.requested` with low lag.
3. Full local-services campaign produced many discoveries quickly; analysis processing lag exists.
4. Pending delivery backlog observed:
   - `analysis-agent` pending deliveries: `32` (at capture time).
5. In this run window, no `scoring.contested`, `scoring.contest_resolved`, `vertical.shortlisted`, or `vertical.rejected` events were observed.

## Pass/Fail Verdict

- **Layer 1 (gates):** PASS with noted UNVERIFIED optional gates.
- **Layer 2 (deferred-event fix):** PASS (live proof complete).
- **Layer 3 (end-to-end through scoring):** PASS (reached `vertical.scored` + `vertical.marginal` on live run).

## Remaining test debt (from requested edge cases)

Not observed in this single live campaign:

- contested dimension path (`scoring.contested` / `scoring.contest_resolved`)
- partial-timeout scoring path (60m incomplete dimensions)
- dead-letter delivery to Operations Analyst in a live run (integration test passes, live injection not exercised here)

