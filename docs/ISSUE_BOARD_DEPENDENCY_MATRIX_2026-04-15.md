# Issue Board Dependency Matrix

Snapshot date: `2026-04-17`
Last refreshed: `2026-04-17` after `#455` rewrite aligned to the expanded event-contract draft and first executable child `#457` was extracted

Purpose:
- durable board snapshot of open-issue status
- explicit blocker/unblocker map
- distinguish active lanes, true blockers, trackers, watchpoints, audits, and stale board entries

Status legend:
- `OPEN`: assignable now
- `BLOCKED`: concrete blocker exists
- `TRACKER`: keep open, but do not implement directly
- `WATCHPOINT`: real backlog item, but not yet a clean direct lane
- `AUDIT-ONLY`: assignable, but audit only
- `STALE`: open board noise; should be closed or retargeted

Core dependency spine:
- `#444 implementation -> #450 -> re-evaluate #401 -> #454 -> then re-evaluate #401 again -> #415/#416 residual extraction -> #417 -> #413`
- `#455 parent workstream -> #457 merged -> #459 next executable child -> later #455 children -> broader contract migration closure`
- `#410 -> #169`
- exact authored blocker `#431` is merged and closed
- runtime boundary-removal child `#426` is merged and closed
- status-projection child `#432` is merged and closed
- later-turn no-`read_file` parity child `#406` is merged and closed
- CLI fallback-era child `#368` is closed as superseded by `#444`
- CLI oversized-result continuation child `#381` is closed as superseded by `#444`
- `#427` was implemented in merged PR `#430` and is closed
- `#429` was implemented in merged PR `#433` and is closed
- `#423` was superseded by the closed `#424` split and is closed

Current high-signal board changes since the original snapshot:
- `#431` closed via merged PR `#435`
- `#432` closed via merged PR `#434`
- `#426` closed via merged PR `#438`
- `#398` closed via merged PR `#439`
- `#406` closed via merged PR `#440`
- `#400` closed via merged PR `#437`
- `#364` was already completed earlier via merged PR `#420`; stale ready state has now been recognized and cleared from the local matrix
- `#441` is closed; the supported path now stalls earlier on the CLI native-tools seam
- `#444` is closed after merge
- `#450` was extracted, gated, implemented, and merged in `#451`
- `#401` is still not the active runtime lane; its former class still does not materialize on the supported path and it is now blocked on exact blocker `#454`
- `#454` is the new exact supported-path blocker in front of validation
- `#455` is open as the cross-cutting parent for the strict event-schema / structured-emit migration
- `#457` is merged as the first executable child under `#455`
- `#459` is now the next executable child under `#455` and the highest-priority implementation lane in that workstream
- `#397` is now gated, ready, and assigned to `a`
- stale child `#399` is now closed as superseded by replacement child `#446`
- `#442` and `#443` are closed as stale/superseded PRs
- `#126` declaration-drift child was already implemented earlier in commit `ae87cf0`; next honest remaining child is now extracted as `#447`
- `#125` no longer advertises stale parent-level `status:ready`
- `#182` is now closed after current-head verification confirmed its extracted children `#389` and `#391` both landed and no further concrete child remains
- `#157` is now closed after current-head verification confirmed its extracted children `#403` and `#411` both landed and no further concrete child remains
- `#170`, `#373`, and `#385` no longer advertise stale child-level `status:ready`
- `#415` no longer advertises stale parent-level `status:ready`; it remains a blocked parent after the `#401/#454` control path

## Matrix

| Issue | State | Unblocked By / Next Dependency | Notes |
| --- | --- | --- | --- |
| `#432` | `CLOSED` | `-` | implemented in merged PR `#434` |
| `#431` | `CLOSED` | `-` | exact authored blocker for `#426`, implemented in merged PR `#435` |
| `#427` | `CLOSED` | `-` | implementation merged in `#430`; board state repaired |
| `#426` | `CLOSED` | `-` | implemented in merged PR `#438` |
| `#423` | `CLOSED / SUPERSEDED` | `-` | older candidate superseded by `#424` split; board state repaired |
| `#455` | `TRACKER / NEEDS-SCOPE PARENT` | `work through extracted children; first executable child is #457` | cross-cutting parent for strict event-schema cross-flow contract migration with structured emit grammar |
| `#457` | `CLOSED` | `-` | first executable child under `#455`: structured `emit:` grammar + emit-site resolution; implemented and merged in `#458` |
| `#459` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | next executable child under `#455`: envelope/payload split + strict system-node emit construction; PRs must update authoritative `platform-spec.yaml` in lockstep |
| `#417` | `BLOCKED` | `#414 + #415 family + #416 family` | deferred cleanup after owner removal |
| `#416` | `TRACKER / NOT DIRECTLY ASSIGNABLE` | `later residual waits on #415 movement and then #414 finalization` | broad parent, not honest direct coding lane |
| `#415` | `TRACKER / BLOCKED PARENT` | `#401/#454 control path, then re-evaluate remaining runtime residuals / child extraction` | broad runtime parent; do not implement directly from the parent |
| `#414` | `BLOCKED` | `#415 family + #416 family stabilization` | spec-last sequencing |
| `#413` | `TRACKER / PARENT` | `closes after #414 + #415 + #416 + #417 family closure` | do not implement directly |
| `#410` | `TRACKER / NEEDS-SCOPE PARENT` | `-` | historical symptom collector; `#424`, `#427`, `#429`, and `#432` are all closed |
| `#406` | `CLOSED` | `-` | implemented in merged PR `#440` |
| `#444` | `CLOSED` | `-` | direct runtime lane for CLI native-tools no-fallback enforcement; merged and closed |
| `#450` | `CLOSED` | `-` | exact supported local launcher false-negative child; implemented and merged in PR `#451` |
| `#441` | `CLOSED` | `-` | closed exact authored blocker; later supported-path control points moved through `#444` then `#450` |
| `#401` | `BLOCKED` | `#454` | supported-path recheck still does not materialize the approved `#401` class; exact blocker is now `#454` |
| `#454` | `OPEN / READY` | `implementation may start under approved first-slice gate, but no longer the clean long-term solution lane` | live scoring -> validation blocker retained as symptom/history and possible tactical lane; broader clean solution now routes through `#455/#457` |
| `#400` | `CLOSED` | `-` | implemented in merged PR `#437` |
| `#399` | `CLOSED / SUPERSEDED` | `-` | stale self-seeding child boundary replaced by `#446` after `#442` review stop proved the remaining class was broader |
| `#398` | `CLOSED` | `-` | implemented in merged PR `#439` |
| `#446` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | replacement child for the stale `#399` boundary; classifies first child-flow subject lineage before self-seeding proof |
| `#397` | `OPEN / READY` | `-` | gated as `approved as first slice`; assigned to `a` |
| `#385` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | stale ready metadata; no thread pre-audit/gate found during triage |
| `#381` | `CLOSED / SUPERSEDED` | `-` | superseded by `#444`; fallback-era CLI continuation child is no longer the right lane |
| `#373` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | only split note exists on thread; no pre-audit/gate found during triage |
| `#368` | `CLOSED / SUPERSEDED` | `-` | superseded by `#444`; fallback-era CLI helper parity child is no longer the right lane |
| `#364` | `CLOSED` | `-` | implemented earlier in merged PR `#420`; stale ready state was local-board drift |
| `#170` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | issue body itself says the first dependency-object slice still must be chosen in pre-audit; stale ready metadata repaired |
| `#126` | `TRACKER / NEEDS-SCOPE PARENT` | `work through extracted children; next remaining child is #447` | not deprecated: merged slices already cover `#294`, `#299`, `#301`, `#309`, `#320`, `#327`, and declaration drift in `ae87cf0`; remaining tail still includes handler/runtime contract-compliance and platform/tooling/environment groups after `#447` |
| `#447` | `OPEN / NEEDS-SCOPE` | `pre-audit + gate required before assignment` | new `#126` child for state/schema target coverage, centered on `payload_field_coverage` + `gate_schema_validation` |
| `#125` | `TRACKER / NEEDS-SCOPE PARENT` | `work through extracted children; live residuals remain in #397 / #446` | not deprecated: `#387`, `#388`, and `#398` are closed; `#399` was superseded by `#446`, and the parent still tracks the remaining residual children without being directly assignable |
| `#375` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#374` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#365` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#363` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#174` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#172` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#169` | `WATCHPOINT / UMBRELLA` | `not direct; closes as child issues land` | umbrella for lifecycle observability/recovery |
| `#165` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#164` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#163` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#161` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#159` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#158` | `WATCHPOINT / NOT DIRECT` | `needs explicit child extraction or deliberate assignment` | parked backlog, not a clean direct lane |
| `#338` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#107` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#106` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#104` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#102` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#101` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#100` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#97` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#96` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#95` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |
| `#94` | `OPEN / AUDIT-ONLY` | `-` | directly assignable for audit work |

## Throughput Notes

What actually blocks throughput right now:
- real blocker chain:
  - `#444` is closed
  - `#450` is closed
  - `#401` is still blocked and the current exact blocker is `#454`
  - `#454` must be classified/cleared before `#401` can be rechecked honestly again
  - `#414` cannot honestly go before `#415/#416` stabilize
  - `#417` stays blocked until runtime/spec/authored owners move
- parallel issue families with no clean child yet:
  - `#125` still has no second clean coding child ready after `#397`; `#446` still needs pre-audit/gate
  - `#126` remains a parent tracker, but its next honest remaining child is now `#447`
  - `#455` is a cross-cutting architecture parent and should move only through extracted children; `#459` is now the active next implementation child

What is directly assignable right now:
- ready implementation with no explicit blocker:
  - `#397`
- highest-priority implementation workstream:
  - `#459` is the next executable child under `#455`, but it still needs pre-audit/gate before assignment
- ready audit-only:
  - `#338`, `#107`, `#106`, `#104`, `#102`, `#101`, `#100`, `#97`, `#96`, `#95`, `#94`

Board hygiene problems:
- `#417` is correctly blocked on the board, but historical thread/comments still carry older ready-language.
- `#126` was previously advertising `status:ready` on the strength of an already-landed child; tracker repair has removed that stale ready state.
- `#385`, `#373`, and `#170` originally read like open-ready candidates, but triage found no thread pre-audit/gate to support assignment; all three are now repaired.
- `#125` and `#415` were previously advertising parent-level readiness; those stale parent states are now repaired on GitHub.
- `#182` is no longer an open parent tracker; it is closed after current-head verification of merged children `#389` and `#391`.
- `#157` is no longer an open parent tracker; it is closed after current-head verification of merged children `#403` and `#411`.
- `#459` is newly extracted and still needs its normal pre-audit/gate record before assignment.
- some agent labels may still be stale relative to actual current work.

Update rule:
- when any blocker or child closes, update both the matrix row and the core dependency spine first
- prefer correcting stale issue state before opening new issues
