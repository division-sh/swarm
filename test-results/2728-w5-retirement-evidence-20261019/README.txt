#2747 / #2728: retired W5 ledger-substring assertion only.

Source: 62ad7f67d631d12068f14657c790daba91b8a2fc, signed GOOD.
Tree: d03447f5b6068a0fe11d6e74f0107c8e40431186.
Base: a1b901ebb3ca387084c8e7a1c7e176d7a90aebb2.
Previous PR source: 91fcf145b0a7ca73ec2975523256720251fec327.
Clean rebase: all six patches compare unchanged (=), no conflict resolution.

Original hosted RED: run 39207647967, job 117915696066.
https://github.com/division-sh/swarm/pull/2747#issuecomment-6261154692
Reviewer-d's subsequent disposition, relayed by the orchestrator: DELETE only
the three-line ledger-substring assertion; keep the mixed root and every public
verify/describe success, tool-schema, module-grant and literal-policy check.

Exact repair: one file, three lines removed; zero production LOC, zero added or
removed roots. No production behavior, test case, other assertion, accounting,
guard, golden, CI selection, budget or timeout changed. The earlier qualified
event-coordinate repair remains unchanged. No concurrency edits or race x3.

Retained RED: exactly one local invocation at published 91fcf145b0, reproducing
the retired tools/input_schema provenance-substring expectation. Public verify
and describe succeeded; describe JSON parsed correctly. The old fatal assertion
prevented the later negative cases from running. It is NOT claimed green.

GREEN at final source: 12 roots pass, zero failures/skips; all four existing W5
negative children execute and pass, along with the later module-grant denial and
literal policy.modules checks. Companion W5 agent/tool/module/schema/retained-
source controls and all 45 compiled-describe cells pass with update mode OFF.
Actual executed roots and child names are in the focused summary and full log.

Earlier receipts retain their original head attribution. Fresh exact-head
preflight log, canonical receipt and machine footer are appended upon completion.
Selection unchanged: lifecycle + catalog-replay-2-3 / local focused. No hosted
qualification or exact-head approval is claimed by this evidence packet.

FINAL exact-head preflight: PASS, 638.295 seconds, all 25 phases passed,
complete=true, zero unstarted. Published preflight/local at the source above.
Canonical receipt and every phase log are under preflight/. Final preflight
stdout and fresh machine footer are retained alongside the original RED.
