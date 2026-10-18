# #2728 read-only pre-implementation census

Baseline: fe9fb54200cff883ce337b377c02e791d6d195bd.
User ruling relayed by LEAD/orchestrator:
https://github.com/division-sh/swarm/issues/2728#issuecomment-6253279779

No production or test edits, proof execution, PR push or hosted dispatch.
This packet is a PLAN, not qualification or a new product decision.

The production inventory lists every file directly using the effective ledger.
The test inventory/reference-site TSV is a lexical caller census, not a proof
root parser or an assertion that every referencing test is ledger-only.
The pre-analysis distinguishes pure deleted-ledger pinning from live coordinate,
presence, ownership and retained-source invariants; no live oracle is waived.

The real planner accepts the proposed lifecycle + catalog-replay-2-3 and
codemod-candidate-overlay body on this baseline. Its inventory emission is not
a test run or final-head qualification. The gate decides the actual selection.

Adjacent pin/connection coordinates and agent field-source classifications are
explicitly flagged for boundary confirmation, not silently deleted or exempted.
Implementation still waits for Gate D and #2735/#2741 delivery.
