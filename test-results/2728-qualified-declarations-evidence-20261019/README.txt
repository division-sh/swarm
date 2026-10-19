#2747 / #2728 qualified declaration coordinate repair.

Source: 91fcf145b0a7ca73ec2975523256720251fec327, signed GOOD.
Tree: e1cefff3feb127f4f9f376d68b3b1173bdedd544.
Base: 0fdbcfb2b7afd7404b4237945e99973a47d63a45.
Pre-rebase repair: 1c7aa6d54c3d735e256c1b6687928651284c8b66.
Clean rebase: all five patches compare unchanged (=); no conflict resolution.

Reviewer-d's invariant/reach and bounded repair authorization:
https://github.com/division-sh/swarm/pull/2747#issuecomment-6260389734
Valid bot finding: 4310592210. Full authored slash-qualified keys must retain
exact declaration coordinates without selecting same-leaf sibling declarations.

Smallest repair: consume localName from the existing canonical declaration
owner, matched to compiled flow/event/source. No new mechanism, ledger, side
map, guessed leaf fallback, source cache or host read. Delta +9/-4 production;
43 test lines add distinct root/nested and sibling negative-control reaches
inside an existing root. Zero added roots; no concurrency changes. Whole PR
production +227/-840, net -613, 1067 touched; tests +173/-536.

RED retained: original fca3652b94 plus the test-only extension, before repair.
Both checked-in tier11 examples lose available coordinates; four same-leaf
sibling cases incorrectly select line 1 instead of the actual lines 2/3.
GREEN retained: all 11 affected roots pass after repair. On the clean rebased
source, those 11 plus TestPersistenceAuthorityFindingRegistry pass (12 roots,
zero failures/skips). Existing refusal/order, alias/merge, missing-source,
retained receiver, missing-index and noncanonical identity controls remain.

The initial preflight refused before phases because the candidate was not
uploaded. After backup upload, the 1c7aa6d54c attempt passed 24 phases but
refused certification at vet when origin/master changed during execution.
Its vet log has no diagnostic. This incomplete trust-identity receipt and all
logs remain here; it is NOT claimed green or a qualifying rerun. Final-source
preflight and the fresh receipt are appended after completion.

No new policy, registry accounting, budgets, timeouts, test deletions, golden
changes or admission-coordinate-owner changes. The accepted two-row registry
retirement is preserved. Selection stays lifecycle + catalog-replay-2-3, local
focused; this packet does not claim hosted qualification or merge approval.

FINAL exact-head preflight PASS: 739.700 seconds, all 25 phases passed,
complete=true, zero unstarted. Published preflight/local at the source above.
Canonical receipt and every phase log are under preflight/. Final stdout and
the fresh receipt machine footer are retained alongside the original REDs.
