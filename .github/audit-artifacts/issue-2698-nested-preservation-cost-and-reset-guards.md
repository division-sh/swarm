# #2698 nested preservation cost and reset correspondence

Track: FULL. Local repair only, not the final landing head. The PR branch is not
pushed by this checkpoint. Authority: reviewer-b 6214334661, continuing
6213085335/6213797930; cold-origin measurement reported in 6214618025.

## Source and preservation

- Bind source-blob/setup/batch validation to one complete owned input image.
  Re-enumerate membership, HEAD and actual bytes before success and at the end of
  the top-level proof. Decode the actual source-blob manifest, not a replacement
  for its current bytes. Use the existing unique-body parser for live sources.
- Reuse immutable declaration parsing by full content digest; clone returned
  maps. Preserve all 29 standing-ingress pins and existing manifest, ownership,
  duplicate/missing, inverse-order and changed/copied/warm controls.
- Authenticate native rule sets once in AdmissionSnapshots and restore each
  complete history through the same invocation's ordered setup/batch/blob pins.
  Every full-ledger, field/order/multiplicity and mutant check remains. Remove
  the three unused family145Check*WholeRule wrappers after their exact lookup is
  consolidated into the existing rule matcher; do not suppress unused findings.
- Profiled repeated JSON decoding consumed 55.72 of 81.76 CPU seconds in
  AdmissionSnapshots. Seven existing inverse owners now reuse only immutable
  decoding AFTER their original complete-byte digest checks. Pin structs contain
  scalar fields; returned slices are cloned. New controls prove changed/missing
  bytes and caller mutation cannot borrow a warmed result. No source-validity
  decision is cached by path or mtime.
- ResetCompletion compares its original physical SQL with existing
  runlifecycle.CountRunIdentityStorageTx, separately proving the complete native
  bridge, selected owner, runID, context, transaction and error behavior.
- ResetRecoveryAftermath follows the moved bridge in test_event_support.go to
  existing eventrecord.CountGlobalNamedEventStorageTx. Whole-store event-name
  scope remains exact. The old declaration location must remain empty, with an
  injected duplicate control. Other queries, replay and fault assertions remain.

## Measurements and receipts

All files below are retained in /home/youmew/.cache/swarm-2542-family145/.
Different load/cache conditions mean these observations are not controlled
speedup ratios or proof that either complete unit fits its existing limit.

- pr2698-admission-snapshot-profile.jsonl and .cpu: PASS82.465s;
  AdmissionSnapshots82.37s. The profile identifies repeated immutable decoding.
- pr2698-decoded-history-caches-and-invocation-cost.jsonl: PASS68.844s;
  AdmissionSnapshots34.76s, immutable inverse controls0.01s,
  source-image end-change controls0.43s, complete source-fork history33.57s.
- pr2698-final-nested-history-reset-controls.jsonl: PASS47.187s, seven roots;
  run-delivery complete history24.84s, source-blob complete history21.76s,
  both reset query/bridge groups and source-image controls pass.
- pr2698-standing-owner-complete-copy-warm-controls.jsonl: complete existing
  selectedreadrole package PASS82.916s; servedsettlement PASS0.601s.
- pr2698-standing-parser-race-controls.jsonl: both parser/multiplicity/caller
  isolation roots PASS under race1.011s.
- pr2698-nested-source-cost-real-ratchet.jsonl: PASS393.805s before the final
  unused-wrapper cleanup. Debt3798/raw2150, zero added; collector7898e5f7...
  unchanged. This is not an exact-final-head preflight receipt.
- pr2698-bounded-history-invocation-and-hostiles.jsonl retains an intermediate
  end-image refusal caused by editing its proof file during the focused run;
  the subsequent frozen-source focused run above passes. Do not erase that RED.

## Cold historical census blocker

pr2698-freshness-measured-diagnostic.jsonl is the authorized phase-only diagnostic,
not qualification: TIMEOUT600.299s. Composed74fd91d46e source materialization0.878s,
packages.Load298.759s/838 variants; lane7224af504 source materialization0.821s,
packages.Load258.267s/838 variants. Reconciliation of that origin completes at
581.828s with3807 vs3806 actual occurrences and no unmatched additions or
resurrection. Incominga2621ebde materializes in1.707s and its distinct cache MISS
still loads at the600s deadline. No third-origin PASS is claimed.

The control previously keyed analysis to the inspected archive's policy rather
than the executing analyzer. Its bounded repair uses the existing executing
checkout cache owner for all three immutable inputs, including the composed
source; later controls can reuse only the same exact completed tuple. Local
replacement invalidation and protected-publisher/PR-read-only policy are intact.
Cold distinct scans remain required. No placement, selector, timeout, cache trust
or scanner change is made. The qualification-owner disposition is pending.

The initial narrower phase overlay's interrupt is retained separately; it is
not qualification. The final diagnostic orphaned a go-list child; its exact
diagnostic archive cwd was verified before stopping that child alone.

## Open Obligations

Both original concurrently interrupted whole units remain incomplete. The next
codemod-owner-guards measurement is sequential/on its own, count-1, p1,
GOMAXPROCS2, unchanged600s. The census cold-origin issue is not retried unchanged.
No complete-unit or fresh canonical preflight PASS is claimed here.

The 41-row and one-row source certificates, historical endpoints, registry4704
keys and collector successor remain unchanged; no debt-deletion credit is taken.
Docker disconnect=false, schema no-launch, ManagedEmit and ColdStatic shutdown
REDs remain unwaived. Frozen HumanTask/Q1 and H2 actual-execution requirements,
194/155 holds and separate S1/S2 disposition stay visible. This checkpoint does
not clear those runtime obligations or authorize a qualifying PR push.
