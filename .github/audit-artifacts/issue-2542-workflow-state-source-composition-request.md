# Workflow-state whole dependency: qualification hold

Part of #2542 / #2151. Signed predecessor
b44b2ff3711b3dc8e65ce278edaeea49594153be, now approved for stacking.
Standing bulk gate; local only, no push/rebase/PR/tier/server2.

## Implemented boundary

Entire requireLifecycleFlowEntity /requirePendingInputStateCount dependency:
28 calls /12 declarations /two helpers, across nine served caller files.
Original raw-container parameters are removed. Workflow entity identity
consumes pipelinepersistence.ReadWorkflowStateEntityStorageTx; exact
run/state entity cardinality consumes entitystore.CountRunStateEntityStorageTx.
Both use existing selected receiver-state inspection, original native read
coordination and fixed SQL-free storetest ports. No new fixture construction,
SQL callback, arbitrary query, raw pool or transaction protocol crosses them.

Exact physical predicates are unchanged: run/instance_path/current_state for
the header entity, and run/current_state for all entity_state rows independent
of header eligibility. No ordering, limit, status, readiness or join is added.
Consumer trailing-slash trimming, root substitution, original background read
context, fifteen-second deadline, twenty-millisecond poll and diagnostics stay.
Only successful absence may keep polling; failed/cancelled/closed ownership
cannot become absence or provisional evidence. Public publication, fork,
explicit-target/sibling exclusion, state/cardinality and restart assertions
remain. Existing raw setup, faults and wider lifecycle interpreters stay parent
debt; these physical facts grant no execution/lifecycle authority.

Authoritative workflow_state_storage_observation clause added. Native peer,
runtime lifecycle and production admission semantics are unchanged.

## Finite proof and corpus

Committed codemod migrate-workflow-state/internal/workflowstate and full output.
Manifest25d031ef995292f317b42fcd901405ca22945818948369ef40a0b4735540cb1f,
immutable b44b2ff37 predecessor. Complete whole-body/owned-census/hostile
controls verify all28 calls and12 bodies, refuse foreign/raw carriers,
shadowing, altered scope/arguments/arity/poll/identity/cuts and alias escapes.
Query controls compare both original SQL literals exactly. Missing raw/nil,
cancelled or closed native owners return no value; missing valid coordinates
return absence/zero, not synthesized state.

Corpus1494 ->1495 /798 ->799 transitions: one addition,11 changed existing
recipe records, no changed Before or dropped rows, reserved entries unchanged.
Transition digestd58dae4c293f6ce31bdce828ceaec7119142e6dc0c8c5314058c99787ae59a39.
New exact inverse composes earlier caller guards. All older manifests remain.
The preceding channel-anchor audit wording is corrected here as requested:
its recipes have ten After updates and one Successor, not eleven After updates.

## Actual qualification

Go1.26.8/GOMAXPROCS=2/-p=1/-count=1; native/served use -race.
Evidence ~/.cache/swarm-2542-family145/workflow-state-*.

- owner-refusals-race: one root/three passes, both backends,15.225s.
- pilot-served-fixed-race: selected-carrier + explicit-target roots,24 passes,
  both backends,142.519s.
- nested-mixed-served-race: nested collision + mixed pending-input completion
  roots,eight passes, both backends,79.132s.
- corpus-composition-fixed:14 roots/1509 passes,64.419s; includes full
  idempotence, lifecycle, admission, fork-caller and entity-conflict/scalar
  composition controls. No failures or skips.
- complete structural/source/import/retirement sweep:82 roots/459 passes,
  four packages all pass. U1000 and new-callable cognition below30 pass.
- real-ratchet-downward: **RED**,148.921s, before any baseline/metadata write:
  reviewed served-debug provenance/body changed at
  selected_fork_pending_input_mixed_test.go /
  TestSelectedForkPendingInputMixedCompletionBothStores.

Development reds: removed now-unused sql import; a tool compilation overlapped
corpus generation and failed on changing embedded-file length. Sequenced
generation followed by the same source controls passed. No retry-until-green,
timeout increase, skip or contract/assertion change.

## Requested exact accounting disposition

The current mixed pending-input body is the complete native-read successor
pinned in the new manifest; its two state-count calls now use the existing
receiver inspector. It remains inside the earlier served-debug source proof,
whose original complete After must stay immutable. Request:

1. Compose ONLY exact file/function/full current After + predecessor/hash
   evidence through this new finite inverse before the existing served-debug
   source validation. Preserve every older preimage, signature pair,
   multiplicity, seven-pair terminal-state condition and hostile control.
2. After the minimal main-policy composition is reviewed, authorize only the
   exact active collector88b0724... -> measured new fingerprint transition,
   with existing local-only checksum/source-bound metadata semantics. No new
   signature relocation, classifier relaxation or baseline increase.
3. Then rerun the actual ratchet to establish the decrease; **no removal or
   remaining-count credit is currently claimed**. Baseline remains6885/raw4656
   and excluded67, collector88b0724..., unchanged. Complete overlay/integrated
   landing obligations remain open; unaffected receipts carry.

This is a finite source/accounting composition question, not a runtime defect
or wider production obligation. Do not broadly accept changed bodies or
refresh metadata past a failed guard. Existing #2542/#2151/watchlist remains
sufficient; no new issue or framework is proposed.
