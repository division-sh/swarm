# Channel-anchor whole dependency

Part of #2542 / #2151, standing whole-family bulk gate. Signed predecessor
adc1db8bd24b1dfcedb691d7be8fc3afe83db268. Local-only checkpoint; no push,
rebase, PR, tier or server2. Parent closure and landing are not claimed.

## Boundary and ownership

Chosen class: all six shared raw channel-anchor helpers and their complete
54-call /26-declaration chain, including the three-call standing-flow lookup
prerequisite. The failing/local helpers were entry points, not the boundary.
Binding spec: platform-spec.yaml selected native fixture contracts and the
new channel_anchor_storage_observation clause; decision_cards,
channel_delivery_plans/receipts and standing_services physical contracts.
There is no production lifecycle, dispatch, admission or eligibility change.

| Canonical row owner | Exhaustive touched consumption | Exact retained concept |
| --- | --- | --- |
| decisionstore.ReadChannelAnchorCardsStorageTx | native/private bridge, storetest.ChannelAnchorObservation, waitChannelAnchorCard and waitChannelAnchorCardInState plus all callers in the finite manifest | all exact run/kind rows with original optional status filter; native anchor JSON; consumer DecodeAnchor/Scope/flow/duplicate assertions |
| decisionstore.ReadChannelAnchorDecisionStorageTx | same bridges, waitChannelAnchorDecision and complete callers | stored status plus original empty verdict default, no terminal classification |
| channeldelivery.ReadFirstSentSourceReceiptStorageTx | same bridges, waitChannelAnchorReceipt and complete callers | source-ID only, sent receipt, ascending settled_at and LIMIT 1; historical witness, deliberately not current-plan/kind/render authority |
| standingdisposition.ReadStandingFlowStorageTx | same bridges, loadServedStandingOwnerByFlow, waitChannelCardMessageID, inbound and mixed-standing callers | exact flow service/current-run/generation tuple, no restart-disposition inference |
| existing original-location read-only selected inspection | one lazy reader on the original parent harness, independent reader for the two standalone fixtures | existing snapshot admission, synchronous read settlement and Close; no schema setup, activation, recovery, pool or transaction transport |

Full source inventory and hostile controls cover direct calls, helper
declarations, aliases and initializers across owned serveapp source, not a
handwritten file exemption. Four SQL interpretations and six raw parameters
are deleted, not hidden behind an alternate raw helper. Card row iteration
and closure move inside the existing selected transaction. Consumer poll
bounds, provider-reference decoding, readiness, temporal cuts and assertions
remain. Failed native and outer snapshot reads return no provisional facts.
Successful receipt absence alone is restored to the exact sql.ErrNoRows
sentinel after successful inspection; transaction/cancellation failure is not.

The parent harness registers cleanup before returning to a child, even if its
reader first opens in that child. Original joined process shutdown precedes
reader close and PostgreSQL service cleanup. No independent background task
or competing close owner is introduced. A real child/sibling/parent journey
proves reuse and final close; exact constructor/hostile controls pin early
registration, including assertion-failure cleanup eligibility.

Sibling inspection distinguished current object-card receipt joins, terminal
render equality, draft state and compound history/fault cuts from the first
historical sent receipt. These remain named parent debt, not falsely migrated
to a broader/less exact observer. Raw diagnostic openers/shared raw-bearing
construction also remain live for their other consumers; no whole-channel,
process-restart or parent failure-class closure is claimed.

## Transform and accounting

Committed finite codemod migrate-channel-anchor/internal/channelanchor plus
the complete output. Manifest cea892c3e4281d02483f29ae522df599d66b886bb94711328d2f1957c1c0ca5e.
Complete before/after declarations preserve all non-carrier arguments and
reject unsupported callers, foreign pools, shadowed helpers, altered arity,
scope, JSON/duplicate/refusal checks and polling/kill/release cuts. Native
query controls compare every original SQL literal byte-for-byte, including
both standing dialects; fixture-only postgres: location tagging is removed
before the unchanged native DSN opener.

Newest exact inverse composes before channel-intent and earlier historical
generators. Old immutable manifests remain unchanged. Corpus1479 ->1494,
783 ->798 transitions: 15 new rows; recipes have ten exact After updates and
one Successor update, while transitions have eleven After updates. No dropped
rows or changed Before, E-reserved entries unchanged. Transition digest
e4f1d21208a12f8f15df724ea8ba914ebd7e2adae0fec05456a994d7fa9ebaa4.

The retirement-text inventory adds only six exact source/field hashes for the
ordinary Go string accumulator in the preserved inbound function across the
two corpora and new preimage manifest. No source edits, blanket path exclusion
or product permission; changed function/field/body/product controls refuse.
An existing reconciliation test omitted the previously approved native event
helper rename; its exact inventory/foreign-name controls now include that
unchanged third path. Neither change modifies the debt collector.

Real normal downward ratchet PASS160.008s:
**6958 findings /4722 raw sites ->6885 /4656; 73 /66 removed, zero added**.
Trusted source26701e7b187579220dffe72e01debd2e89cd2675, collector unchanged
88b0724a6221e16b93c92943cb9090ab9178318f906edb65cc1e02fc2dcd1b66;
all67 excluded-source occurrences retained. Final read-only ratchet PASS82.431s
on the simplified proof/current bridge bodies. No added transfer pair,
baseline increase, manual deletion credit or source reclassification.

## Proof receipts

Go1.26.8/GOMAXPROCS=2/-p=1/-count=1; runtime/native use -race.
Evidence /home/youmew/.cache/swarm-2542-family145/channel-anchor-*.

| Manifestation | Status | Exact execution/source proof |
| --- | --- | --- |
| Real anchors, scan isolation and uncertain copy ownership, JSON references and decision settlement | execution-proven through the same corrected path | pilot-served-race:3 roots/17 passes, both stores,292.158s |
| Human temporal defer/expiry across restart and inbound standing-flow/message prerequisite | execution-proven through the same corrected path | standing-lifetime-race: two unchanged successful roots/12 passes; disclose other new-root failures below, no aggregate green claim |
| Native invalid/raw/cancelled/closed-owner and missing card/receipt/standing refusal | execution-proven through the same corrected path | owner-refusals-race:1 root/3 passes, both stores,11.815s |
| Real parent-owned reader lifetime and standard cancellation/absence distinction; mixed healthy/terminal restart tuple | execution-proven through the same corrected path | lifetime-standing-fixed-race:2 roots/12 passes,48.351s; final-lifetime-race: unchanged assertions after extraction,1 root/9 passes,45.915s |
| ACK crash/replay, diagnostic settlement and onboarding activation publication cut | execution-proven through the same corrected path | ack-diagnostic-crash-race:3 roots/11 passes, both stores,276.516s |
| Complete 54-call/26-body rule, exact SQL predicates, early cleanup, finite corpus/hostiles and historical generator conservation | execution-proven through the same corrected path | corpus-final:21 roots/1525 passes,70.683s; historical intent generator remains original34/41 and03b674c3da5a9d6aa45eaad38dc9005037239a2ff993ac71cce136e4120b2355 |
| Complete four-package candidate typing | execution-proven through the same corrected path | candidate-overlay:1 root PASS187.561s; later proof-helper extraction and outer-error zeroing change no recipe/exports and are compiled/executed by final lifetime/unused proofs |
| Old accounting/source/collector/signature and seven-pair terminal-state controls | execution-proven through the same corrected path | accounting-composition:16 roots/625 passes PASS27.729s |
| All persistence/import/construction/retirement guards plus exact new text controls | execution-proven through the same corrected path | structural-owner-guards-final:82 roots/459 passes, all four packages PASS |
| Physical raw-authority decrease, no additions | reproduced and fixed | real-ratchet-downward and final-ratchet-readonly, exact73 decrease |
| Broader parent construction, atomic snapshots, live faults, historical and integrated holds | split / escalated as separate class | existing #2542/#2151 records; no parent zero-debt or merge qualification claim |

U1000 passes. New proof initially measured cognitive31; assertion extraction
preserves the assertions and puts every new callable below30. Signed
complexity receipt is posted with the checkpoint. No planner/tier workload
changes, timeout increase, retry-until-green, skip or broader tier execution.
Unchanged receipts carry rather than repeating an exhaustive native matrix.

Development reds disclosed: initial finite call count came from measured source,
not the earlier estimate; a missing harness capture/import was repaired; copied
inverse composition accidentally self-recursed and was fixed; new absent-row
test data used an invalid PostgreSQL UUID and wrongly required a custom cancel
cause rather than the existing context.Canceled contract; the standalone
fixture passed its tagged store locator as a DSN, now corrected in the finite
rule. Retirement corpus classification and inherited helper-name inventory
were repaired with exact controls, not exceptions. A preview ran before its
manifest generation finished and failed closed; sequenced generation passed.

Closure: the named six-helper dependency is canonicalized. Parent remains
open at6885 /4656; remaining child tail is the existing mixed observation,
shared setup/lifetime, fault and transaction families, still too heterogeneous
for an honest precise slice count. Watchlist: existing mapping sufficient;
update checkpoint/remaining count, no new issue. Architecture value is read
confinement and one parent-owned observer lifetime, not measured flake or
performance elimination; remaining raw constructors stay explicitly tracked.
No restoration framework or compatibility capability. All SQLite public-fork,
M01/Docker, D3/M07, drain/static-fork/deadline, hosted-history and integrated
holds remain. Exact adc1db8bd history needs package-keyed hosted provisioning
at landing. Preflight remains required before any eventual clean/rebased push;
rebase once after #2617 and orchestration handoff, not now.
