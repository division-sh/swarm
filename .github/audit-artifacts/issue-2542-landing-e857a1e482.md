## #2542: ONE exact-head incremental landing request

**Track: FULL. Candidate `e857a1e482a2e08ab63f2e8169b4472e608cfdc2`, tree
`e42c9b83736ac0b59a2a3bbc2366b03e78d68ba1`, all four commits GOOD signed.**
Based on master335839f7f1; clean including nonignored untracked source. This is
an incremental landing, NOT #2542/#2151 closure or zero-debt completion.

### Source, authority and reviewed carry

User a1 [6203866523](https://github.com/division-sh/swarm/issues/2542#issuecomment-6203866523),
contracts [6203773156](https://github.com/division-sh/swarm/issues/2542#issuecomment-6203773156)
and [G6203881861](https://github.com/division-sh/swarm/issues/2542#issuecomment-6203881861),
finalized by [6203901856](https://github.com/division-sh/swarm/issues/2542#issuecomment-6203901856).
The exact4,813-key active list matches signed4a7e8b8f66 and SHA256
`5f5c9cf2ea361070e0bf3dba00f2029bf9205d7befcfe5ba793bf00fafc823cf`.
No existing registry row, role classifier, debt baseline, collector or history
judgment was changed. Later reviewed B migrations can only shrink against
origin AND trusted previous master; no additions, resurrection or deletion
offsets. The same owner contract remains executable on merged master/nightlies;
no expiry/master RED or implied E/S6 waiver. Strict completion remains FALSE.

The original9,520 strict diagnostic occurrences, including reason multiplicity,
match the pre-disposition MULTISET byte-for-byte: SHA256
`4707389ca76448bdd3935f01cee45c7dd6b47ab53a8c39cb91c77781740f1512`.
Unknown/stale/new keys, duplicates, compound violations, other invalid roles and
scan errors still fail. Normal runner/attestation/required-root semantics stay
unchanged; only this finite existing-owner completion disposition is admitted.

The release-only rebase is recorded at [6204052096](https://github.com/division-sh/swarm/issues/2542#issuecomment-6204052096).
Backup was pushed BEFORE rebasing bf667b0a0f: `refs/backup/agent-b/2542/20261015-before-release-rebase-a1`.
All four range-diff entries are `=` (36373->719d,12c889->b407,4a7->8e9b,bf667->e857).
Only release.yml differs between old/new candidate trees; no claim of identical
trees. Signed source is uploaded at `refs/backup/agent-b/2542/20261015-registry-a1-landing`.
The immutable4a7 source object stays protected at its existing CapturedReceipt
backup ref while the retained contract/history manifest requires it; do not
delete that required proof source as temporary landing cleanup.

Prior owner/native/both-store/race/process/history receipts carry under6197518496,
6198598314,6200267441,6200510991 and6201297415. The CapturedReceipt repair is
accepted6203403324; its seven-method blocker is FIXED, not waived. Exact whole
family/caller/predicate/source maps remain in the signed artifacts, especially
`issue-2542-catch-up-native-entry.md`, `issue-2542-captured-receipt-6203063953.*`
and `issue-2542-registry-a1.*`. No E capacity source or new migration wave.

### Qualification

| Proof | Actual result |
| --- | --- |
| Finite admission / original hostile protections |11 owner/retained/effective/raw/compound roots PASS12.258s; exact-head rebased retained controls also PASS. |
| History declaration / shallow exact fetch |4 roots PASS0.484s; exact4a7 object declared without removing any prior input. |
| Unrelated failure / foreign or incomplete receipt |4 existing schedule/status roots PASS1.984s; unrelated process failure still blocks and no imported success can attest. |
| Strict diagnostic preservation |All9,520 occurrences retained exactly; original RED archives and failed canonical receipt untouched. |
| CapturedReceipt/native/process migration cuts |Accepted named both-store/race/COMMIT-cut, original-selected-writer and Docker/descendant receipts carry. No duplicate local tier or speculative full rerun. |
| Fresh canonical preflight |**PASS580.935s**, exit0; all25 phases passed, complete=true, no unstarted/failed phases. Owner-published preflight/local success56479717919 at23:10:26Z, matching pending56479113158 and attempt93dc0a85-ac2e-4e48-9fe2-7b465355156a. |
| Real ratchet / native / effective method |Fresh canonical PASS110.70s /20.30s /4.28s; registry revised-contract PASS4.32s, live source oracle PASS0.97s. Actual debt3806/raw2152, unchanged. |
| Conflict |Candidate contains current335839; merge-tree exits0 and equals its exact treee42c9b83736ac0b59a2a3bbc2366b03e78d68ba1. |

The revised registry root's PASS is an explicitly authorized incremental
contract PASS, with `strict_completion=false`, full original strict output,
retained digest, user/G provenance and trusted predecessor captured in the
normal canonical package log/receipt. It is NOT registry completion or an
edited failure/status. Every formerly unstarted phase must execute normally.
They now DID execute: runtimepersistence22.197s, planner10.511s, codemod14.106s
and full vet162.847s, all PASS. Preflight exceeded the soft five-minute target;
no timeout, budget, workload or selector was changed. Outer launcher-to-exit
measurement585.11s is retained separately, not substituted for command latency.

Canonical receipt:
`test-results/preflight/20261015T230044.614169383-93dc0a85-ac2e-4e48-9fe2-7b465355156a/receipt.json`,
SHA256`a6968d1ff73be81690368276f3156aba87e2b4fa5177b933d937e42f559d7432`.
Head=e857a1e482; base/origin-master/native-source=335839f7f1. Selection
`6f658b5e57d27adc7a2e4de8c61bdbc228ca62040658bf55ee76bb8be3551ee5`,
comparison diff`4a7140ae6d7bf2927018ff81acfc6eaab8bddcb0e8e9ffddc1758751ecd160c3`.
The normal owner created and published this receipt; the earlier RED remains
unchanged, and independent tail passes were NOT used as its completion data.

### Separate open items and ledger

| Item | Explicit disposition |
| --- | --- |
| S1/S2 | Original REDs and inconclusive causal attribution remain. User6202436617/reviewer6202457522 accept that uncertainty for THIS landing only; #2666 A/reviewer-c follow-up retained. No inherited/PASS claim or fresh-RED waiver. |
| **Q1** | **OPEN, separately tracked, not absorbed or waived; not a blocker for this incremental landing under its existing disposition.** No repeated unmatched-load pair or new closure claim. |
| **194 findings /155 raw holds** | **OPEN and UNcredited, separate from Q1 and S1/S2; not landing blockers.** Their original exact ledger/dispositions remain; no credit through integration or admission. |
| Parent migration |Physical3806/2152; qualification-adjusted4117/2376; cumulative accepted2516/2046 unchanged. No new removal credit, zero-debt claim or parent closure. |

Please pin the implementation/carry verdict to **e857a1e482** and confirm landing
instructions/tier metadata. Proposed normal PR against master:
`[agent-b][issue #2542] Integrate selected-store authority migration`, `Part of #2542`,
hosted FULL / local FOCUSED named receipts plus canonical preflight, no duplicate
local tier. PR body and mandatory PR proof-audit drafts are ready. No PR or
hosted dispatch yet; orchestrator selects the quiet window. Every required
hosted gate and final exact-head approval remains mandatory; scheduling targets
are not a waiver. One signed evidence-only backup packet preserves full logs
without changing the qualified candidate or transferring its status.

Evidence backup: `refs/backup/agent-b/2542/20261015-landing-e857a1e482`.
It retains the complete new canonical directory/log/status history, exact
strict-multiset comparison, range-diff and prior12c evidence bundle. Archive
SHA256`0717c977835a5cc2b7ad6100102fcfe779b92549cb6ed7ea083f092a5dbbe383`.
The evidence commit is NOT the proposed PR/qualification head.
