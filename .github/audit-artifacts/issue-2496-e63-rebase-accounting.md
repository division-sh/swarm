# Local Rebase Accounting: #2496 / #2525

Preserved pre-rebase batch4809c5cb8 on
`agent-e/2496-before-e63-rebase-4809c5cb8`. Local branch
`agent-e/2496-current-proof-ledger` rebases113 commits onto
`e63f4bdb197e473fe35e94f65ff3c77655957123`, producing
`2abec8556117d30c61f892e23ebb213fe8d6b74f`. No GitHub push.

Only conflicts: two generated complexity snapshots and one historical describe
baseline. Snapshots were regenerated against each exact staged source; the
describe resolution preserved all45 fixture/surface keys, normalization and
historical hashes pending a fresh capture. No production conflict. Range-diff
differences are confined to five inventory/baseline bookkeeping commits.
E's manager, selected-readiness, tools and pipeline production files remain
byte-identical to4809. Upstream spec additions and scenario test are retained.
Merge-base equals e63; `git diff --check origin/master..HEAD` passes.

Exact2abec complexity collector and ratchet PASS versus e63. Cognitive counts
at least30/50:573/193 ->570/193, maximum302 unchanged. Cyclomatic counts:
264/53 ->260/55, maximum185 unchanged. Policy is unchanged; no individual
threshold decrease is inferred from the aggregate ratchet. Raw exact snapshots:
`/tmp/agent-e-2496-2abec8556-complexity/{head,delta}.json`.

Fresh45-cell compiled describe on2abec is RED90.049s on ten JSON hashes.
SHA-verified structured comparison classifies every difference as merged spec
source digest/mapped line provenance. All semantic fields and provenance
membership remain exact;35 hashes unchanged. Generated detailed report:
`issue-2496-describe-e63-rebase-provenance.json`. Only the ten hashes and
baseline metadata are updated. Acceptance was pending at this capture's cut.

Accepted clean-head source`ccaab9ce3a4496f47dabd80f2733c5f47fe50562` then
PASSes all45 cells, two internal repetitions/cell, package98.577s through
`go run ./cmd/swarm-test`. No failures/skips. Exact capture and SHA are in
`issue-2496-describe-e63-rebased-accepted.json`; the preceding RED is retained.
Four small identity/retirement roots also PASS race/count3:12 root passes and
30 cell passes, no fail/skip (e63-rebased-owner-guards.json). These are focused
controls, not full managed qualification or closure of unimplemented C22.

C22 projection/receiver probe remains RED race/count3,3.290s on2abec; see the
additive issue audit5968864157. Test-only handler-contention disposition remains
requested under #2353 comment5969125613. Neither owner has been edited pending
its independent ruling. Earlier failed qualifications remain failed; the
rebase/metadata classification is not full-suite or class-closure evidence.
