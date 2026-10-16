# #2698 measured partition qualification

Track: FULL. LEAD6220978797 authorizes the bounded partition; reviewer-b
6221326027 accepts G's signed donor87b8d58c469007e800936587373a93ccb54b0704.
B composed it with attribution at577ad7d00f2b9a3040ab0f83c4007224eafd0337.
Both commits have identical treef74e684e43263062568865c316b479f1d61e702d.
No assertion, accounting, source-policy, allowance or runtime change is in
the donor. Only the approved partition/inventory/spec-owner changes apply.

## One attempt per affected unit

All seven real units ran ONCE, sequentially, on that clean unchanged signed
head: Go1.26.8/linux-amd64, GOMAXPROCS2, count1, p1, original600s Go timeout,
no overlay, no retry, no concurrent B test. Each has ordinary240s baseline
and312s buffered ceiling, without an override. Source/tree and clean status
were checked before and after every command; raw JSON, stderr, load/flags/
timestamps and outer time are retained per unit.

| Unit | Store roots PASS | Go JSON package seconds | Launcher seconds |
| --- | ---: | ---: | ---: |
| store-admission-full | 96 | 100.348 +39.766 testpostgres | 160.17 |
| store-admission-debt-01 | 18 | 213.294 | 231.19 |
| store-admission-debt-02 | 18 | 247.804 | 288.92 |
| store-admission-debt-03 | 19 | 246.452 | 287.43 |
| store-admission-debt-04 | 18 | 210.067 | 221.81 |
| store-admission-debt-05 | 40 | 197.291 | 211.46 |
| store-admission-debt-06 | 20 | 203.621 | 224.42 |

All229 store roots PASS exactly once, no FAIL/SKIP/missing/duplicate root.
testpostgres remains wholly in base:122 PASS/2 unchanged auxiliary SKIP,
0 FAIL. The separate two-root census/16 required children/20m exception is
untouched. Every core/lifecycle/full, backend/whole-child, environment and
count obligation remains the accepted donor's exact conserved envelope.

Every outer launcher is below312s even INCLUDING precompile, queue, owned
service setup and joined cleanup. Debt02/03 exceed240s and retain that visible
baseline warning; no quieter rerun or timing adjustment is claimed. Debt03
queued25s; all queue/precompile diagnostics remain in stderr. Package elapsed
and launcher time are reported separately, not substituted for hosted primary
command evidence. No pressure/inheritance attribution is made.

Sum of Go JSON package elapsed1458.643s; outer launcher sum1625.40s. These are
fresh local observations, NOT billed runner-minutes, hosted qualification or
new measured scheduling weights. The first exact-head FULL must report all
seven primary commands and actual runner-job intervals/setup against master's
recorded183s command weight (run38871894148 attempt1). Obtain the old actual
job interval if available; otherwise disclose that the weight is not matched
runner billing. Fixed runner/minute/concurrency capacity is unchanged.

Exact measured rows/checksums:issue-2698-partition-measured-results.tsv,
SHA2567288a937faf17b5dddc3820a8b8136883aaf9af7d13dee24db62224631a25cd6.
Exact229-root actual conservation:issue-2698-partition-measured-root-conservation.tsv,
SHA25657cbe0d8d9a43c8aead867e2d0298c75141e15ea8a8b3404fad99de5fee32b27.
Cache prefix per unit is pr2698-partition-577ad7d00f2b9a3040ab0f83c4007224eafd0337-UNIT,
with .jsonl/.stderr/.meta/.launcher-time. Original whole-unit600s REDs stay
retained; neither their unfinished roots nor the85-root diagnostic is called
complete qualification. This partition supplies the previously missing union.

## Integrated guards and freeze

After the seven units, affected focused guards PASS with0 FAIL/SKIP:
testplanning7 roots/32.160s; testtiming4/2.895s; testcatalog3/1.121s;
CLI plan/selection/evidence/budget18/45.957s. JSON hashes respectively:
d3eb3dc56a6b2ba6b0f7f66711560724b0ec786a0d846a2e3df1fda37caaed67
and1ebe629239997b50566b57f537fa7efe0db6eb72fb596ad22a1e2c7d8937eed2.
This includes bound old/new root/child/profile conservation, native-selector
children, prospective unassigned-root refusal, hostile drift, ordinary budget
isolation and actual current plan binding. Unused analysis on the changed
planner/timing/catalog packages PASS; exact-snapshot complexity PASS.

The410-root tool PASS308.542s remains unchanged-input carry. Tool source,
imports/helpers, recipe/history manifests, pinned source objects and its
selection are untouched by the donor; no tool test reads the changed proof
policy. No repeated whole tooling run or duplicate local tier is claimed.

This evidence-only update does not change any tested source or selector.
Fresh canonical preflight and frozen-head local HumanTask requester plus
producer both-store receipts must be published at the final exact head before
the single qualifying push. They are NOT claimed by this pre-freeze record.
Hosted FULL remains required; all five USER9 roots, Q1 requester BOTH stores
and H2/#2715 postgres/paced must actually execute and PASS under their existing
dispositions. The194/155 holds remain separate/open/uncredited. No parent
completion or new permission is claimed. Donor allocation posts at FULL start.
