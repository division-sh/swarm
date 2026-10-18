# Cycle attribution: route and artifact plan

DIAGNOSTIC ONLY. Preparation, NOT permission to dispatch. No qualifying rerun.

- Base: f4fb25401114b8aa1bf994bcb97e8fd91c44b77b, including the accepted
  original-cause and failure-site overlays; frozen product base f2e0d031a0.
- Separate non-PR preparation branch: agent-g/diagnostic-2614-deadlock-cycle.
  Neither the prior diagnostic branch nor the PR branch moves.
- Existing isolated workflow only: canonical-plan/build setup, then ONE
  serveapp-runtime batch, Ubuntu-latest, same canonical backends/count/settings,
  setup-Go/private-Postgres/cleanup owners, unchanged 10m/18m job bounds.
- The workflow and selection script are unchanged. Only the identity guard's
  exact diagnostic branch changes. expected_sha must match the signed dispatch
  head; workflow_dispatch and attempt 1 only. No retries/automatic dispatch.
- Canonical unit/batch digest:
  7d8d0b3f137d55449fccf768be67e7f1d0eb36ff14302b4a5809b7a19aabb78c.
  No deadline, workload, failure control, timeout, selection or runner change.

## Required handoff before execution

The parser yields detector edges but cannot map owners. The finite source map's
last section names the exact closed native owner callbacks and lock-statement
hunks needed for correlation. Those hunks are PROPOSED, NOT silently added.
They would add one same-transaction pg_backend_pid() observation and bounded
in-process owner/site bookkeeping, capturing before the original victim rollback.
No SQL text/value, DSN, authority callback or transaction is exported. Unknown
participants remain INCOMPLETE. A post-abort pg_locks query is not proposed.

Reviewer-g must approve that scope/handoff (or the complete native saved-proof
fixture/driver seam) before an execution-ready successor and one-run approval.
Do not spend a hosted run on this parser-only preparation. If the native fixture
route is chosen, preserve genuine session admission and original transaction
boundaries: do not manufacture a similar SQL deadlock with unrelated statements.

## Artifacts on an approved successor

Retain the same exact run/attempt/head/tree/plan/effective-Go identity, untouched
canonical command receipt, all terminal test/package records, environment,
wrapper exit and reported cleanup status on success OR failure. Add this source
map, trace allowlist and cycle offline/API validation receipts to the evidence
artifact and its SHA256SUMS. Original Retry and failure-site capture stay intact.

The Retry node may contain:

- cycle_unparsed, without edges, for any unsupported or partially recognized
  Detail, extra prose, missing participant, disconnected graph or exceeded bound;
- cycle_observed_owners_unmapped with a complete 2-8 edge transactionid graph,
  ShareLock/ExclusiveLock and p1..p8 / x1..x8 observation-local labels only.

No Detail/Where/InternalQuery, SQL/parameters, raw logs, tuple values, native
PIDs/XIDs, QR/session/account material or free-form error text is uploaded. An
owner-correlated successor needs an explicit additional allowlist/control; this
delta does not pretend to emit that evidence. Existing causal INCOMPLETE is not
overridden by a parsed cycle, and a green observation cannot lift the merge hold.

## Cost and stop

Previous observation: 13m47s summed job time; command 383s, package 357.923s.
Planning estimate remains 13-16 Linux runner-minutes for the same route, with one
worker and no new concurrency/capacity. No current dispatch is authorized and
none has been made. Zero new test roots. Offline controls add no runtime/SDK
reproduction. If a successor is approved, one invocation only; return either
complete owner-correlated cycle evidence or the precise INCOMPLETE/negative gap.
