# SQLite channel-source fork deadline: causal diagnosis in progress

Track: FULL. Ruling6167104335 remains an unwaived supported-path qualification
hold. This diagnostic work is interleaved with the selected served read-role
batch, not deferred until landing. No timeout, retry, assertion or production
runtime change is proposed or made here.

At the 3a4a08d41 checkpoint, the original affected source file has SHA256
514090ae89c40da79cefbdfc8d2c096f58dc0db26a20c338118ad15f379e6081.
The uninstrumented exact SQLite/fork cell fails under race/GOMAXPROCS2, retaining
the five-second public HTTP request deadline. CPU, block and mutex profiles were
captured on that single discriminating run, not a retry-to-green loop.

The attached temporary probe observes the actual selected store's existing
transaction collector with default options (NO injected delay). It records
request-entry/three-second/return snapshots and one live three-second goroutine
dump. It wraps the existing public fork executor, forwarding its request/context
and result unchanged; the original test/journey and deadline still run.

| Named hypothesis / condition | Observation |
| --- | --- |
| Selected settlement waiter is necessary for the failure | Counterexample: a Go overlay restores ONLY the first pre-fork call to the retained original raw waiter. The same journey still fails; executor duration5.14339539s versus selected-waiter5.00994127s. This is NOT a clean-master comparison or a general no-regression claim. |
| Leaked read transaction stalls the writer throughout fork | The observed writer makes progress: selected-waiter entry438 begins/211 read commits/225 write commits; at3s582/287/293; end684/342/340. The original-waiter counterfactual progresses428/203/223 ->575/283/290 ->682/338/342. This does not prove absence of every shorter lock wait. |
| Same injected-delay family as D12 | Not established: all observed InjectedDelay, DelayedCommits and DelayDuration counters are ZERO. No delay-fault fixture is installed by this probe. Source/workload/commit amplification may still be related, requiring comparison with D12's evidence rather than assuming equivalence. |
| Exact active fork path at3s | Live stack is in modernc SQLite COMMIT, selected backend runTransactionOnceOutcome -> mutationprotocol -> PipelineSQLiteOwner.AdvanceFlowAttachment -> completeSelectedContractFlowRoutes -> startSelectedContractAgentRuntime -> selectedContractForkLocalRuntimeContainer.Publish. No retained channel-inspection callback is on that captured request stack. |
| Cost split signal, not final causal accounting | Whole-test CPU profile includes2.96s CPU under ExecuteSelectedContractRunFork, including1.22s Prepare,1.15s Publish and0.97s source load. Block profile attributes0.70s aggregate fork-path wait to SQLite writer admission. These overlap/cross goroutines and are NOT wall-time components to add together. |

Evidence: /home/youmew/.cache/swarm-2542-family145/.

- fork-diagnosis-candidate.jsonl, RED; SHA25651187cc9a8932bf6639b736ecd9df6175b175919bb7cc8a194157c16b361f579.
- fork-diagnosis-live-owner.jsonl, RED; SHA256b94e70917ad09b7eb76650f6183268e0281df659e7cf04498f55af9c15f7838a.
- fork-diagnosis-original-waiter.jsonl, counterfactual RED; SHA2560ec61dc909a3ffd93f905ec2b73811f20a1b4385d668e9b122718710b992e6ba.
- fork-live-1791979064419049347.stacks, three-second selected-waiter stack;
  SHA256c1418e893b6fe01511e0dbbcc4687861985ee07c0d4c7ac59a2f8c6cb8cc75a5.
- fork-diagnosis-candidate.cpu/.block/.mutex; pprof attribution above.

Current disposition: failure remains unclassified/qualification-open. The
counterfactual narrows away necessity of the new settlement waiter, and the live
evidence points to genuine source-load/attachment publication work plus concurrent
writer traffic, not a proven deadlock or injected commit-delay cause. A production
optimization of that path would need its actual ownership/scope decision; this
test-only read-role migration does not silently change it. Continue the disjoint
read-role batch while reviewer-b/C compare the named publication/commit evidence
with D12. No claim of a repaired fork, inherited flake or green qualification.

## One historical-master comparison, ruling6168582525

Executed ONCE at26701e7b187579220dffe72e01debd2e89cd2675, a separate detached
diagnostic worktree with ONLY the identical temporary probe added. The original
journey, production code and five-second HTTP deadline were not replaced. Probe
SHA256561bfe49b520245f6cb0e6b0c00bede3b90d1cdb5197500b3abc16fe72f4460c
matches the candidate's saved probe byte-for-byte. Historical journey file
SHA25604d2f29fe96d44f2ca5ba5001c09232ddd6ca57d4d03a2e53689923e8c860282.

Command from the historical worktree:

```sh
env GOFLAGS= GOTOOLCHAIN=go1.26.8 GOMAXPROCS=2 TMPDIR=/home/youmew/tb4 \
  go test -p=1 -race -json ./internal/serveapp \
  -run '^TestChannelSourceForkDeadlineDiagnosticProbe$/^default_sqlite$/^fork$' \
  -count=1
```

The JSON proves exactly the requested default_sqlite/fork cell executed and
failed24.01s, not a skipped/zero-case pass. It reached the original run.fork HTTP
request and timed out awaiting headers, with executor elapsed5.020780225s.
Entry counters404begins/183read commits/219write commits progress through
551/263/286 at3s to657/317/338 on return; the same two earlier failed/rolled-back
transactions remain two throughout, not newly failed commits. Every injected
delay counter remains zero. The3s request stack again reaches
AdvanceFlowAttachment/completeSelectedContractFlowRoutes/container.Publish.
That single sample does not establish a prolonged COMMIT or the D12 cause.

Predeclared classification: this exact deadline failure EXISTS at historical
master BEFORE the unmerged B stack. It is not evidence of zero branch-added
cost, today's-master behavior, a timing flake, a fixed path, or permission to
raise the deadline. Reviewer-b owns the final classification/disposition; the
supported-path landing hold remains explicit. No additional comparison is run.

Superseding disposition6168843088: reviewer-b independently verified this
receipt and classifies the signature as a pre-existing, historical-baseline-
reproduced deadline residual. The EXTRA #2542-specific repair/probe hold is
REMOVED; ordinary exact-head CI remains required and is not waived. The residual
is tracked in #2535, handoff6168836466, with reviewer-b retaining classification
and reviewer-g asked to triage. No further fork probe/runtime change is required
for this batch. It is not a fixed/execution-proven path or a proven #2394/D12
single-writer cause. Earlier open/unclassified text above records chronology,
not the current disposition.

Concurrent context: preparation overlapped the combined batch ratchet and the
reporter run, both finished by12:47:11Z. The measured baseline run started
12:50:09Z, with only the finite source-control job still live locally; no new
heavy census was added during its fork interval. Host load before was
15.54/14.12/10.40; this was NOT a reserved quiet machine or a quantitatively
load-matched A/B, so do not infer relative speed/cost or pressure causation.

Retained evidence:

- fork-baseline-26701e7b1.jsonl SHA256270f472b7bb3be5f8a2b30b01fe738492cbe31e85c31677f03f9361da35724b3.
- fork-live-1791982227602440909.stacks SHA25635782e0451e5fc52ad3dbd1cd07c311571912c8edd6a35913b4c7881b15b8f47.
- fork-baseline-host-before.txt SHA256cf37fe703880d5d6b0a425cbed62ec2ffdea711f0d63faab4cf6aa7a42c80a9b.
- fork-baseline-processes-before.txt SHA2561c0b52e34bcff46f3dc2e04af51505ef87e972228f736dbc2520027d1db5e572.
