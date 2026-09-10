# Serialized #2440 Integration Checkpoint

Agent-g. This is implementation progress, not a final proof audit or review request.

Historical checkpoint, superseded by the final separate #2321/#2432/#2439 proof
audits. Qualifying HEAD 1f5680f25 (code/test be9e98fe3) passed the full-profile
swarm-test after the T7 cancellation and bounded failure-capture corrections.
Earlier queued/frozen/failed states below remain historical records, not current
blockers. LSF-028/029 remain explicitly observed/unclassified in #2353 under the
lead's nonblocking historical-evidence disposition, not claimed fixed.

## Base And Binding Decision

Rebased agent-g/2321-command-execution onto origin/master
47c0e70d8ef89a2c99f0be4af632f24692121b83, preserving the historical merge graph and
reapplying the reviewed PreparedStartup/reset composition resolution. Backup branch:
agent-g/2321-before-2440-rebase-55e49729e. The separate #2439 commit remains intact.
No #2008 WIP or shared docs worktree was modified.

Binding integration disposition:
https://github.com/division-sh/swarm/issues/2432#issuecomment-5604559542.
Merged B causal fix and receipt policy supersede the old reset freeze; they do not
close #2432 or #2439 by implication.

## Owner Convergence

- Removed AgentOwner.ProjectAgentLifecycleDiagnostics, its facade and independent
  transaction writer. Removed the unused CommitRuntimeLogRecordTx ports.
- Retained B RuntimeLogger -> EventOwner.PersistLifecycleDiagnostic as the only
  diagnostic event/receipt transaction. First-settlement posture and immutable
  producer parent semantics remain B's contract; retry context never supplies run,
  parent or mode. Required typed actor/causal modes remain provenance facts.
- Moved operation/transition/selected-fork historical validation and read-only
  acknowledged observation enumeration into EventOwner. Both fork validators use
  that exact relation; no payload-tag roots or observer descendants are admitted.
- First live projection validates retained provenance. Pending admitted-reset
  history gets the existing standalone historical_cleanup settlement. Acknowledged
  receipts do not require a surviving event/run or recreate one after cleanup.
- Explicit selected-fork discard still removes pending/projected rows with events;
  ordinary selected close retains delayed projection. No extra retry or cleanup
  framework was introduced.
- PostgreSQL receipt JSONB formatting is not canonical event-byte authority.
  Observation validation compares the canonical encoder output after validating the
  receipt facts. The deterministic event identity is B's namespaced outbox-derived
  UUID, not the raw outbox UUID used by the retired G projector.
- Retained complete-set reset fencing and channel publication before execution.
  Preserved upstream bound-listener transfer and B's paired burst iteration split.
  MCP binds port zero in live and compiled-test children; no preselected MCP port.
- B's new retained static-data mock proof uses the existing compiled lifecycle
  harness (H). Public verify remains structural command proof; no false public-live
  or public-fresh-test credit is claimed for the retained mock process.

## Checkpoint Proof

These are local working-tree checkpoints. Final exact-head accounting remains due.

| Proof | Result / retained log |
| --- | --- |
| Standard all lifecycle diagnostics, including B admitted reset/receipt/causal controls | Initial integrated run failed only obsolete G event-ID and receipt expectations. Corrected named fork/observation rows then passed below. Final integrated run remains required. |
| Dual-store ForkLifetime and actual ForkActivationBothStores | PASS 13.299s / 14.816s; /tmp/agent-g-rebase-fork-v2.log. |
| PreparedRuntimeStartup, ResetCandidateSet, topology-before-timers and composition | PASS; /tmp/agent-g-rebase-startup.log. |
| SQLite exit/read/write/bootstrap race selection | PASS 16.971s / 13.910s; /tmp/agent-g-rebase-sqlite.log. |
| Manager diagnostic/terminal/reconfigure and conformance consumer census | PASS 5.865s / 4.991s; /tmp/agent-g-rebase-consumers.log. |
| API specification | PASS 1.873s; /tmp/agent-g-rebase-spec.log. That initial combined command also exposed a stale census location, repaired and executed in the consumer run. |
| Persistence authority inventory | PASS 17.934s; /tmp/agent-g-rebase-inventory-v2.log. Every moved/removed private SQL and prepared-start callback finding classified; no blanket allowlist. |
| New retained static-data invocation shard, SQLite/PostgreSQL | PASS 50.115s; /tmp/agent-g-rebase-release-v2.log. Earlier child rejection exposed the retired required MCP port in the test protocol; removed it rather than selecting a port. |
| Broad diagnostic race count=3 | Four-minute package attempt NOT PASS: cumulative timeout, no reported assertion failure/data race. Reissued through swarm-test with 15-minute allowance, no workload/assertion changes: PASS store 656.324s and catalog/fork 116.361s. /tmp/agent-g-rebase-diagnostic-race-managed.log. |
| Whole full-profile suite | First integration run FAILED in four packages; /tmp/agent-g-rebase-integrated-full.log. CLI fixture failures were repaired in 15dfc47b2. Additional failures and repairs are recorded below; a new full run is required. |
| Second full-profile suite, 5426deb7b | FAILED only TestExecutor_NativeWebSearchCustomProviderUsesRateLimit; all lifecycle/store/release packages passed. /tmp/agent-g-rebase-integrated-full-v2.log. ae8bbb782 repairs the test's downstream-arrival timing oracle, not the admission owner; 20 repetitions and focused race count=3 pass. Fresh full-profile suite queued in /tmp/agent-g-rebase-integrated-full-v3.log. |

## Remaining Acceptance

The first integrated full run exposed two test-only cross-branch contract debts:
SourceInvocationCommandsShareSelectedRoot still authored runtime.execution_posture,
and CopyScenarioSetup restored a node.id deleted upstream. Removed the retired
fixture fields, retained singleton setup/selection semantics and all assertions,
and removed the now-no-op posture rewrite in the pure-system-node release proof.
The three failing CLI tests pass together (15.720s), recorded in
/tmp/agent-g-rebase-cli-fixtures.log. The first full run remains failed, not a green
receipt. The completed run also identified these bounded integration debts:

- The retained mock harness put workspace state in an authored-source directory.
  Move that state under the reserved .swarm directory; preserve invocation-root
  and omitted/dot/alias verification, including missing-data negative assertions.
- The startup source guard named the retired loader. Assert both shared composition
  loading and reset-recovery loading remain after selected-store activation.
- B's causal selected-fork diagnostic fixture supplied only caller lineage. Issue
  and claim the durable fork execution, enqueue the accepted-event origin, then
  quiesce/close before delayed projection and activation. The targeted proof passes
  in /tmp/agent-g-rebase-causal-fixture.log; no payload-tag exception is restored.
- G's provider-state release used container-name deletion. Delegate to upstream's
  existing identity-checked projection cleanup owner, retaining the strict release
  assertion that every removal targets a created immutable Docker ID. No new
  cleanup owner or provider-volume deletion is introduced.
- The diagnostic history reader now uses the same canonical JSON decoder as the
  pending diagnostic reader. Exact fork history proofs pass in
  /tmp/agent-g-rebase-canonical-reader.log.

Final full-profile result, residual disposition and final PR proof comments are
still required. Managed diagnostic race passed as above. After explicit user
authorization, current-head live/restart passed on SQLite and PostgreSQL using
the supported host network (bridge egress failed independently). Twelve successful
fresh replies, no settled-delivery replay. The complete failed/successful attempt
accounting is in issue-2321-postimplementation.md. The old PostgreSQL burst failure's
causal attribution remains unproven; its failure-only event/log evidence retention
is preserved. Later green runs are not described as eliminating that old cause.

Existing #2432/#2439/#2250 watchlist mappings and B's merged refinement remain the
tracking decision. No new issue, gate cycle, compatibility path, table, outbox or
POTENTIAL_ISSUES entry is requested by this bounded integration.
