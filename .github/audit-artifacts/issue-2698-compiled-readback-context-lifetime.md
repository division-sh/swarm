# Compiled Readback Context Lifetime

Track: FULL, consolidated2698/6205327685 decision4. The complete original
TestReadProofFactoringCompiledSurfaces reached terminal workload completion on
SQLite, then entered conversation.list with its STARTUP context already expired:
rpc_budget_at_entry=-6.27704673s, rpc_elapsed=25.91us. This was not a failure of
the list query or permission to inflate a timing limit.

Readiness retains its original goldenStartupTimeout context and cancellation.
The run still uses its original goldenRunDeadline. Only after terminal completion
does the readback phase create its own bounded context, using the same existing
goldenStartupTimeout value. No timeout constant, polling, retry, assertion,
workload, backend cell, schema parameter or compiled command was changed.

Receipts in /home/youmew/.cache/swarm-2542-family145/:

| Receipt | Result |
| --- | --- |
| pr2698-release-admission-surface-ledger.jsonl | Original aggregate remains RED. Compiled read root RED133.16s at expired first SQLite read; admission factoring and selected-root boundary independently PASS16.74s and99.44s. Schema-relation root remains separately RED, not credited by this fix. |
| pr2698-compiled-readback-owned-context.log | Complete root PASS133.398s, 71 root/child records, both stores, public RPC inventory and compiled plain/JSON/quiet read surfaces. |
| pr2698-external-source-and-read-context-vet.log | Both affected packages vet PASS. |

Separate guard, source-history composition, Docker smoke, teardown and remaining
failed-root items remain open. This is not final-head preflight or hosted proof.
