# #2542 continuation: selected construction authority argument

The existing PostgreSQL selected-contract execution constructor accepted an
unused sql.DB alongside its actual selected native store. Its shared harness
caller propagated the raw handle even though every dependency, optional lifecycle
collaborator and joined cleanup already consumed the selected semantic owners.
Delete that parameter and its sole forwarding argument. No constructor, delegate,
fallback, lifecycle policy or selected store is added or reconstructed.

Both complete finite inverses permit only the parameter/argument deletion. They
retain original durable dependencies, manager roles, opaque workflow persistence,
PipelineObligations, error refusal and RetireSelectedContexts cleanup. The harness
keeps reuse of its exact previously bound owner and its unchanged SQLite sibling.

Focused real proof is recovered valid activation and committed submission failure
under race, both stores. These exercise the shared constructor and optional
lifecycle collaborator while preserving activation/result/final-state evidence.
The whole-owner and harness codemod/type controls protect every retained field
and cleanup statement. Original generic catalog construction and other raw
Tier12 helpers remain tracked #2542/#2151 debt; no complete construction or parent
closure claim. The existing gate/watchlist and raw_sql_policy suffice, with no
new architecture, framework, compatibility or runtime-spec change.

Counts and final receipts belong to the increment issue record. No aggregate
core/full, hosted CI, server2, fork-deadline or parent qualification is claimed.
