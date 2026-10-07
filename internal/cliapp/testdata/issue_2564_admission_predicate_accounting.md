# #2564 Startup Predicate Census Accounting

The independent startup census correctly rejected two added credential-cache
methods and the changed `newRuntime` composition body during final core proof.
The existing generator now records those three bodies. No owner is removed,
reclassified or exempted, and the mutation ratchet remains required.

The inherited A12/P10/P21 and other row numbers identify #2286's admission audit,
not #2564's manifestation/receipt numbering. A body hash detects drift; it does
not supply execution proof. This accounting names the actual owners and proof.

| Audited body | Owner and consumers | Execution witness |
| --- | --- | --- |
| `providerCredentialCache.resolve` | A cache belongs to one existing API runtime and delegates missing values to unchanged `ProviderCredentialResolver.Resolve`. Anthropic, compatible and Responses `continueSession` consume it. Successful resolution is synchronized; failure/cancellation leaves no cached success. Resolver precedence, credentialless profiles and errors remain strict. | `TestProviderCredentialCacheColdConcurrentManagedSessionsAllAPIAdapters`, `TestProviderCredentialCacheFailureCancellationRetryAndSuccessRetention`, `TestProviderCredentialCacheRuntimeAndProviderIsolation`, `TestProviderCredentialResolver_StoreWinsOverEnvForActiveProfiles`, `TestProviderCredentialResolver_EnvOnlyFailsClosed`, `TestProviderCredentialResolverMissingPortIsUnavailableNotMissingSecret` |
| `providerCredentialCache.snapshot` | All three real request writers consume the synchronized snapshot: Anthropic `x-api-key`, compatible `authorization`, Responses `authorization`. The mutex ends before HTTP; no cold-session prewarming or first-turn serialization is added. Verify and retained dependency inspection still consume the original resolver directly, not this execution cache. | The cold managed-session test observes every adapter's actual concurrent requests and headers. `TestProviderCredentialCacheFiniteConsumerGuardRejectsRawHeaderRead` restores each raw header reader and proves rejection; `TestProviderCredentialCacheOptionalProfileDoesNotInventCredential` and `TestProviderCredentialCacheBaseURLConstructorCensus` preserve adjacent contracts. |
| `newRuntime` | Ordinary tool composition supplies the existing pipeline coordinator as `EntityFieldMutations`. Selected-fork composition does the same in `agent_runtime_materialization.go`; tools cannot fall back to the removed raw store writer. Construction adds no new provider admission predicate or credential authority. The existing instance gate, R1 carrier, mutation plan and commit owner remain authoritative. | `TestIssue2564ServedH1ReconstructedEquivalentBothStores`, `TestIssue2564ServedH3CollectionOperationsBothStores`, `TestIssue2564ServedM33NonterminalDeadlineLateResultBothStores`, `TestEntityOperationSurfaceWriterDependencyAndPostCommitResponse`, `TestEntityOperationSurfaceActorRouteAndWriteOwnership` and the raw-writer negative boundary guards. Retained final-head lifecycle proof remains mandatory. |

Approved credential absorption is #2564 comment6001398406; the exact six-site
handoff to E is #2269 comment6001755642. Provider lifetime/cancellation/session
settlement remains E-owned. These census corrections change test evidence only.

Regeneration uses the existing `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`
mechanism. Qualification runs with that variable absent, including
`TestVerifyBootStartupPredicateCensus` and all
`TestAdmissionPredicateRatchetRejects*` actual mutation controls. Do not confuse
generator success with qualification or use a hash refresh to waive a test.

## F01 Native Fence Evidence And A19/A28 Preservation

Core at54b3cd4bb correctly rejected the stale `Backend.runTransactionOutcome`
fingerprint. F01, authorized under #2438 comment6008619373 and implemented at
d52c2cbc7, changes one measured body and adds four measured error-evidence
helpers. The unchanged census measures every function in this audited file,
including helpers that are not themselves startup admission predicates.
The generator records all five; no surface, audit row or proof family is removed
or exempted. A19/A28 and P18/P24 still refer to the original #2286 audit.

| Measured body | Owner/consumers and preserved admission | Execution witness |
| --- | --- | --- |
| `Backend.runTransactionOutcome` | Existing native pooled transaction owner, consumed by named runtime mutations and `schemastore.Postgres.InspectSchema`. Read-only inspection remains caller-cancelled and cannot mint write-conflict retry evidence. Arbitrary callbacks still execute once. Only a mutable, explicitly rolled-back pre-COMMIT error with clean connection disposition may return sealed evidence. | `TestPostgresSerializationConflictRequiresCleanNativeSettlement` includes pooled/retained read-only, cancellation, rollback/cleanup failure, already-settled and uncertain-COMMIT refusal; `TestInspectionNativeReadCancellationJoinsTransactionAndDisposes`, `TestInspectionSnapshotBindsReadersAndRejectsEscapes`, `TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession`. |
| `rolledBackSerializationConflict.Error/Unwrap` | Private native settlement evidence retains the original cause for diagnostics. Error text or an error-tree match does not confer retry authority; independent cleanup/cancellation remains non-retryable. | `TestPostgresSerializationConflictClassificationIsExact`, the native settlement matrix, and `TestNativeRetainedPostgresFenceSettlementRefusesRetry`. |
| `IsRolledBackSerializationConflict` | The existing mutation protocol consumes only the exact sealed value at `AcquireFence`, before domain entry. Wrapped/joined evidence, post-domain failure, uncertain/acknowledged commit and whole fork execution cannot replay. | `TestNativeRetainedPostgresFenceConflictRetriesBeforeDomain`, `TestNativeRetainedPostgresPostFenceConflictsNeverReplay`, and native consumer/settlement controls. |
| `exactSerializationConflict` | Native settlement alone recognizes one exact40001 cause chain; it does not classify joins, other SQLSTATEs or arbitrary strings as proof of rollback. Recognition is necessary but never sufficient for retry. | `TestPostgresSerializationConflictClassificationIsExact` plus native clean/dirty settlement controls. |

Retained-session settlement is accounted for in the F01 proof record; it does
not replace pooled startup inspection. Fresh native admission, fixed historical
cut, unchanged operation identity and original owning cancellation still fence
each protocol attempt. No domain/provider/materialization callback is replayed.
Qualification reruns the census in normal environment, actual omission/restored
refusal controls, read-only startup/inspection and native protocol tests. The
full failed core receipt remains evidence; interrupted units earn no proof credit.

## D1 Runtime Construction And Preserved Admission

The exact68cd1aa01 lifecycle receipt correctly rejects the stale `newRuntime`
fingerprint after the separately approved D1 composition (origin E ed8b59011,
#2564 approval6021549457, rebased implementing commit b7f81ddf0). Only this
measured body changes in the regenerated census; the owner, inherited
A05/A08/A11/A13/A18/A23/A24/A25 and P04/P08/P09/P11/P15/P20/P21 classifications
remain unchanged. This records the actual deletion, not a guard exemption.

`newRuntime` still constructs the normal PipelineCoordinator from its exact typed
ports and supplies it as the tool mutation owner. It no longer injects the
ordinary-final `InstanceDeactivationPreparer`; selected-fork construction removes
the same injection in `agent_runtime_materialization.go`. The deleted preparer
is not source/provider/workspace/receiver admission or explicit termination
authority. Their existing owners, validation and cleanup remain. Ordinary
handler/timer final entry cannot acquire operational retirement through another
injection or callback, and explicit termination retains its separate command.

| Consumer / preserved obligation | Actual execution witness |
| --- | --- |
| Normal constructor ports, process grant and current dependency graph | `TestRuntimeRequiresProcessGenerationGrant`, `TestRuntimeDepsValidateOwnsRequiredBootInputs`, `TestRuntimeDepsValidatedDerivesCanonicalBootGraph`, `TestNewRuntimeValidatesInboundPublicationIntegrityBeforeWiringGateway`, `TestRuntimeCorePersistenceRolesAreConstructorInputs`, `TestServeCompositionProvidesExactRuntimePorts`. |
| Normal real node/agent/timer execution without final-entry retirement | Original unchanged four-cell H2 PASS256.347s; deterministic `TestIssue2564H2FinalEntryCompletionOrderingBothStores` and actual M33 late provider/save/result proof PASS190.465s at race-three. Exact source-bound receipts are in the #2564 D1 composition record; these are not final-tier credit. |
| Selected construction, accepted work and recovery refusals | Complete `catalog-runtime-fork-readiness` initial/staged unit passes238 entries on68cd locally and again149.018s under server2 lifecycle load. Connected causal/identity/provider/future-oracle preservation passes168 entries at race-three; unsupported staged work still refuses without mutation. |
| Ordinary publication/settlement and retained explicit cleanup | Complete `catalog-runtime-scatter-safety` passes all seven variants/both stores, including exact cause receipts, original100-item reverse completion and duplicate/held no-op assertions. Explicit middle-member/suffix/panic/cancel and provider/directive retirement controls remain required and pass connected race-three; no ordinary retirement is restored. |
| Drift detection / fail-closed caller preservation | `TestVerifyBootStartupPredicateCensus` runs with update mode absent; all three `TestAdmissionPredicateRatchetRejects*` controls restore an unaccounted startup refusal, remove a verify consumer or change provider prerequisites and must still reject. No scanner, expected surface, classification, workload or deadline changes. |

The68cd lifecycle failure and interrupted/unstarted units remain unqualified.
Generator success is not execution proof. Final lifecycle plus the four existing
supplements and hosted full still qualify the next committed head.
