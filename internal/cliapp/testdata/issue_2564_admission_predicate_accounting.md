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
