# Embedded-Spec Plumbing Removal: Startup Predicate Accounting

Authority: #1801 user ruling 6197217312 and reviewer-g gate 6200650148.
Comparison base: `53788da1332cbecaf805b13722316257d7ac6829`.
The authoritative contract already says `swarm` always uses its embedded spec:
`cli_specification.foundations.source_platform_spec_path_resolution`.
This is removal of the internal selector, not a new admission predicate.

## Owners And Consumers

| Changed startup owner / callers | Preserved boundary and execution evidence |
| --- | --- |
| `runVerifyCommandWithOutput`, `inspectVerifyDeployment`, selected-store inspection/recovery helpers | Source/config admission still runs before source construction and selected-store observation. `CLISourcePaths` carries only the admitted source root. The terminal module/retained loaders materialize the embedded spec; no implicit repository-spec fallback. `TestResolveCLISourcePlatformSpecPathsRejectsConfigOverrideBeforeSourceLoad`, `TestPlatformSpecFlagRetiredAcrossCommandSurface`, `TestVerifyRetainedSourceConsumerSharesFrozenConfiguredPackGeneration`. |
| `loadLocalPreflightSource`, `RunServeLocalClaudeCLIPreflight` | Same selected source and configured pack generation; local preflight cannot select another spec. `TestSourceInvocationCommandsShareSelectedRoot`, `TestRunServeRuntimeConsumesLocalClaudePreflightAfterBundleDecision`. |
| Serve `Run` / `buildRuntimeComposition`, loading/materialization and runtime-context publication | Source-root and config selection, platform schema bootstrap, startup ownership, source availability and publication checks retain their order and refusal branches. The forwarding-only `servePreCatalogPlatformSpecPath` disappears; embedded materialization remains at that existing pre-catalog phase. Per-bundle spec-path fields and an unused runtime-context path field disappear. `TestLoadServeRuntimeBundleRejectsMalformedPackBodiesBeforePublication`, `TestPrivateTestAdmissionPrecedesResourceAcquisition`, `TestPrivateTestStartupFailureUnwindsAcquiredResources`, `TestServeBootLegacySchemaRendersTeachingError`. |
| Serve retained artifact/budget/reset recovery and `SourceArtifactSelectedContractSourceLoader` | Exact artifact/hash, selected base generation, compatibility, source-run/target checks and recovery/fork guards are unchanged. Each terminal loader unconditionally uses embedded materialization. `TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores`, `TestPackPlatformRetainedSourceExecutesSelectedForkBothStores`, and the existing selected-loader missing/corrupt/unavailable/crossed-target negatives. |
| Public API handler construction | Default registry comes from embedded materialization, not an option or caller cwd. Existing admitted registry/parser fixture seams remain below command construction. `TestHandlerUsesEmbeddedRegistryWithoutPathSelection` and the public API package's existing registry controls. |

The A/P rows already assigned to these whole-file surfaces remain assigned;
none is removed, declared proved by a hash, or narrowed to a check registry.
The census refresh records the changed signatures/callers and the deleted
forwarder, not a new permission, lifecycle, backend or transaction policy.
Unchanged backend predicates and their existing proofs carry unchanged.

The exact changed census consumers are:

- CLI: `RunServeLocalClaudeCLIPreflight`, `loadLocalPreflightSource`,
  `inspectVerifyDeployment`, `inspectVerifyRecoverySnapshot`,
  `inspectVerifySelectedStore`, `prepareVerifySelectedStoreInspection`,
  `verifyRetainedSourceLoader`, `runVerifyCommandWithOutput`.
- Serve: `Run`, `buildRuntimeComposition`, `buildServeRuntimeBundleContext`,
  `initializeServePlatformStateStores`, `loadBudgetRecoveryStageSource`,
  `loadServePlatformSpecDocument`, `loadServeRuntimeBundle`,
  `loadServeRuntimeBundleFromArtifact`, `loadServeRuntimeBundles`,
  `materializeRuntimeBundle`, `plannedServeRuntimeContexts`,
  `prepareServeRuntimeContextSet`, `serveConfigLoadDetail`.

The only removed census row is `servePreCatalogPlatformSpecPath`, a constant-path
forwarder with no independent predicate. Every retained consumer keeps its
existing A/P classification. `TestVerifyBootStartupPredicateCensus` and all three
`TestAdmissionPredicateRatchetRejects*` negative controls qualify that refresh.

## Test-Seam And Failure-Cut Accounting

- Runtime option structs and module constructors no longer accept a spec path.
  `TestRuntimeOptionsDoNotSelectPlatformSpec` checks the public/internal carriers;
  the unrelated-cwd module and handler tests prove hostile adjacent files cannot
  change the embedded authority. CLI source/root/config errors stay fail-closed.
- A deliberately missing-spec startup fixture now fails the existing projected
  schema bootstrap interface instead. It still asserts the exact injected failure,
  no ready callback, joined store/workspace cleanup and removal of the private root.
  No production fault hook, global/env selector or alternate-spec option is added.
- The selected-loader compatibility negative changes frozen source requirements to
  exclude the actual embedded version and still requires the integrity error and
  exact compatibility cause. The original alternate-version compiler refusal is
  separately retained at `LoadWorkflowContractBundleFromArtifact`, the explicit
  lower-level loader (`TestRetainedSourceCompilerExplicitSpecRemainsTestSeam`).
- The test-local admitted-source fixture uses the same embedded materialization as
  its production retained reader. Both-store source bytes, provenance, channel
  plans and inventory digests must remain exactly equal; none of those assertions
  is weakened to ignore the former repository-versus-embedded location mismatch.
- Generator `swarm-openrpc-gen --platform-spec` remains explicit and unchanged.
  No platform-spec, OpenRPC schema, generation algorithm, deadline or debt baseline
  is changed by this deletion.
- The compiled release test relocates the binary and adds hostile spec files at
  its original directory, its new directory and the project. Pack inventory stays
  exactly equal; compiled portable verify and describe still succeed. The serve
  preflight-ordering test starts from an unrelated cwd containing a hostile spec,
  and still reaches the same precise missing-credential refusal before store
  selection (`TestRunServeRuntimeConsumesLocalClaudePreflightAfterBundleDecision`).

Execution receipts and any remaining test-seam disposition belong in the PR audit;
this accounting file alone is not a passing supported-surface receipt.
