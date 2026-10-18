# #2720 startup-composition accounting

Settled contract: https://github.com/division-sh/swarm/issues/2720#issuecomment-6242655302
Accepted implementation: https://github.com/division-sh/swarm/issues/2720#issuecomment-6245827935

The only changed census identity is
`internal/serveapp/main.go::buildRuntimeComposition`. Its two calls to
`startServeRunStalledEscalation` no longer pass a runtime-context policy resolver:
the run-level monitor uses the fixed 300-second threshold, not a flow-selected
policy. The constructor's typed reader, publisher, process-work ownership and
execution posture remain the same. No startup admission/refusal, registration,
publication ordering, lease, provider or listener check is added or removed.

The canonical `TestVerifyBootStartupPredicateCensus` refresh changes only this
existing identity's body hash. Its audit rows and proof families are unchanged;
there are no added/deleted census identities or permissions. The exact caller
set remains the initial-start and reset branches of `buildRuntimeComposition`.

Execution evidence is retained under the signed #2720 checkpoints. Focused
revalidation covers `TestMonitorUsesFixedThresholdAcrossScopes`,
`TestMonitorPublishesDomainNeutralDiagnosisWithExactAttribution`, and all
`TestServeRunStalledReader*` controls, including mixed-scope suppression.
The native both-store attribution, coherent-snapshot and waiting-work controls
remain unchanged. The census's hostile uncovered-startup-refusal controls must
also pass without update mode; final exact-head preflight retains their owner.
