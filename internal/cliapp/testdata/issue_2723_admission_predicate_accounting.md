# #2723 Startup Predicate Census Accounting

Exact-head preflight at `f3cb9f8743` correctly rejected the changed
`internal/serveapp/main.go::buildRuntimeComposition` body. The only production
change in that body selects the public listener's exact-context inbound
handler instead of wrapping every route in the managed-registration-only
handler. This composition is accepted in #2723 comments
[6245006678](https://github.com/division-sh/swarm/issues/2723#issuecomment-6245006678)
and
[6248525264](https://github.com/division-sh/swarm/issues/2723#issuecomment-6248525264).

The existing generator refreshes exactly this body's current fingerprint.
No owner, inherited audit row, proof family, refusal, surface or mutation
control is added, removed, reclassified or exempted. The A/P identifiers
remain #2286's startup audit, not #2723's W01-W08 proof numbering. Fingerprint
agreement detects source drift; it is not behavioral qualification.

| Owner / consumer | Preserved boundary | Execution proof |
| --- | --- | --- |
| `buildRuntimeComposition` -> `runtimeProcessInboundHandler` on the public listener | Existing public exposure, startup authority, registration reconciliation, listener ownership and failure handling remain unchanged. API and public listeners use the same ingress handler type; the public handler additionally receives the real registration controller. No API/MCP/tools/health routes are exposed. | `TestQAPublicRawWebhookReadyRouteBothStores`: real HTTPS public request, declared HMAC, real delivery/settlement and public/API duplicate equality on SQLite/PostgreSQL. Its private-route and missing/wrong authentication refusals remain. |
| `runtimeProcessInboundHandler.ServeHTTP` -> `AcquireIngress` -> `RuntimeContextUse.AdmitPublicIngressTarget` | One acquired runtime supplies exact target coordinates, complete declared binding evidence and frozen learned/prebinding ownership. Missing registration evidence never grants raw authority; no context is reacquired for publication. | `TestRuntimeProcessInboundHandlerSelectsExactLoadedContext`, `TestPublicIngressClassificationUsesCompleteDeclaredAndLearnedOwners`, `TestPublicIngressClassificationRejectsMissingAndForeignEvidence`, `TestPublicIngressMixedBindingsKeepAuthorAndManagedTargetsDistinct`. |
| Managed arm -> existing registration `Handler` -> `serveAcquiredProcessWebhook` | Explicit, learned/prebinding and unresolved withdrawal routes retain selected callback token, exposure currentness and exact target checks. Ownership uses resolved canonical target coordinates, not the URL spelling. | `TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission`, `TestManagedRouteOwnershipIncludesFailedAndUnresolvedWithdrawal`, `TestProviderRegistrationReconcilerCollisionConvergenceAndNoResend`; retained canonical managed-transport controls refuse missing/foreign token and uppercase-path bypass. |
| Author arm and gateway -> captured `registrationAdmission` / credential validation | Declared authentication, existing gateway/pre-publication validation and publication owners remain. Runtime withdrawal or captured credential replacement cannot publish through a stale classification. No transaction-held guarantee is invented. | `TestPublicIngressCapturedClassificationFencesLearnedReplacementBeforePublication`, `TestPublicRawWebhookWithdrawalRefusesBeforePublicationBothStores`: both-store no-publication/no-construction refusal, then separate explicit restoration and fresh admission. |

The exact-f3 managed 13-root focused cohort and six affected guards passed;
their immutable receipt identities and earlier race-x3 proof pins are in
#2723 comment
[6248492358](https://github.com/division-sh/swarm/issues/2723#issuecomment-6248492358).
The original public-404 RED, intermediate hostile failures and incomplete
preflight remain retained. This refresh does not convert them to successes.

Regenerate only through the existing
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1` mechanism. Review the resulting
diff for exactly one changed body hash and no row/classification changes.
Qualification runs with that variable absent and includes the census plus
all three unchanged `TestAdmissionPredicateRatchetRejects*` controls:
uncovered startup refusal, removed verify consumer and uncovered provider
prerequisite. A clean exact-head preflight and hosted lifecycle selection
remain required; generator mode alone supplies neither.

Cost: adds 0 roots in tier lifecycle. The existing census invariant and all
three boundary-negative reaches are preserved.
