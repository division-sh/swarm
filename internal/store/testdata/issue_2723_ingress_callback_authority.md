# #2723 Exact Public-Ingress Callback Accounting

Reviewer-c authorized exactly this FULL-track classification in
[6248650613](https://github.com/division-sh/swarm/issues/2723#issuecomment-6248650613):

```text
typed-process-local  callback-type  typed  internal/runtime/inbound.go  InboundTarget  field:registrationAdmission  func(context.Context) error
```

The registry uses TSV tabs for this single exact identity. No wildcard,
scanner/guard change, new disposition, raw-SQL exemption or debt/baseline
increase follows. The callback is private and does not expose an SQL handle
or independent mutation authority. It DOES delegate currentness checks to
the existing credential projection; it must not be described as read-free.

| Boundary | Existing owner and exact binding |
| --- | --- |
| Producer | `runtimeProcessInboundHandler` first acquires one `RuntimeContextUse`. `RuntimeContextUse.AdmitPublicIngressTarget` validates that context's bundle and exact standing target coordinates, declared publication generation and frozen learned/prebinding ownership before assigning `InboundTarget.registrationAdmission`. Missing registration evidence never supplies author-only authority. |
| Captured validator | `Runtime.standingIngressCredentialScope` returns the existing validator bound to the frozen admission and selected binding. It rechecks current binding presence, enabled state, signing key, alias, compiled plan generation, learned operation, credential keys and admission receipts under the existing runtime lock. No new credential snapshot, credential lookup fallback, transaction owner or authority lifetime is introduced. |
| Delegated reads | The captured `SecretBindingProjection` owns `ValidateCurrentKeys` for the captured key set and `ObserveAdmittedActivationCredential` for each admitted key/seal/receipt. Read failures and stale observations remain refusals. The carrier neither bypasses these reads nor becomes a replacement persistence owner. |
| Consumer | `InboundGateway.handleResolvedWebhook` invokes the callback at its existing validation/pre-publication boundaries, alongside the existing credential validator. Publication remains under the same acquired runtime, source, target and native publication owners. The callback is not a transaction-held guarantee or fresh registration/execution authority. |

Existing proof reaches and their original pins are retained in
[RED-to-GREEN 6225624515](https://github.com/division-sh/swarm/issues/2723#issuecomment-6225624515)
and the exact-f3 integration receipt
[6248492358](https://github.com/division-sh/swarm/issues/2723#issuecomment-6248492358):

- `TestPublicIngressClassificationRejectsMissingAndForeignEvidence` keeps
  missing/foreign context, target and publication refusal.
- `TestPublicIngressCapturedClassificationFencesLearnedReplacementBeforePublication`
  rejects replacement of the captured learned admission before publication.
- `TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission`
  prevents registration refresh from replacing frozen runtime credentials.
- `TestPublicRawWebhookWithdrawalRefusesBeforePublicationBothStores`
  preserves withdrawal refusal without publication or construction on both
  stores, then requires explicit restoration and a separate fresh admission.
- `TestRuntimeProcessInboundHandlerSelectsExactLoadedContext` and the real
  both-store public-webhook root retain exact-context use, authentication,
  duplicate identity, actual settlement and durable conservation.

The classification is tested with registry update mode absent, including
effective-method raw-authority rejection and the existing hostile resolved
types, transitive carriers, unknown operations and compound fixture controls.
No root or accounting registry is added. Both earlier incomplete preflight
receipts remain failures; this row cannot supply their missing proof credit.

Cost: adds 0 roots in tier lifecycle. Only one exact finding is classified;
all existing authority invariants and negative reaches remain unchanged.
