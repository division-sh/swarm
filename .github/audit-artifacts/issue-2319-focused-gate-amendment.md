# Pre-Implementation Coverage Audit: Focused #2319 Amendment

This is a request for an independent bounded gate decision, not an implementation or closure claim. The approved G1/G2 class and ONE-PR ceiling are unchanged. Production work is preserved in local commit `c76f007e1`; no #2319 PR or branch push has occurred. #2545 has separately been rebased and pushed as `1945bb726`, with its generated describe test passing. #2319's no-PR-before-#2545-merge condition remains binding.

## Missed Consumers And Evidence

The original audit named onboarding retry, preflight, reconciliation, writer, and public readback, but did not explicitly enumerate their early-admission reset and error-projection paths. The added permanent proofs expose these omissions rather than treating the reported line as the boundary.

| Row | Consumer / current outcome | Permanent failing proof / proposed correction |
| --- | --- | --- |
| M55a | `Service.driveLocked`, `PhaseCredentialsAdmitted`, stale admission: after exact signing cleanup fails, advances to preparing and clears all admissions | `TestRejectedCredentialCleanupRetainsAdmittedResponsibility/credential_stale`: FAIL. Real FileStore/writer and injected receipt-deletion failure; selected-store fixture changes `credentials_admitted/9/2 admissions` to `preparing/10/0`. Preserve exact evidence until successful cleanup, then revision-fenced reset. |
| M55b | The same phase's rejected-preflight branch repeats that reset despite failed/partial cleanup | Same root `/preflight_rejected`: FAIL, same discarded phase/revision/evidence. Use the same bounded owner-local reset decision, not a separate copy. |
| M55c | `Service.ReconcileLocal`, `PhaseCredentialsAdmitted`, stale admission clears evidence without invoking cleanup | Same root `/local_reconciliation`: FAIL; returns nil and clears the same evidence. Route through the same owner; cleanup failure remains an error with unchanged durable admission evidence. |
| M56 | `apiv1.channelOnboardingError` preserves missing/stale credential application errors but drops `credentials.ErrCredentialValueUnusable` into `platform.internal_failure/unclassified_runtime_error` | `TestChannelSigningFallbackPublicRefusalBothStores`: all eight empty/whitespace cells FAIL on the proposed precise public refusal expectation, across reserved and previous-current keys on SQLite/PostgreSQL. No credential writes or extra provider effects occur. Preserve a precise unusable-value refusal using the existing failure-envelope owner, distinct from absence. |

Commands actually run on the local #2319 tree:

```sh
go test ./internal/channelonboarding -run '^TestRejectedCredentialCleanupRetainsAdmittedResponsibility$' -count=1 -timeout=30s
go test ./internal/serveapp -run '^TestChannelSigningFallbackPublicRefusalBothStores$' -count=1 -timeout=90s
```

Results: cleanup root FAIL, 0.021s, all three cells; public root FAIL, 10.010s, eight unusable-value classification cells. The public root also ran eight corrupt-file/actual filesystem-read-error cells successfully. It uses real served RPC, selected stores, FileStore, provider protocol fixture, connected predecessor ceremony, and byte/member/effect invariance checks. The cleanup proof's selected-store fixture is not claimed as both-store or public closure evidence. Logs are local scratch; permanent tests are the reproducible artifacts.

## Bounded Design And Consumption Audit

1. **Cleanup-before-reset owner:** use one small early-admission reset operation inside existing `channelonboarding.Service`. It must retain the full exact admission set on cleanup error, preserve partial-cleanup retry evidence, and commit preparing only after receipt/value-seal-fenced cleanup succeeds and the selected-store revision still matches. No generic reset bypass, new recovery worker, schema, migration, or framework.
2. **Consumers moved together:** the two explicit retry branches and local reconciliation above. The existing pending-identity reset and confirmed-binding reconciliation already keep admissions until cleanup succeeds; preserve those owners and their child/binding requirements rather than applying an early-phase helper to them.
3. **Other cleanup consumers checked:** terminal failure/destructive teardown retains terminal operation evidence and uses `CredentialWriter.ReleaseOperation`; superseded activation cleanup retains predecessor operation history. They are not early-admission reset interpreters. Keep successor and observed-credential deletion fences unchanged.
4. **Unusable public error:** proposed implementation projects only the existing typed unusable-value sentinel through `runtime/failures`' existing `platform.authentication_required` class with finite detail `credential_value_unusable`, retaining the cause. This introduces no new application code, API method, public error framework, or guessed string classification. Genuine absence remains `CHANNEL_CREDENTIAL_REQUIRED`; corrupt/I/O outcomes remain fail-closed and must not be relabeled as absence. This exact detail expectation is proposed, not falsely attributed to a prior ruling.
5. **Public consumers:** real `channel.onboarding_start` and `channel.onboarding_retry` exercise the same error mapper. Structural boot and local observation already preserve unusable/error distinctions through the credential owner. The public proof is registered in the existing `serveapp-standing` CI partition; no new partition or ceiling change.

The old non-authoritative paths are the three independent early-reset branches and the API's unclassified projection of a known credential failure. No old-store interpretation or compatibility path is retained.

## Required Proof After Approval

- Promote M55 to real selected-store tests on both stores, covering cleanup failure/partial cleanup, phase/revision fences, replay, restart, successful exact reset and no new effects. Preserve inherited reconnect evidence and existing pending-reset crash proofs; do not broaden the pending-child reset API to a phase it does not own.
- Complete M56's public matrix, including absence as the positive signing-generation control, invalid reserved and retained keys, exact error class, corrupt/read-error refusal, byte preservation and zero added registration/delivery effects. Keep typed owner/API tests and secret-redaction controls.
- Re-run the existing recovery, pending-reset and admitted-handoff matrices plus the default `swarm-test` qualification on the eventual final tree. No `--full`, timeout waiver or closure claim now.

## Gate And Tracking Decision

Chosen class remains declaration/prerequisite/executable-binding/readiness drift over the full binding lifecycle. Parent/sibling dispositions and watchlist mapping from approved #2319 G1/G2 remain unchanged. These rows extend the same early-admission/recovery and public-consumption census; no new issue or staged follow-up is proposed. Closure commitment remains full chosen-class elimination in one PR.

**Tracker-state decision:** append M55/M56 to #2319's gate record and receive independent confirmation of this bounded owner-local repair before production edits to these paths. Existing production changes are preserved; no silent widening. The public proof intentionally asserts a proposed classification that needs confirmation.

**Architecture disposition requested:** promote now inside #2319 using existing Service, credential and failure owners. The broader parent is not absorbed or newly claimed closed. A small owner-local repair and focused lifecycle proof are feasible; no new semantic container is justified.

**Gate outcome at posting: pending.** Please confirm the bounded cleanup owner/three-consumer repair and the precise unusable-value public projection, or rule the latter a distinct class with an explicit tracking disposition. No wholesale pre-audit rewrite requested.
