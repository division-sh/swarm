# #2577 Declaration-First Bootstrap Predicate Accounting

This supplements #2319's accounting for the existing startup predicate census.
The census's A10/A27 and P08/P10/P21 labels belong to #2286, not #2577's separate
channel proof numbering. No predicate scanner, refusal classification, owner
exemption, debt baseline or test ceiling is changed.

| Changed Owner | Consumer And Authority | Executed Proof |
| --- | --- | --- |
| `serveChannelCandidatesForPlan` | Session discovery now consumes the same exact ingress declaration as webhook discovery. It retains source/flow/provider/alias and accepted trigger generation, but creates no executable target, publication sequence, credential or signing role. The former target-free session shortcut is removed. | `TestServeSessionDiscoveryRetainsDeclarationWithoutAuthority`, `TestSessionCandidateRequiresDeclarationNotExecutableTarget`, `TestCredentialReservationsFollowDeclaredRoles` |
| `buildRuntimeComposition` | Existing onboarding composition supplies the concrete session bootstrap owner. Construction is metadata-only; unsupported hosts return typed unavailability and retain session refusal without affecting unrelated webhook channels. Runtime source selection precedes private SDK construction. | `TestServeSessionBootstrapConstructionDoesNotOpenState`, `TestWhatsAppBootstrapRequiresReservationBeforeStateBothStores`, `TestWhatsAppPublicBootstrapReservesBeforeQRBothStores` |

The public-handler proof runs the actual authenticated `/v1/rpc` handlers,
selected principal, onboarding service/reservation and guarded SDK QR owner on
SQLite and PostgreSQL. Runtime selection/transport attachment are explicit
fixture inputs. This is not a claim of complete RunServe, human claim, business
activation, outbound or physical interoperability qualification. Those remain
in this same #2577 PR before its final audit and supported-provider merge.

The added SQL argument for the immutable connection reservation belongs to the
existing private `backend/channelonboarding.reserve` transaction. Its registry
entry is replaced one-for-one with the new exact signature under the identical
`private-backend` disposition. No new raw-authority site or permission is added.
Both-store original reservation, exact replay, rejected foreign account and
revision-preservation proofs establish the writer/reader ownership independently
of that generated fingerprint.

Only after those named tests pass is the predicate JSON regenerated using
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`. Ordinary census and hostile mutation
controls rerun without that variable. Hash equality is drift detection, not
execution proof or complete #2577 closure.

## N66 Logout Recovery Consumer

`channelonboarding.DestructiveService.Recover` now dispatches the distinct
`logout` responsibility through its typed session-lifecycle port. It does not
invoke the ordinary activation refresher, release credentials, select a fresh
SDK occurrence or complete the responsibility itself. The original operation,
revision, paired-account admission, declaration/source coordinate and SDK
occurrence remain frozen in the existing selected teardown owner.

`TestSessionLogoutRecoveryConsumesFrozenResponsibilityBothStores` executes the
real SQLite/PostgreSQL reservation and recovery owner twice. Its observation
adapter asserts unchanged frozen evidence, no fresh preparation, no ordinary
disconnecting cleanup, no invented settlement and canceled-wait refusal.
`TestSessionLogoutFencesCurrentActivationBothStores` proves that reservation
retires current executable activation and rejects standing republication and
unjournaled completion. The observation adapter grants no SDK or journal
authority: native dispatch, outcome settlement, restart-to-completion and the
served logout journey remain unfinished in the same #2577 PR.

This accounts for the changed A10/A27 consumer body before regeneration; it
does not alter the scanner, its scope rules or the existing P08/P10/P21 proof
obligations, and does not claim their full N66 execution closure.
