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

## N66 Journal And Original-Transport Retention

`serveSessionRetirementRequired` now consumes the canonical
`channelonboarding.RetainedSessionLogoutPending` decision before disconnecting
retired session operations. This exception is only cleanup possession: pending
logout still rejects business execution and standing republication. The exact
frozen responsibility retains its original SDK until the existing journal
settles; unrelated ordinary retirement retains its previous join/retention
behavior. No source lookup, replacement account or fresh connection is adopted.

`TestServeSessionLogoutDispatcherRetainsOriginalUntilSettlementBothStores`
proves pending transport retention, business refusal, real SDK unlink, atomic
teardown settlement and subsequent cleanup on SQLite/PostgreSQL under `-race`.
`TestServeSessionDestructiveLogoutUsesJournalAndExactReplayBothStores` exercises
the real destructive service, original connection and effect journal, including
revision refusal, exact replay, changed-key refusal and canceled readback.
`TestServeSessionCanonicalRemovalJoinsOnlyOriginalOwnerBothStores` and
`TestServeSessionTeardownJoinsOriginalOwnerAndPreservesPairingBothStores` retain
the ordinary removal/interface/context controls.

`TestSessionLogoutJournalBothStores` separately observes committed launch before
the actual SDK unlink frame and proves acknowledged versus lost-result outcomes.
`TestSessionLogoutJournalRecoveryAndRollbackBothStores` proves authorized,
launched and response-observed recovery, repeated recovery, no dispatch/deletion,
exact historical target retention, and rollback for incomplete success evidence.
`TestSessionLogoutJournalRejectsForeignAuthorityBothStores` covers ten authority
contradictions and byte substitution without journal persistence or SDK effects.
These are executed component/service proofs, not physical interoperability,
public RunServe logout, process-death or the complete N66 closure matrix.

The source-derived primitive census now explicitly includes the logout launch
file and its `sdk_logout` primitive, just as it includes the native send file.
The owner remains operator infrastructure, with distinct `channel_logout`
registration, typed attempt, and `MarkLaunched` before unlink. The unjournaled
convenience method exists only in component test code. No scanner exemption,
read-only classification, baseline ratchet relaxation or test deadline changes.

`buildRuntimeComposition` installs the exact serve-session dispatcher and
selected effect owner into the existing destructive service. The existing
`TestRunServeWhatsAppSignedPairingBothStores` boot/auth/pairing journey and
`TestServeRestoresNativeOwnershipBeforeRetainedProofExecution` startup-order
control pass with that construction. This does not claim a public RunServe
logout/interruption matrix merely because the component dispatch path passes.

`TestSessionLogoutJournalDrainsAndCancelsWaitBothStores` additionally holds a
real SDK decryption transaction through journal authorization. Launch must stay
authorized until the drain finishes; original-caller cancellation releases its
wait, preserves unresolved cleanup possession, and joins the counted settlement
without unlink/deletion. The setup performs account observation before holding
the private transaction; otherwise the setup's own read cannot reach dispatch.
