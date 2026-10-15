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

## Integration With The Merged Instance Owner

The local post-#2617 integration uses master `0fe666d986` and preserves the
accepted provider checkpoint `e7c11cb003`. The `newRuntime` startup consumer
keeps master's removal of the manager's route installer, verifier and restorer
roles and the coordinator's `FlowRoutes` role. Those retired materialized route
roles do not become session-provider authority. The exact admitted instance
reader and construction/publication owners remain the runtime dependencies.

`EventBus.PrepareInboundDeliveryBatch` now composes master's run proposal and
same-batch construction preview with the existing private inbound publication
seal. It does not restore route-topology writes or select another source,
connection or capture. `PublicationCommand.ValidateInbound` still requires the
original admission and exact output ordinal, content and authorization; generic
publication and reconstructed commands cannot erase that responsibility.

The only provider test adapters affected by those retired interfaces are
`newSessionPublicationFixture`, `activeInputFixture.publicationBus` and
`reconstructPublication`. Both bus fixtures consume the real selected-store
`pipeline.FlowInstanceIndexReader` instead of the removed descriptor/route
interfaces. The reconstruction probe copies every surviving exported field;
its original generic/API/fan-out refusals and event-conservation checks remain.

The first integrated execution exposed a second stale fixture premise: the
handwritten semantic bundles had no admitted artifact, while their selected
contexts did. `sessionBusinessSemanticFixture` now compiles its existing exact
default artifact through `LoadWorkflowContractBundleFromArtifact`.
`whatsappPublicationComponentSource` reads, decodes and compiles its original
selected artifact through the same owner. Neither attaches a requested hash to
handwritten semantics or changes persisted source identity. The real compiler's
structural root and static declarations are retained. Provider-declaration
overlays remain explicit isolated component inputs, not source-loading proof.
`TestSessionPublicationFixtureUsesItsAdmittedArtifact` checks the canonical
lookup boundary positively and rejects a foreign artifact.
The store-only proof selects that same valid fixture artifact before reserving
its standing responsibility. Its former generic standing artifact contained a
retired `agents.yaml` wrapper; it is not compiled as a workaround or migrated.
The shared standing fixture, production parser and all rollback/replay assertions
are unchanged.

Affected execution witnesses are the original
`TestPrepareInboundDeliveryBatch*`,
`TestProviderRawSettlementAdmissionRequiresCompleteInboundAuthority`,
`TestGenericPublicationCannotMintProviderRawSettlementAdmission`,
`TestWhatsAppBusinessCommitTemporalFencesBothStores`,
`TestWhatsAppBusinessReconstructionAuthorityMatrixBothStores`,
`TestWhatsAppBusinessCommandReconstructionCannotDropFenceBothStores`,
`TestWhatsAppSessionCaptureNormalizedPublicationBothStores`,
`TestWhatsAppSessionPublicationOutputAuthorizationBeforeMutationBothStores`
and `TestWhatsAppInboundPublicationRollbackAndLostAcknowledgmentBothStores`.
The startup-order and authored-target witnesses remain
`TestServeRestoresNativeOwnershipBeforeRetainedProofExecution` and the
`TestSessionActivity*` projection roots. Receipts keep their exact source pins;
this accounting is not a new served timing exposure or physical qualification.

Only after those named tests pass is the predicate JSON regenerated using
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`. Ordinary census and hostile mutation
controls rerun without that variable. Hash equality is drift detection, not
execution proof or complete #2577 closure.

## N66 Logout Recovery Consumer

`channelonboarding.DestructiveService.Recover` consumes strict pending logout
responsibility without SDK dispatch. Existing effect-journal recovery owns
unlink evidence and its atomic historical projection. Destructive startup
recovery does not require a dead process's SDK, invoke the ordinary activation
refresher, release credentials, select a fresh occurrence or complete the
responsibility itself. The original operation,
revision, paired-account admission, declaration/source coordinate and SDK
occurrence remain frozen in the existing selected teardown owner.

`TestSessionLogoutRecoveryConsumesFrozenResponsibilityBothStores` executes the
real SQLite/PostgreSQL reservation and recovery owner twice. Its observation
adapter refuses any SDK dispatch and asserts unchanged frozen evidence, no fresh preparation, no ordinary
disconnecting cleanup, no invented settlement and canceled-wait refusal.
`TestSessionLogoutFencesCurrentActivationBothStores` proves that reservation
retires current executable activation and rejects standing republication and
unjournaled completion. The observation adapter grants no SDK or journal
authority. The later journal, public logout and process-death sections below
name the separate execution proofs; observation alone establishes none of them.

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

## N66 Public Logout And Accepted Settlement Tail

`TestRunServeWhatsAppPublicLogoutBothStores` executes signed pairing, principal
claim, enabled-ingress restart, Inbox, authenticated logout and a second restart.
It proves revision/account refusal, private-target-free exact result, idempotent
replay, historical readback and no implicit SDK reconnect after successful
unlink. This adds public RunServe proof, not physical or process-death closure.

`RuntimeConnection.DispatchSessionLogout` reserves finite work from its existing
process owner before journal authorization. Only journal/history settlement and
the original cleanup use that accepted tail; SDK drain, preflight, launch and
unlink retain their original runtime/source/occurrence fences. No new process,
completion capability, execution exemption or shared handoff rule is introduced.
Retirement previously left a launched attempt without its result because the
settlement tried to acquire completion work from the retired runtime.

`TestSessionLogoutJournalCanceledAfterLaunchRemainsUncertainBothStores` is RED
on the preceding production tree and GREEN after this repair on both stores
under `-race`: caller cancellation, runtime retirement, atomic uncertain result
and retained pairing. `TestSessionLogoutRequiresAcceptedProcessTailBeforeAuthorizationBothStores`
proves process fencing refuses before authorization/frame/deletion, with work
counts and the frozen responsibility unchanged. The real local-deletion fault
in `TestSessionLogoutJournalDoesNotTreatLocalDeleteFailureAsNoEffectBothStores`
proves that remote acceptance followed by local failure stays uncertain and
does not authorize resend. Process-death and successor public journeys remain
in this same PR; these named proofs do not substitute for them.

## N66 Launched Process Death And Startup Ordering

`TestServedWhatsAppLogoutLaunchedProcessDeathBothStores` executes the actual
SDK in a child RunServe process after genuine pairing and operator confirmation.
The external peer verifies the exact linked-device unlink frame and withholds
its result. The fixture kills and joins only its owned child, joins its original
HTTP caller and peer disconnect, then boots twice. Public history must retain
the same effect/responsibility, a single uncertain settlement, no READY state,
no new connection and no unlink on exact replay or changed-key refusal.

The first execution was RED on both stores: destructive startup recovery tried
to locate the dead SDK before runtime effect recovery, blocking boot entirely.
Only that automatic dispatch is removed. Strict pending inspection and ordinary
teardown ordering are unchanged; the existing effect recovery owner remains the
only interpreter and atomic writer for unlink outcome evidence. Reservation
without journal evidence stays pending, not fabricated complete. Explicit live
logout still uses the retained native owner and requires current launch fences.
The repaired both-store process-death journey is GREEN under `-race`.

The changed A10/A27 predicate fingerprint is regenerated only after these
execution proofs and the no-dispatch/no-invented-settlement store proof pass.
No scanner, inclusion, P08/P10/P21 obligation, deadline or census policy changes.

The cancellation boundary is also retained after inspection: cancellation during
the pending-row lookup must not return successful recovery or authorize SDK work.
The selected-store cancellation adapter delegates the real read and then cancels;
it changes no rows and adds no journal authority.

The actual public `cliapp.Execute` journey additionally consumes the existing
pre-dispatch API-flag leaf classifier. `channel.logout` was missing there even
though Cobra declared its flags; the internal command-construction fixture did
not traverse that public entry point. The classifier now includes the exact
logout leaf, and its existing surface/isolation table covers all channel leaves
and still refuses the channel parent. No guessed endpoint, compatibility flag,
internal client override or private-store access substitutes for public CLI
admission. CLI unlink/replay execution receipts must pass before claiming closure.

`TestRunServeWhatsAppPublicLogoutBothStores` now initiates the actual unlink
through public `cliapp.Execute`, explicit server-base/token-file flags, real RPC,
original SDK and journal. RPC then proves exact terminal replay and historical
readback. `TestServedWhatsAppLogoutLaunchedProcessDeathBothStores` additionally
uses that same CLI after each restart and checks the unchanged uncertain result.
Both-store `-race` journeys pass; the fixture uses a parsed server base, not a
forbidden direct RPC endpoint or internal client override.

`TestWhatsAppLogoutJournalProcessDeathBothStores` uses the existing private SDK
and selected-store owners in an owned child. It kills only after acknowledged
authorization (zero unlink frames) or genuine SDK unlink/deletion/join before
journal settlement. The parent joins the child, reconciles twice with its
admitted source/runtime context, and requires one unchanged frozen result.
Authorized death retains the exact paired device; post-deletion death remains
uncertain and exact private reopening refuses missing pairing, never recreating
deleted bytes. All four store/boundary cells pass under `-race`. No SDK patch,
SQL injection, alternate journal or new startup dispatcher is used.

`TestSessionLogoutJournalBothStores` also attempts concurrent native dispatch
while the original unlink result is withheld: refusal, unchanged launched
outcome and zero second frame. The selected-store recovery test's cancellation
during lookup is independently RED before the post-inspection guard and GREEN
after it on both stores. These are additional finite N66 proofs, not physical
P01-P09 or a claim that the remaining served notice/successor matrix is closed.

## Served Native Notice Acknowledgment

`TestRunServeWhatsAppNoticeAcknowledgmentBothStores` drives the authored
`notify_human` tool through a finite authenticated OpenAI-compatible test peer,
not a seeded mailbox row. Actual SDK-encrypted delivery must equal the canonical
notice content and its exact journal receipt. A foreign customer is rejected;
an unknown quoted receipt gets teaching; the authenticated operator's genuine
reply acknowledges through the neutral intent owner. Public unread count becomes
zero without altering notice status or inventing decision/history mutations.
The same RunServe restarts with the acknowledgment retained and no notice resend;
the producer is invoked for exactly its original two completion turns.

The both-store `-race` journey passes at this checkpoint. Earlier native receipt
wait timeouts, including a captured private-database fsync stack, remain retained
as qualification evidence; this passing run alone is not a causal latency repair
or deadline waiver. Physical provider interoperability is still unqualified.

`TestSessionLogoutReservationIsolatesSameAccountSiblingBothStores` reserves two
distinct connections through the canonical operation owner, without copied rows.
Logout retires only its original connection; the same-account sibling's complete
operation stays unchanged and does not inherit pending cleanup possession. A
cross-connection destruction target is refused with no second responsibility.
This is selected-store isolation proof, not an assertion that metadata alone
grants native execution or that competing physical registrations are supported.
