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
`whatsappPublicationComponentSource` compiles the original immutable artifact
admitted by its selected-store fixture writer through the same compiler owner.
Its input is the artifact, not another selected-store capability. Neither attaches a requested hash to
handwritten semantics or rewrites an existing persisted source. The real compiler's
structural root and static declarations are retained. Provider-declaration
overlays remain explicit isolated component inputs, not source-loading proof.
`TestSessionPublicationFixtureUsesItsAdmittedArtifact` checks the canonical
lookup boundary positively and rejects a foreign artifact.
The store-only proof selects its own valid, isolated root artifact before
reserving its standing responsibility. Reusing the default artifact would collide
with that store fixture's already registered default event catalog. Its former
generic standing artifact contained a retired `agents.yaml` wrapper; it is not
compiled as a workaround or migrated.
The shared standing fixture, production parser and all rollback/replay assertions
are unchanged.
The external native-activity journal proof is another compilation-time consumer
of #2617's changed fixture APIs, despite having no textual conflict. It now uses
`activityJournalProofSource`, consumes the context returned by
`requireActivityReplayRun` and retains the normalized intent returned by
`seedSelectedActivitySource`. These existing owners compile the original tool
and construct/read its actual run/root/receiver; they do not supply the missing
channel activation or install an SDK. The unchanged refusal, zero-SDK-call and
exact reopened-journal assertions remain the proof's boundary.

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
`TestNativeActivityWithoutOriginalActivationJournalsRefusalBothStores` covers the
updated selected-activity adapter on both stores.
The startup-order and authored-target witnesses remain
`TestServeRestoresNativeOwnershipBeforeRetainedProofExecution` and the
`TestSessionActivity*` projection roots. Receipts keep their exact source pins;
this accounting is not a new served timing exposure or physical qualification.

Only after those named tests pass is the predicate JSON regenerated using
`SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`. Ordinary census and hostile mutation
controls rerun without that variable. Hash equality is drift detection, not
execution proof or complete #2577 closure.

## Canonical Activity Identity Integration

The #2674 integration consumes the canonical
`contracts.ActivityResultEventsForSite(ActivitySite) (ActivityResultEvents, error)`
product instead of the retired inferred RuleID shape. Reviewer-g accepted this
integration in [6192143525](https://github.com/division-sh/swarm/issues/2577#issuecomment-6192143525)
and confirmed preparation at
[6192484767](https://github.com/division-sh/swarm/issues/2577#issuecomment-6192484767).
The initial preflight at a881c0d25f exposed the unrefreshed A10/A27 fingerprints;
that failed receipt is retained. This update records the actual owner, consumers
and existing execution controls before regenerating those fingerprints.

| Changed owner | Consumer and preserved boundary | Execution witness |
| --- | --- | --- |
| `channelonboarding.ResolveSessionActivitySite` | `pipeline.PipelineCoordinator.compileAuthoredSessionActivity` and `runtime.validateActivityToolTransport` consume the exact authored node/handler/tool/result identity. Canonical identity errors propagate; a missing or ambiguous declaration cannot select native execution. | `TestSessionActivityProjectionJoinsExactDeclarationAndIngress/explicit_rule_site`, `TestSessionActivityProjectionRejectsMissingAndAmbiguousDeclarations`, `TestRuleActivityAdmissionRequiresExplicitID` |
| `channelonboarding.sessionActivitySiteDeclared` | `compileSessionActivityDeclaration` requires exact Kind, Source, handler, rule index/reference, tool and canonical activity ID before connector/ingress projection. No handler default or rule position is inferred. | The `missing_site_kind`, `handler_with_rule_position`, `rule_without_position`, `unknown_site_kind` children of `TestSessionActivityProjectionRejectsMissingAndAmbiguousDeclarations`; the explicit-rule positive above |
| `runtime.validateActivitySpec` / `validateActivityToolTransport` | Pass the already validated `ActivitySite` and result product into native transport admission. Preserve master's rule-ID, presentation, collision and approval checks and the provider's source/connector/ingress qualification. | `TestRuleActivityPinnedNamesPreserveEveryInventoriedBinding`, `TestActivitySiteContextCannotBeInferredFromLabelOrIndex`, existing `TestValidateWorkflowContractSurface_*Activity*` controls |
| `runtime.validateHandlerActivitySurface` / `rejectUnsupportedNestedActivityContexts` / `activitySiteContext` | Retain master's explicit handler/rule Kind and index, presentation-only labels, forbidden handler/rule combinations and nested-context refusal. The native target consumer cannot infer identity from the display label. | `TestRuleActivityValidationRequiresExplicitIDAndPreservesCombinationRefusal`, `TestActivityDiagnosticsUseRulePresentationAndPreserveRefusal`, `TestActivitySiteContextCannotBeInferredFromLabelOrIndex` |
| `runtime.activityRevisionConsumerExists` | Activity approval consumes the effective source's matched subscription, not a restored raw handler-name lookup. | `TestValidateWorkflowContractSurface_ActivityApprovalBoundary` |
| `runtime.validateActivityResultEventNameCollisions` / `admitActivityResultEventName` | Preserve master's admitted authored-event producer census and normalized generated-name collision checks. The new helper already exists on master; it is recorded rather than excluded from this whole-file census. | `TestActivityResultEventAdmissionPreservesExactCollisionAndRegistration`, `TestValidateWorkflowContractSurface_DurableActivityResultEventsRejectAuthoredCollision`, `TestValidateWorkflowContractSurface_DurableActivityResultEventsRejectGeneratedCollision` |

Fresh clean-a881 execution uses the managed PostgreSQL runner with race,
count1 and p1 across the contracts, pipeline and runtime packages. It includes
all `TestSessionActivity*` projection roots and
`TestNativeActivityWithoutOriginalActivationJournalsRefusalBothStores` plus
`TestRuleActivityCallbackJournalAndRestartBothStores` on both stores. All pass
after joined cleanup; the earlier direct invocation lacking a PostgreSQL DSN
remains a failed command, not a runtime or whole-packet success. The original
395 packet retains its own source pin.

The audit rows remain A10/A27 and proof families remain
2577-authored-session-activity/P08/P10/P21. No row, consumer, negative case,
scanner scope or admission policy is removed. These are component proofs, not
physical P01-P09 or the separately held served PostgreSQL resume exposure.

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

## P01 Authorized Terminal Pairing Observation

The bounded gate is #2577 comment 6198923816. This changes only the
`ReadSessionPairing` consumer within A10/A27; all P08/P10/P21 and existing
2577-native proof obligations remain. The four original public RED leaves
identify execution-health `currentConnection` as the first refusal after a
genuine unpaired QR transport ends. No business health is inferred from a
retained pointer.

| Owner / caller | Preserved or repaired predicate | Execution proof |
| --- | --- | --- |
| `serveSessionBootstrap.ReadSessionPairing`, called by onboarding result/list readback | Select the completed original attempt; construction/connect errors remain refusals; its private owner authorizes the selected principal, operation and coordinate | `TestReadinessPublicUnpairedQRWaitDiagnosticBothStores`, both stores, live exhaustion plus independent remote close |
| `RuntimeConnection.PairingReadback` / `pairingQR.readAuthorized` | Same current-operation, source/runtime, principal and coordinate gates; only zero-code observed terminal state projects | `TestWhatsAppPairingTerminalReadbackIsObservedAndScoped` plus existing authorization/cancellation controls |
| SDK disconnect handler / pairing QR owner | Actual disconnect records failed, or retains previously observed QR exhaustion as qr_expired; a dead socket or generic cancellation cannot invent expiry; malformed evidence remains an error; original strict failure cause is retained | The four served leaves plus terminal readback source-cancellation, retirement, malformed-batch and selected-store negatives |
| `currentConnection`, `CheckBootstrap`, account admission and channel execution | Execution health stays strict; observation installs no account, standing activation or readiness | Every served terminal leaf refuses current/execution/account authority; `TestServeSessionBootstrapAttemptOutcomesBothStores` and `TestServeSessionPendingConstructionFailureRemainsARefusalBothStores` retain genuine transport/construction/retirement refusals |

`2577-native-terminal-pairing` names this execution proof, not an exception to
the census. At signed 6b2bf8e0a7, the focused packet passed all 369 test/subtest
records under race x3, including every both-store public leaf three times. The
original diagnostic aggregate stays RED classification evidence. The initial
preflight then correctly caught this changed consumer fingerprint; regenerate
only after the owner/caller and proof mapping above. This is not a claim of
final qualification or physical interoperability; exact-head preflight, the
agent-feasible live QR wait and normal hosted qualification remain separate.

### P01 Duplicate Disconnect / Retirement Boundary

Reviewer-g's #2577 comment 6199462678 identifies a distinct same-owner defect:
the second observed disconnect erased the first terminal snapshot. The bounded
repair makes repeated observed disconnects idempotent, but leaves generic
retirement as a clearing refusal; later disconnects cannot recreate that state.
No execution-health, disclosure-scope or failure admission is relaxed.

`TestWhatsAppPairingDuplicateDisconnectReadbackBothStores` orders two cuts at the
existing private read owner over actual selected SQLite/PostgreSQL stores and
the original unpaired SDK connection, then invokes the full SDK lifetime fence.
Both failed and observed-expired states
are checked after events one and two through native pairing readback, onboarding
get and connected-channel list. Scope and zero-code observations must stay
exact, while standing readiness and channel execution remain unavailable.
Generic retirement followed by another event, and actual runtime retirement
followed by another event, must both refuse. These are read-owner cuts, not a
claim that the external socket peer generated duplicate SDK callbacks. The
existing four served public controls retain real transport execution proof.

With the original clearing branch restored in a disposable overlay, all four
ordered store/status leaves fail specifically on event two and the later full
lifetime fence; event one and both retirement refusals pass. Raw RED SHA256:
46b508013519ea2838e2ab07b07e6beda142ca3d0f454ec91832408d4547f35b.
The earlier full-lifetime probe is retained separately (SHA256
5b623d8af40ac635097ebb9d4807a4cb0800622fe4f8bf0895bfe01242ff0997):
one PostgreSQL first read also failed, so that probe does not establish the
callback count at its first read and is not the ordered counterexample.
The existing isolated authorization matrix also repeats the disconnect before
each scope/error check. Fresh focused/race, exact-head preflight and real
QR-render/expiry receipts are required for this successor; old passes are not
relabeled as its qualification.

### P01 Explicit Replacement After An Exhausted QR Batch

Reviewer-g's #2577 comment 6201030995 approves only the exact reuse owner.
The real supplied-command rehearsal at b2a300d5ee refused after the first QR
expired: same original operation, activating_provider/revision 11, CLI exit 2.
That RED remains separate from the two load-sensitive pre-QR preparation REDs
and the accepted paired-PostgreSQL resume residual.

| Owner / caller | Boundary | Execution proof |
| --- | --- | --- |
| Service.Retry -> resumeSession -> serveSessionBootstrap.ResumeSession | Explicit continuation of the SAME retained operation/reservation; an unfinished connection remains pending, not a replacement instruction | TestServedExpiredQRExplicitRetryReplacesOriginalOccurrenceBothStores verifies the expired occurrence already completed connect and its socket is live |
| RuntimeConnection.CheckSessionReuse -> exact occurrence-owned pairingQR.read | An unpaired qr_expired batch refuses reuse despite live transport; current QR and paired connected reuse are unchanged; observation errors retain their cause, never permission to reconnect | TestWhatsAppPairingReuseExpiryAndObservationErrorsBothStores plus TestServeSessionExplicitResumeRetainsPairedIdentityBothStores and TestServeSessionResumePreservesObservationErrorWithoutReconnectBothStores |
| Existing ResumeSession Close-before-open and RuntimeConnection.Close | Join original SDK/callback/QR work and release private-directory possession before a successor can open the same reservation; cancellation retains cleanup ownership | Both new roots, TestServeSessionResumeCancellationRetainsJoinAndPossessionBothStores and TestServeSessionConcurrentResumeConstructsOneSuccessorBothStores |
| Authenticated public retry/get and original scope | Fresh QR/expiry with the same operation/principal/source/interface/target and nondecreasing selected revision; no account, identity, standing activation or executable readiness | TestServedExpiredQRExplicitRetryReplacesOriginalOccurrenceBothStores, both stores; terminal/scope/duplicate-disconnect controls remain mandatory |

The deterministic public control failed on BOTH stores before the production
change, specifically because retry reused the live, completed expired
occurrence. Its test source is unchanged between RED and GREEN. The repaired
nine-root packet passed -race -count=3: 135 test/subtest PASS records, no failures
or skips. Provider package 86.296s; served package 213.697s. The initial extra
provider-control attempt lacked its fixture's QR batch and remains a setup RED;
the corrected test sends a genuine public SDK pairing request before testing
the reuse decision. No product deadline or existing assertion changed.

Receipt SHA256: pre-fix public RED
7ce13a8485959c541bd55686107c11e63f8cd455876551c1b612a81b51ed53c5;
repaired race packet
96718556439c97b32d2a308794721dc84305b7c3a9136e4573cd5b9152932be2.
Focused evidence is not a local tier, hosted credit, physical pairing or a
readiness PASS. Fresh affected guards/preflight and the single quiet actual-SDK
same-operation replacement rehearsal remain required before the PR push.

### P01 First-Batch Preparation Projection

Reviewer-g's conditional gate #2577/6201624807 is met by the deterministic
TestWhatsAppHeldFirstQRPublicCLIProgressBothStores. The genuine SDK peer holds
its first QR until an explicit wire send, after connect completes. The actual
authenticated get and retry results are separately captured and replayed only
in memory through the real CLI entrypoint and its private progress validator.
Both stores refuse BOTH responses with exit 2 before the repair: a valid exact
selected declaration, activating_provider, awaiting_qr, no code/expiry, no
pairing/connection/account/identity/READY. Releasing the real first batch proves
ordinary current-QR progress. No QR or account values are logged.

| Owner / caller | Boundary | Execution proof |
| --- | --- | --- |
| Service.result -> Result.projectInitialSessionPreparation | After all read owners and caller cancellation checks, only exact initial non-executable evidence becomes preparing; no private state or durable revision write | TestWhatsAppHeldFirstQRPublicCLIProgressBothStores; TestInitialSessionPreparationProjectionIsExact |
| Result.ProviderCredentialRequirement -> SessionPreparationPending | Selected declaration, operation, coordinate, target, posture, ceremony and phase must agree; no account, binding, identity, logout or READY | Finite projection negatives; existing caller-cancellation and pairing read-error/scope matrix |
| CLI channel resume -> validateCLIChannelProgress -> SessionPairingPending | Both captured get/retry responses consume the existing preparing contract; awaiting_qr still requires current material/future expiry | Both-store held-first-batch control, released real QR, contradictory/expired CLI controls |

The quiet supplied-command first-progress RED at 3d05729332 remains retained
under #2577/6201572376. It captured the CLI exit and last preceding public
read, not the rejected response itself; the deterministic cut identifies the
same possible predicate for both responses without claiming a retrospective
trace. It remains distinct from the earlier preparation-deadline REDs, expired
batch reuse RED and accepted paired-PostgreSQL resume residual. Fresh focused
race, affected guards/preflight and ONE quiet supplied-command rehearsal are
required before hosted qualification or readiness.

The identical held-first-batch test source is used for RED and GREEN (SHA256
799c74ff611e84ea7049baee7be9dded3a07a55cf730988ea972df3194718f9e).
RED raw receipt SHA256
443151ba43882d18a448d460d7ba2fe4ef7d286081956b402f23f2458f79c3d9;
initial GREEN receipt SHA256
b3e2129ddcbd4c76895c43eb0e22d70a4c65683e034290ebab430fa3028a1ee9.
The initial combined GREEN invocation first stopped at two new test compile
errors (interface fixture assignment and noncomparable operation comparison),
before admission; the corrected projection table and unchanged held-QR root
then passed. This setup failure earns no execution credit.

Reviewer-g's P2 hardening in #2577/6202089115 requires a positive bounded CLI
request count for EACH separately captured held get, held retry and released
QR result. The existing resume contract is exactly onboarding_get then ONE
explicit onboarding_retry. The replay handler enforces that order and both
requests, so a zero-call success, extra continuation/poll or wrong method
cannot earn proof.
This consumer assertion does not change the public result, transport, SDK,
private state, operation or the retained RED's production predicate.

The strengthened both-store root and the two existing canonical CLI progression
controls pass race x3: 42 test/subtest records, no failure/skip, joined wrapper
exit zero. Raw receipt SHA256
45480e53d21de39f605d96e2dc709a4322223bc1c26e9d75443342839425b0e9.
An initial local hardening attempt incorrectly allowed only a get and rejected
the CLI's existing explicit retry; that test-only setup RED is retained and
corrected to the existing get-then-retry contract, without a production edit.

### Paired Bootstrap Transport Versus Login Cut

Reviewer-g #2577/6202241187 authorizes a deterministic diagnostic, not a
production repair or qualification repeat. TestWhatsAppHeldAuthenticationCheckpointBothStores
records actual private-owner booleans: before auth, current/live/non-fenced,
non-closed/non-retiring, transport and bootstrap complete, SDK logged-in=false,
checkpoint ok=false with nil error. After releasing the genuine SDK success
frame and joining its existing connected milestone, only login/checkpoint
become true. No account or session values are logged.

TestServeSessionHeldAuthenticationCheckpointBothStores independently exercises
the REAL outer ResumeSession with an empty retained account. Its bootstrap
attempt completes while the paired socket's auth-success frame is held;
checkpoint is false/nil. After release, the exact socket's SDK post-auth count
IQ is the fixture milestone, and the unchanged checkpoint owner returns true.
Original operation, coordinate, reservation, revision transition, account,
private header, no identity/activation and no extra selection are asserted.
The native control records SDK booleans directly; the outer control uses the
real protocol milestone and does not claim access to private SDK fields.

Both roots pass race, count 1, both stores (6 test/subtest PASS, zero fail/skip,
joined wrapper exit zero). Raw SHA256
1e4489f0a79f1c6b54bd67fb8d1a81b9897af105de9f698982269d1f07464630.
The first outer diagnostic failed to enter its test-only hold because it
tested a numeric username value rather than optional-field presence for the
existing synthetic SDK account. That setup RED is retained; the corrected
presence check preserves the original fixture and does not change any product
deadline. The existing failed paired-resume receipt remains failed evidence.

Reviewer-g's bounded fixture-wait gate
[#2577/6202592663](https://github.com/division-sh/swarm/issues/2577#issuecomment-6202592663)
authorizes the implemented test-only correction. The paired peer signals only
the login socket's post-auth encrypt/get/count IQ, not the unpaired seed socket
or Noise completion. Its single buffered token is consumed once, not a closed
channel reusable for a distinct successor. All non-paired peer modes retain
their previous signal semantics. No product checkpoint, scope rule, deadline,
SDK implementation or implicit reconnect changes.

| Fixture consumer | Milestone consumption and unchanged oracle |
| --- | --- |
| TestServeSessionResumePublicationRebindBothStores | One bounded first-login wait, then the original checkpoint and publication/candidate/single-successor assertions |
| TestServeSessionExplicitResumeRetainsPairedIdentityBothStores | One bounded first-login wait, then the original checkpoint, reservation/account/header/revision/no-identity and repeated-resume assertions |
| TestServeSessionResumePreservesObservationErrorWithoutReconnectBothStores | One bounded first-login wait, then the original checkpoint and selected-read-error/no-reconnect assertions |
| TestServeSessionConcurrentResumeConstructsOneSuccessorBothStores | One bounded first-login wait, then the original checkpoint and concurrent exact-one-successor assertions |
| pairedServeSessionObserver and its four readiness consumers | The existing 5s bound now contains one milestone wait and one real checkpoint, replacing the mutating 5ms checkpoint poll; missing login or false checkpoint still fails |
| TestServeSessionHeldAuthenticationCheckpointBothStores | Preserve the explicit pre-auth false/nil checkpoint; release auth, consume the real milestone and require the original true checkpoint and all identity/header/account assertions; unpaired/held signals refuse and a distinct authenticated successor cannot reuse consumed evidence |

A disposable old-immediate-oracle assertion failed deterministically at the
held cut on BOTH stores: transport/bootstrap complete, checkpoint false/nil;
after release the unchanged checkpoint was true/nil on both. No other assertion
failed. Wrapper exit 1 is expected RED evidence, not qualification. Raw SHA256
2b83aeef9c4c5770925a2e22e0234ab0451c95fe95f8d2b234327820440481f7;
stderr SHA256 3653aaf9fa541aa42d496f629bb931f8b798eb03ea0fda25eb7c2d491f751513.
The disposable assertion is not retained in product/test source. Fresh corrected
both-store race, affected guards and exact-head preflight must pass before the
single quiet real-SDK rehearsal. A fixture milestone alone grants no production
readiness, account authority, hosted credit or sitting approval.

The corrected ten-root control passes race, count 1 on both stores: 30
test/subtest PASS, zero fail/skip, joined wrapper exit zero. Native package
14.409s, serve package 92.309s. It includes the explicit held negative,
released checkpoint, distinct-successor signal refusal, four immediate callers
and four readiness consumers. Raw SHA256
bda61420073e3fdf0fb0acb99902012835d45e418d1eada6cc1703d03c549729;
stderr SHA256 744a5f38901a875b3df29a94310db42fa70f970917729d44202c30f24a6f1efb.
These are focused iteration receipts, not final-head qualification or a quiet
physical rehearsal. Final-head race/guards/preflight and rehearsal receipts
are named in the subsequent signed issue checkpoint without rewriting history.
