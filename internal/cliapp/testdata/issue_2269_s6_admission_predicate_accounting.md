# S6 Descriptor Startup-Census Accounting

The inherited whole-file runtime.go census assigns A05/A08/A11/A13/A18/
A23/A24/A25 and P04/P08/P09/P11/P15/P20/P21 to every body. S6 changes only
AuthorActivityEventDescriptors and its extracted authoredCreationEventKeys
projection in this measured file, not those admission predicates, boot
sequencing, hydration, timers or recovery authority.

Governing user ruling: #2269/6170784774; bounded review6178053370.
AuthorActivityEventDescriptors remains the source-to-story-descriptor owner.
Its new creation-input classification projects the authored auto-emit line
regardless of schema ownership. It does not add a schema, routing permission,
platform occurrence kind, readiness grant or provider/claim permission.

The unchanged production callers are newRuntime in runtime.go,
SelectedContractExecutionOwner.Prepare in runforkexecution/preparation.go,
ActivateSelectedContractRunFork in activation_gate.go, and
SelectedContractExecutionOwner.beginSelectedCancellationInspection in
cancellation_recovery.go.
Each retains its actual source and existing catalog lease/lifetime. Caller
code is unchanged: no boot step, selected recovery ordering or dispatcher
connection is introduced. The descriptor is observation metadata, not a new
startup refusal owner. The existing check registry and A/P labels stay intact.

authoredCreationEventKeys contains the unchanged creation-event loop extracted
from AuthorActivityEventDescriptors to preserve the complexity ratchet. Its
only caller is that descriptor owner; it consumes the same source, resolves
the same authored auto-emits and returns the same exact event-key set. It is
not a new admission predicate, source reader, catalog lease or boot step.

Execution witnesses: the five TestAuthorActivityEventDescriptors roots cover
internal stage timers, join demands/conflicts, normal catalog lifetime, compiled
joins and creation classification. TestRebuildPendingCreationConsumesCompiledReceiverOccurrence,
TestS6CreationReferenceOrderMetadataDiagnostic and
TestReceiverDescriptorStillRequiresExactEvidenceAndLiveLease cover the actual
constructor descriptor, exact publication/source coordinates, foreign/released
lease refusals and unchanged non-creation inputs. The native both-store
TestS6ReferenceOnlyCreationEmitsNativeActivityBothStores and completion-mark
fault family execute this descriptor owner and validate ordinary child-only
event.emitted, exact duplicate/refusal/sequence conservation and pre-Finalize
abort. They are not complete startup or recovery qualification.

The clock validator continues to use the declaration-only event predicate;
an authored creation line cannot manufacture timer declaration eligibility.
The same named clock checks and constructor relation controls remain required.

Regenerate through SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS and compare the
artifact delta: only the descriptor body hash and its extracted helper entry
may change, with the same owner, caller family, labels and all other entries
retained. Generation gives no execution
credit; exact receipts belong in the S6 checkpoint audit. Connected dispatcher,
occupancy, selected-preparation and runtime recovery joins remain gated.

## Selected Preparation Read Projection (A22/A24, P20/P21)

P18/P19 bounded gate #2269/6161191417 permits an immutable selected actor
projection, not connected occupancy or a second claim-profile ledger.
`loadSelectedRecoveryRecordTx` remains the latest-execution reader for the
unchanged mutation and verification callers: `recoverSelectedForkTx` and
`inspectSelectedRecoveryTx`. It delegates to `loadSelectedRecoveryExecutionTx`
with no execution selector, preserving the original latest-generation query,
fixed-cut origin/binding, preparation/declaration fingerprints and failure
validation. Only the original mutation caller may request a row lock.
`selectedRecoveryExecutionQuery` owns only the closed latest/exact selector and
the original dialect/lock suffix; it has no decoder, transaction or authority.
Separating that query construction preserves the complexity ratchet without a
policy/baseline exception.

The two native `ReadSelectedAgentPreparation` methods compose the exact claimed
agent delivery, run snapshot, fixed binding and that same retained decoder in
one read transaction, supplying the delivery's exact execution ID. An absent
or foreign execution never falls back to the latest execution. The projection
requires the original generation, source, fork, actor identity and declaration
configuration revision. It does not consult today's agent cache, acquire a
process, reinterpret startup receipts, mutate a row or authorize a launch.
Immutable historical evidence may be read after quiescence; the unchanged
selected delivery claim/read fences continue to govern live work.

Execution witnesses are
`TestSelectedRecoveryUsesNativeAdmittedPreparationWithoutProcessJournalBothStores`
(the existing boot/inspection owner and hostile provenance),
`TestSelectedPreparationExecutionBindingBothStores` (the original preparation
and execution admission checks),
`TestSelectedContinuationReadFenceBothStores` (live vs terminal/fenced
observation), and
`TestSelectedAgentClaimReadsOriginalPreparationWithoutOrdinaryCaptureBothStores`
(native first claim, physical NULL ordinary profile, exact caller negatives,
quiesced historical readback and full-table conservation), and
`TestSelectedAgentPreparationCoherentMembershipRefusalBothStores` (actual native
issuance/claim with coherent preparation fingerprints, then exact actor or
declaration-config membership refusal, with no post-commit fault). The original
ordinary first-claim/reclaim/reopen families also exercise the shared exact
claimed-delivery reader. Named receipts belong in the checkpoint, not in the
generated census. The selected preparation fault owner follows reviewer-b
#2542/6180524710 and reviewer-g/6180527018: one-column exact native CAS,
owner-captured restoration receipt, canceled/foreign/stale/changed-replacement
refusals and full conservation. Its foreign-actor variant retains the original
fingerprint and proves integrity refusal only, not downstream membership.

Regeneration must change only the decoder's wrapper/body entries and add its
new shared decoder/query helper entries under the inherited A22/A24, P20/P21
labels. The
recovery/process, declaration, readiness, startup sequencing and every other
owner/caller stay unchanged. These witnesses are not S6 qualification or
permission to connect the dispatcher, occupancy or runtime recovery.

## Private Retained Recovery Read (A19/A20/A28, P18/P19/P24)

Bounded gate #2269/6190014170 and reservation #2542/6190280510 retain the
original process capability/session as the sole possession owner. The
native-interruption proof is authorized separately at #2542/6190548200.
The unchanged whole-file census measures two affected bodies; neither adds a
boot caller, provider attempt, slot result, schema preparation or admission
fallback. A19 is still read-only selected-store/schema observation; A20 still
requires native possession plus exact generation; A28 still distinguishes
typed read availability from complete execution authority.

| Measured body | Actual owner and direct consumers | Preserved execution proof |
| --- | --- | --- |
| `postgres.(*Backend).NewSessionAuthority` | Existing native session constructor now retains its immutable selected Backend identity, alongside the pre-existing transaction-test slot. Its unchanged production callers are `PipelinePostgresOwner.reservePostgresPipelineClaimConnection`, `conversationForkStore.runPostgresMutation`, and `Backend.observePossessionNative`. Each keeps its original native connection, transaction, cancellation, release/discard and lock owner. `AcquireAdvisoryLockLeaseWith` records the same identity at its existing separate constructor; no caller receives new authority. | Existing native possession observation, retained-session cancellation/exit and inspection escape controls still exercise their original owners. The new both-store native seed interruption and foreign/borrowed/closed-scope controls execute the immutable backend association; neither borrowed nor foreign inspection can acquire a recovery seed. |
| `postgres.(*Backend).InspectRetainedAgentRecovery` | One fixed retained-recovery aperture, solely consumed by `startupownership.postgresSession.ReadAgentRecoverySeed`. It reuses the original lease's `ProveCurrentWithRead` transaction and the unchanged private `inspectTransaction` binding. It refuses nil, closed, foreign and caller-supplied scopes; no SQL transaction escapes. Existing standalone `InspectSnapshot` and all public verify/schema callers are unchanged. | `TestAgentRecoverySeedNativeReadBothStores`, `TestAgentRecoverySeedRefusesBorrowedInspectionBothStores`, `TestAgentRecoverySeedMalformedMemberDoesNotFenceBothStores`, `TestAgentRecoverySeedSourceRefusalCanRecoverBothStores`, and the held release/takeover, cancellation and native-interruption roots prove one same-cut complete read, joined native cleanup, zero partial seed, domain/native error separation, and original terminal fencing. |

P18 additionally retains the actual public fresh SQLite/PostgreSQL verify
observations and `TestAdmissionInspectionRealReadScopeBothStores`: inspection
does not acquire process authority or repair schema. P19 keeps the original
native-generation monitor and exact-session/SQLite-proof controls. P24 keeps
`TestInspectionNativeReadCancellationJoinsTransactionAndDisposes`, native
possession failure/cancellation controls, public observation failure/deadline
controls, and all three `TestAdmissionPredicateRatchetRejects*` hostile mutants.
The seed's domain error is retained separately while the same-cut native tail
proof and settlement still execute; actual read/commit/close/cancel errors are
returned, not hidden or converted into free capacity.

Regenerate only through `SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS=1`; expected
delta is the constructor hash plus one retained-inspection body under the
original whole-file classifications. Preserve every prior owner/row/proof and
the source-currentness and hostile controls. Generation supplies no execution
credit. Exact source-bound receipts belong in the checkpoint audit. No boot,
dispatcher, slot, accepted-tail or global DC-3 gate is opened by this accounting.

## Private Native Write Freshness (A19/A20/A28, P18/P19/P24)

Gate #2269/6192553264 and single-writer reservation #2542/6192598032 add
passive cache revocation only. The original backend constructs the observer
before process binding; its original private PostgreSQL/SQLite retained session
supplies that exact observer to `newProcessCapability`. No selected facade,
boot step, verify repair, provider slot or new SQL authority is introduced.

| Measured body / boundary | Owner and consumers | Execution proof |
| --- | --- | --- |
| `postgres.(*Backend).AgentRecoveryWriteObserver` and SQLite peer | Original backend identity, consumed only by its original native startup session and native boundary tests. Binding preserves pre-existing active writes and refuses a different live process. | `TestAgentRecoveryNativePreBindWriteMustJoinBothStores`, queued pool/mutation-admission controls, and process binding/foreign/takeover controls. |
| `postgres.(*Backend).runTransactionOutcome` and `runAuthorityTransactionWithProof` | The existing pooled and retained native transaction owners begin passive invalidation before pool/session admission; end only after their unchanged rollback/discard/close joins. The observer never takes process `opMu` or retains `p.mu` during native work. | `TestPostgresFreshnessObservationJoinsOriginalCleanup`, `TestPostgresFreshnessObservesPoolWaitBeforeProcessBind`, native cancellation/uncertain-commit exits and generation-monitor controls. |
| SQLite `RunTransactionOutcome` and `runTransactionWithConnectionScope` | Existing logical retry/mutation owner and native connection owner retain their cleanup order, including B's scoped prepare failure. Nested scopes are cache observations, not occupancy. Read-only transactions do not invalidate. | `TestSQLiteFreshnessObservationJoinsOriginalCleanup`, `TestSQLiteFreshnessObservesQueuedWriteBeforeProcessBind`, original transaction exit/cancellation/retry controls. |
| `startupownership.SeedAgentRecovery` publication | Existing retained canonical reader plus original-process mutex; epoch mismatch or any active write refuses publication. A later explicit call reconstructs the entire image, never increments/decrements cached occupancy. | `TestAgentRecoverySeedRevokesAcrossClaimAndSettlementBothStores` (12 held/installed cuts), reclaim/retry/failure/bulk, parent provider settlement, canceled-turn and foreign backend controls; original loss/takeover/full-storage controls remain. |

P18 remains observation-only; P19 still requires native possession and the exact
generation; P24 preserves actual error/ACK and cleanup semantics. Writable
locking proof/source operations conservatively revoke even if unchanged;
genuinely read-only inspections and native proofs do not. Generation of the
census itself provides no execution credit. Native installed seed bytes remain
unobserved; this packet proves revocation and explicit complete reconstruction,
not a live provider/capacity decision or integrated S6 recovery.
