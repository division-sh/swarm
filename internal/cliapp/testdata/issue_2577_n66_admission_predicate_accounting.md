# N66 Original Process Attribution

Binding amendment: #2577 comment 6174572914. Governing clauses are
`session_explicit_logout_reservation`, `session_explicit_logout_effect`,
`runtime_process_generation` and managed external-effect authority.

This is an ownership repair, not a hash-only census refresh. Master retired
the startup-authority fact journal in db2d71dde8. Runtime-ID searches and physical
Release are no longer evidence of irreversible withdrawal. They are removed,
not restored, migrated or silently replaced with the current singleton alone.

## Owners And Consumers

| Owner | Exact consumers and observations | Execution proof |
| --- | --- | --- |
| Original runtime generation grant | `serveSessionBootstrapRuntimeSelector` / `serveSessionProcessBinding` -> `completeSessionAttempt` -> native `RuntimeConnectionOptions`; frozen before opening the original SDK occurrence. Pure declaration/incoming selection does not invent this authority. | `TestServeInstalledWhatsAppArtifactSelectsBootstrapWithoutExecutionBothStores`; `TestServeSessionIncomingSelectionConsumesExactCompiledOwners`; `TestServeSessionRuntimeSelectionRejectsUndeclaredCandidate` |
| SDK original attribution | `PrepareSessionLogout` returns operation/revision/account/source/occurrence and the connection's immutable grant binding. It cannot assert a native stamp or adopt a successor grant. | `TestSessionLogoutPreparationCannotAdoptSuccessorGrantBothStores`; `TestWhatsAppRuntimeLogoutPreparationUsesOriginalSDKBothStores` |
| Selected reservation / strict target codec | `prepareTeardownReservationTx` takes the generation fence before principal/operation locks; `FreezeLogoutProcessTx` joins the exact admitted normal grant and source set to the native stamp. Exact replay compares the original preparation and retains the stored stamp. All teardown loaders validate the selected backend. | `TestSessionLogoutReservationFreezesAndFencesBothStores`; `TestSessionLogoutReservationRejectsContradictionsBothStores`; `TestSessionLogoutReservationOverlapsAndRollbackBothStores`; `TestLogoutTargetCodecRejectsMissingOrContradictoryOriginalStamp`; `TestSessionLogoutTargetRequiresConsistentOriginalProcessAttribution` |
| Current native generation | Both-store inspection readers and authorization/launch mutation callers consume `LogoutEffectAuthorityCurrent` / `ProcessAuthorityCurrent`. Mutation callers fence generation before principal/teardown locks. `requireCurrentLogoutDispatch` rechecks that same selected owner before and after the journal launch, then revalidates SDK/caller lifetime. The frozen original stamp cannot borrow successor execution. | `TestSessionLogoutSuccessorFencesOriginalAndPreservesJournalBothStores`; `TestSessionLogoutPreauthorizationRecoveryPreservesCommittedJournalBothStores`; `TestSessionLogoutSuccessorBeforeSDKUnlinkRetainsUncertainJournalBothStores` |
| Successor recovery | `RecoverUnlaunchedChannelLogout` -> `RuntimeAuthorityWithdrawnTx`, exact journal absence and terminal failure share one fenced transaction. Strictly greater generation plus a different native lease is required. Runtime/boot/owner reuse and multiple real successors are valid; release, failed/canceled acquisition, missing/malformed/equal/lower observations and higher-with-same-lease are not. | `TestSessionLogoutPreauthorizationRecoveryRequiresWithdrawnOriginalBothStores`; `TestSessionLogoutSuccessorFencesOriginalAndPreservesJournalBothStores`; `TestRuntimeLogoutWithdrawalRefusesInvalidTemporalEvidenceBothStores`; inherited `TestProcessCapabilityPublicationRequiresAcknowledgedNativeCommitBothStores` cancellation/rollback cells |
| Historical effect journal | `SettleLogoutEffectTx` and the exact journal settlement authority remain historical; they do not require the original process to be live. Recovery never reconnects, sends unlink or deletes pairing. | `TestSessionLogoutPreauthorizationRecoveryPreservesCommittedJournalBothStores`; `TestSessionLogoutSuccessorFencesOriginalAndPreservesJournalBothStores`; `TestWhatsAppLogoutJournalProcessDeathBothStores`; `TestServedWhatsAppLogoutReservationProcessDeathBothStores`; `TestServedWhatsAppLogoutLaunchedProcessDeathBothStores` |

The new census proof families `2577-logout-original-stamp`,
`2577-logout-successor` and `2577-logout-native-process-death` name the executions
above. They refine A10/A27 and their P08/P10/P21 adjacent admission/readback
contracts; they do not add a separate verification authority or claim physical
WhatsApp qualification. The issue checkpoint records executed cells and receipts
separately; merely listing a test here is not a passing receipt.

## Rebase Accounting

The existing #2612/#2627 owner/caller accounting remains authoritative for their
unchanged inherited startup predicates. The source snapshot is regenerated on
the integrated tree because master changed those bodies; no refusal, supported
surface, hostile control, baseline or census policy is removed. The N66 additions
extend the finite census to the actual logout reservation, codec, grant join,
inspection/mutation, recovery and SDK dispatch consumers rather than auditing
only the formerly failing withdrawn-reader helper.

Physical P01-P09, the human-written upstream SDK PR and the retained PostgreSQL
resume RED are still open on #2577. This census refresh waives none of them.

The rebase debt guard also exposed the branch's unsupported-platform duplicate
`InvocationState.Retire`. Join validation and retirement state now have one
common owner; the native build installs only descriptor-relative file disposal.
The unsupported duplicate is deleted, not classified away. Native nil/foreign/
incomplete join and unsafe-entry refusal controls remain unchanged. There is no
debt baseline, census policy or filesystem-cleanup permission expansion.
