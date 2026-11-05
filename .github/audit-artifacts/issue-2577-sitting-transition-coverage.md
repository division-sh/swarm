# WhatsApp Sitting Transition Coverage

This is a source-to-proof map, not a PASS ledger. Read it with the exact-head
qualification packet on #2577. A test name, an ancestor receipt, or 47 passing
roots is not a complete phone-journey guarantee. SQLite/PostgreSQL coverage must
be reported from actual child completions, not inferred from the root name.

## Proof Boundaries

- `TestWhatsAppPublicSittingEarlyFlowBothStores` uses the public CLI entry point
  in process, real selected stores, the real SDK, and the existing encrypted
  Noise/service peer. It captures actual CLI QR output before asking the peer to
  pair; it does not install a pre-paired device. Its `--yes` approvals are
  scripted, not interactive-terminal or signal-handling proof.
- `TestWhatsAppReservedPairingCheckpointBothStores` also exercises the actual
  callback and native store owners, but `pairedBootstrapStateFixture` supplies
  retained paired SDK state. That setup is not a phone/server QR handshake.
- The SDK logger completion witness is the pinned SDK's exact-message
  `Handler for %s failed`, after the handler returns false. This alone does not
  distinguish pending from fatal: the public journey also checks connection
  reuse, unchanged operation/capture rows, no receipt, and later successful
  claim/confirmation on the same connection.
- Real-binary unpaired rehearsal and private display/relay rehearsal are
  preparation evidence. They certify neither a physical account nor overnight
  retention. Real provider/device actions require separate consent/readiness.

## Transition And Wait Map

| Sitting state or transition | Named automated cell / control | Scope and remaining gap |
|---|---|---|
| Fresh operation reserves before QR | `TestWhatsAppPublicBootstrapReservesBeforeQRBothStores`; `TestWhatsAppBootstrapRequiresReservationBeforeStateBothStores` | Native reservation/private-state ordering, not a physical scan. |
| QR preparation and display wait | `TestWhatsAppHeldFirstQRPublicCLIProgressBothStores`; `TestChannelConnectWhatsAppRegistersAccountBothStores`; new public early-flow root | CLI progress, actual rendered QR and SDK-peer handshake. Private relay visibility is a separate rehearsed gate. |
| Message callback during unpaired QR wait | `TestWhatsAppReservedPairingCheckpointBothStores`, `QR wait` assertion | Synthetic SDK event through the actual callback: false/no ACK, no capture/failure/operation mutation or admission. Not encrypted phone traffic before pairing. |
| QR expiry and bounded replacement | `TestWhatsAppDirectQRActualExpiryAndBoundedReplacement`; `TestWhatsAppDirectQRADVRotationPreservesScopeAndExpiry` | Owner expiry/scope tests. A real phone refusing an expired QR remains physical proof; it was not observed in the stopped sitting. |
| Signed pairing, stream end, authenticated successor | `TestChannelConnectWhatsAppRegistersAccountBothStores` | External peer performs signed pairing and post-login; tests the pending checkpoint before stream end, both serve-stopped/running. Not a real WhatsApp server. |
| Linked but unapproved account preview | new public early-flow root; `TestWhatsAppReservedPairingCheckpointBothStores` | Must preserve the original operation/account and no execution authority. This was the actual field-failure cut. |
| Inbound while preview approval waits | new public early-flow root, `preview`; native checkpoint callback | Encrypted service-peer input and callback completion before CLI approval; no ACK/capture/operation mutation, reusable same connection. Native callback additionally checks no fatal failure/fence. |
| Explicit account approval | new public early-flow root; native checkpoint `comms`/`hitl` cells | Public CLI scripted approval and exact native admission. Actual interactive input and its cancellation remain a separate gap below. |
| Reject the previewed account | native checkpoint `reject_comms`/`reject_hitl` cells | Selected-store refusal/unlink controls, using retained SDK-state fixture. Not the full interactive CLI rejection composition. |
| Operator challenge wait | new public early-flow root, `challenge`; `TestWhatsAppOrdinarySetupTextCannotPoisonClaimRecoveryBothStores` | Ordinary encrypted traffic cannot claim the operator or poison the pending connection; exact emitted challenge is used. |
| Inbound while challenge waits | new public early-flow root, `challenge` | False/no ACK, unchanged operation/capture rows, same connection still reusable. No business holder is installed during this standalone enrollment. |
| Genuine operator claim | new public early-flow root; `TestWhatsAppRuntimeClaimSettlesBeforeAcknowledgementBothStores`; `TestWhatsAppRuntimeClaimDoesNotAutoBindFirstSenderBothStores` | Real SDK decrypt/claim with exact synthetic sender and durable-before-ACK controls; not phone-to-ID stability proof. |
| Claimed identity awaiting confirmation | new public early-flow root, `confirmation` | CLI output hold before scripted confirmation; exact operation remains the enrollment owner. |
| Ordinary inbound while claimant confirmation waits | new public early-flow root, `confirmation` | False/no ACK and unchanged capture/operation; subsequent confirmation still succeeds on the original connection. |
| Confirmed deployment registration | new public early-flow root; `TestDeploymentChannelRegistrationWithoutRuntimeBothStores` | Public account readback and exact bound operator; deployment registration is not business/Inbox READY. |
| Authored source takes the account and replies | new public early-flow root via `requireOfflinePairedWhatsAppServiceRoundTrip`; `TestRunServeWhatsAppAuthoredCustomerReplyBothStores` | Actual SDK service-peer ingress/outbound and native activity request/result conservation. No LLM substitute for native activity. |
| Two independent customer chats | public service round-trip's two independent sender cells | Correct encrypted destination/text and distinct reply effects after durable input receipts. Real phone histories, exact IDs and audience observation remain physical. |
| Ordinary stop/restart with retained pairing | public early-flow/service round-trip; `TestWhatsAppSessionStateRealSDKRestartRetainsExactAccount`; `TestServeSessionExplicitResumeRetainsPairedIdentityBothStores` | Retained exact account/operation, no QR, continued authored replies. Today's stopped preview's particular pairing is not assumed reusable. |
| Pending resume/login observations | `TestServeSessionBootstrapPendingAttemptRetainsOwnershipBothStores`; `TestServePairedSessionObservationCancelsPendingResumeBothStores`; service round-trip's held login response | Same owned attempt and truthful pending state. A new inbound message at every login-response wait is not one executed end-to-end composition. |
| Process death / recovery | `TestServedWhatsAppLaunchedProcessDeathBothStores`; `TestServedWhatsAppUnpairedSDKProcessRestartBothStores`; `TestWhatsAppPublicRecoveryRestoresOriginalPairedResponsibilityBothStores` | Existing subprocess/native recovery controls. Not a physical overnight/device reconnect. |
| Disconnect and explicit resume | `TestWhatsAppPublicResumeRestoresOriginalPairedResponsibilityBothStores`; `TestServeSessionResumeCancellationRetainsJoinAndPossessionBothStores`; `TestServeSessionResumePreservesObservationErrorWithoutReconnectBothStores` | Native/served ownership, observation and cancellation controls. Actual network socket cut and phone-visible state remain sitting observations. |
| Public unbind | `TestRunServeWhatsAppSignedPairingBothStores`; `TestRunServeWhatsAppConnectAfterUnbindResetBothStores` | Withdraw old execution authority, join receiver, preserve pairing and explicitly reconnect. Not automatic authority by retained pairing. |
| Inbound against withdrawn/unbound authority | `TestWhatsAppRuntimeIncomingAuthorityFencesBothStores` (`unbind`, `retired_activation`, `runtime_fence`) | Refusal/no ACK/no publication controls. Incoming traffic at every exact public unbind RPC scheduling point is not one composed journey. |
| Retained-source reset | `TestRunServeWhatsAppPublicResetRetainsPairingBothStores/retained_source`; connect-after-reset root | Real reset owners preserve paired state while withdrawing old execution authority. |
| Cleared-source reset | same root `/cleared_source`; connect-after-reset root | Source retirement and explicit fresh activation, no private session copying. |
| Incoming during reset/receiver retirement | authority-fence and callback join/cancellation controls in `internal/sessionprovider` | Shared fencing controls, not a complete public reset-RPC plus concurrent-message schedule. This limitation must remain explicit. |
| Logout reservation, SDK unlink, local completion | `TestRunServeWhatsAppPublicLogoutBothStores`; `TestServedWhatsAppLogoutReservationProcessDeathBothStores`; `TestServedWhatsAppLogoutLaunchedProcessDeathBothStores` | Original native effect, checked uncertainty/recovery and retained pairing on incomplete cleanup; not local deletion presented as provider unlink. |
| In-flight work while logout drains | `TestSessionLogoutJournalDrainsAndCancelsWaitBothStores`; `TestSessionLogoutRequiresAcceptedProcessTailBeforeAuthorizationBothStores`; callback join controls | Native admission/accepted-tail fences. Not every public interactive logout scheduling point with a phone sender. |
| Final re-pair and next-day messages | same pairing/claim/restart source paths above | Next-day elapsed time, real linked-device list and final provider unlink remain physical-only and NOT RUN until observed. |

## Readiness Gaps That Must Not Be Hidden

1. The interactive confirmation reader blocked on SIGINT in the stopped
   sitting. Scripted `--yes` tests do not close that gap. Establish separately
   reviewed safe handling before invitation; do not expand this callback repair
   into an unreviewed prompt/lifetime change or ask the user to recover it live.
2. Public interruption at the unapproved interactive prompt and all concurrent
   inbound schedules during login, reset and logout are not a single composed
   end-to-end test. Existing native controls have the narrower scopes above.
   Reviewer-f owns the final prepared-journey gate and disposition of these gaps.
3. The actual display/relay must be rehearsed offline, including visibility to
   the operator. Use only the current invocation's complete unexpired frame for
   a live scan; expired captured images are rehearsal/refusal-only. No fallback
   from incomplete capture and no identities/QR/session material on GitHub.
4. Real device/WhatsApp-server handshake, delivered-ID stability across supported
   text/quote/edit/revoke/reconnect, physical unlink, and next-day retention
   cannot be certified by a peer double. No PN/LID inference or normalization.

The personal and fourth-account customers remain sender-only and are never
linked/reset/cleanup targets. Steps 4-6/Inbox/group/notice remain deferred, beta
has no automatic takeover, and the original PostgreSQL resume timeout retains
its accepted recurrence rule. No new invitation or SITTING READY follows from
this table without an explicit exact-head readiness gate.
