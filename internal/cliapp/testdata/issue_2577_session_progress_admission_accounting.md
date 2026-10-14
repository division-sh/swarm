# #2577 Session Progress: A10 / A27 Owner And Consumer Accounting

Binding repair: reviewer-f 6177967097; CLI clarification 6179483377. This is
non-executable progress, not a new public lifecycle phase or admission waiver.
P08/P10/P21 and the prior native-readiness/reuse/write/retirement/reset/logout
families stay mandatory. Their original evidence is retained, not requalified
by a census regeneration.

| Canonical owner | Production consumers and retained boundary | Named execution proof |
| --- | --- | --- |
| channelonboarding.Service Start/Retry, prepareSessionPairing | serveSessionBootstrap BootstrapSession/ResumeSession select the existing counted runtime/process binding once. Private open remains owned; no SDK dial on successful pending handoff, no replacement/account/READY/business authority | TestServeSessionRetryProjectsPendingOwnedConstructionBothStores; TestServeSessionPendingRequestProgressAndExplicitContinuationBothStores; TestServeSessionPendingConstructionFailureRemainsARefusalBothStores |
| serveSessionBootstrap connectSessionAttempt | Fresh explicit Bootstrap/Resume consumes the same prepared connection; live request cancellation still fences SDK dial/handshake. Failed/closed outcomes are refusals, not bootstrap success | TestServeBootstrapOriginalCallerCancellationBothStores; TestServeSessionResumeCancellationRetainsJoinAndPossessionBothStores; TestServeSessionBootstrapAttemptOutcomesBothStores |
| channelonboarding.Service recoverOperation/restoreSessionProgress and serveSessionBootstrap AwaitSessionBootstrap | RestoreSessions and Recover wait for the original private preparation and transport before existing proof/identity recovery ordering. Public Start/Get/Retry do not call that waiter | TestServeSessionStartupAwaitsOriginalPendingPreparationBothStores; TestWhatsAppStartupRestoresBeforeProofWithoutDrivingEffectsBothStores |
| serveSessionBootstrap current/finishedConnection, ObserveSession, ChannelExecution | Current transport checks are distinct from historical ownership; nonblocking pending health supplies no execution authority, closed observations remain absent, observation failures remain errors | TestServeSessionReadinessUsesOriginalOwnedObservationBothStores; TestServePairedSessionObservationCancelsPendingResumeBothStores; TestServeSessionResumePreservesObservationErrorWithoutReconnectBothStores |
| serveSessionBootstrap RetireInactiveSessions plus canonical selected-store TeardownStore | Context retirement fences the exact original reservation and joins its private construction before release. Failed/canceled join retains responsibility; no successor or provider work from retired preparation | TestServeSessionContextRetirementJoinsPendingPreparationBothStores; TestServeSessionTeardownInterruptedJoinRetainsOriginalOwnerBothStores |
| channelonboarding.Service result and Result.SessionPreparationPending | Existing authenticated pairing readback explicitly reports preparing. Empty/unknown fields, QR/account/connected/READY evidence, foreign declaration/operation, terminal history or logout cannot manufacture pending authority | TestOnboardingSelectedProviderRequirementSurvivesReadback; TestChannelCLIRefusesMalformedPendingAndGenuineBlockage; TestNativeResultLateReadbackCancellationDoesNotDisclose |
| Candidate explicit role readback and Result.ProviderCredentialRequirement | CLI Start selects without reading input. CHANNEL_CREDENTIAL_REQUIRED plus exact Get/reservation may authorize hidden input and one exact-operation Retry. Explicit replacement consumes the same validated declaration; empty/missing/contradictory requirements are distinguished, and signing is separate | TestChannelCLISelectedCredentialAdmissionRetainsOperation (webhook and credential-requiring session); TestChannelCLIRejectsInapplicableCredentialBeforeInput; TestChannelCLISelectionRefusalDoesNotReadCredential |
| completeChannelOnboarding and channel_onboarding_progress helpers | Connect returns truthful pending; resume advances once, without a new polling/reconnect policy. Both READY renderers require succeeded/current-ready readback; unchanged non-pending progress, failed/retired/unknown/historically non-ready states retain refusal | TestChannelCLIExplicitSessionPreparationProgress (connect/resume x human/JSON/quiet); TestChannelCLIPendingResumeCanReachGenuineReady; TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors |

`2577-native-pending`, `2577-native-startup-wait` and
`2577-native-cli-progress` map to the respective rows above. These are real
owner/caller executions, not assertions that a shared helper proves closure.
The census now also pins the previously unmeasured service and its new private
progress/result/CLI adapters; existing surface classifications are not removed.

Current deterministic receipts: both-store pending matrix race PASS18.852s;
blocked Retry SQLite18.075487ms / PostgreSQL8.955369ms; startup/cancellation and
context retirement race PASS14.585s; refactored startup race PASS13.384s;
CLI race PASS3.388s; full channelonboarding package race PASS1.448s. The precise
blocked-open RED (both original 5s caller deadlines) remains on #2577 6178823545.
No deadline/assertion, SDK pin, admission policy, baseline or debt allowance changed.

Served QR fixture mapping is approved by 6179981115: the shared process-death
pairing helper, paired RunServe, served process, and unbound-connect fixtures
consume exact-operation Retry only for validated preparation, within the same
absolute original pairing deadline. Errors, terminal/closed/uncertain state or
changed identity/source fail; missing QR alone permits only Get. Every original
claim, confirmation, readiness, journal and no-replay assertion remains.
TestServedSessionPairingContinuationRequiresCanonicalPending covers these
consumer controls on both stores, including expiry without a renewed window.
The single authorized original launched-logout served exposure remains a
separate receipt obligation; this accounting update does not claim it passed.
No supported-provider/served completion, destructive-reset closure, physical
P01-P09, upstream SDK PR or hosted lifecycle qualification claim is made here.
The separate retained PostgreSQL timing RED remains open under #2353.
