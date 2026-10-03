## Focused Pre-Audit Amendment: Learned Registration Versus Fresh Reconnect

**Implementation paused at this seam.** Existing approved G1/G2, M55 cleanup and M56 error-projection changes remain preserved locally. No PR or intermediate push. Request an independent focused disposition, not a wholesale pre-audit or a new issue.

### Observed Execution

The expanded M56 public matrix uses real SQLite/PostgreSQL serve and the standing Telegram protocol double, without manufactured onboarding rows:

1. Complete public connect, authenticated claim, confirmation and retry to `succeeded`.
2. Remove that connection's exact provider credential through the real file-store owner; retain its signing credential.
3. Public reconnect without a provider credential truthfully reserves a `preparing` responsibility and returns `CHANNEL_CREDENTIAL_REQUIRED`.
4. Public `channel.onboarding_retry` supplies a fresh provider credential. The credential writer admits its operation-owned occurrence and the explicit standing target owner admits the pending responsibility.
5. `RefreshChannelActivationCandidates` tries to reconcile the previous learned registration using the old missing provider key, before the fresh prebinding registration can finish. The public retry fails with `platform.internal_failure/unclassified_runtime_error` rather than reaching the fresh claim ceremony.

Both stores reproduced this in `TestChannelSigningFallbackPublicRefusalBothStores/{default_sqlite,explicit_postgres}/previous_current/retry/valid`. A coordinator-controlled SQLite repetition failed in 1.164s after acquiring its slot, confirming this is an assertion/ownership defect rather than a timing waiver question. Separate attempts during host contention stalled in SQLite fsync; those are not closure evidence.

Temporary local diagnostics identified the internal cause: `refresh channel activation candidates: provider registration pair <learned-binding> credential <previous-operation-key> is UNBOUND`. They have been removed from production source. No credential value, receipt or seal needs to be disclosed to prove this case.

### Canonical Owner And Consumer Gap

This refines the already-audited M36 learned/prebinding/renewal family, M39 reconnect handoff and M52 stale learned authority; the chosen class remains unchanged, not a narrow M56-only error patch.

`Runtime.currentStandingCredentialAdmissions` already projects the pending responsibility's complete admitted evidence for the exact target. Its explicit admission consumer can therefore enable the target truthfully. However, `serveapp.resolveServeRegistrationPairs` separately consumes both the retained channel-activation publication and prebinding intents. `ProviderRegistrationController.Reconcile` admits/identifies every pair before its slot-collision resolution. A stale predecessor pair can poison recovery even when the exact fresh pending responsibility has been admitted.

Named path: public retry -> `channelonboarding.Service.driveLocked` -> `serveChannelActivationRefresher.AdmitChannelTarget` -> `RuntimeContextManager.AdmitChannelStandingTarget` -> runtime exact standing credential admission -> `RefreshChannelActivationCandidates` -> `resolveServeRegistrationPairs` -> `ProviderRegistrationController.Reconcile` -> predecessor `admitAndIdentify/admitCredentials` -> UNBOUND refusal.

The credential backstop is correct to refuse that predecessor. It must not become an absence waiver, adopt a replacement key/value, or emit provider effects from stale authority. Merely remapping this error would leave the recovery path blocked.

### Focused Gate Question

Please confirm the bounded owner direction for this exact live reconnect handoff: registration selection must consume the same exact admitted pending responsibility as standing target admission, while preserving predecessor binding/history and the existing slot/effect fences until the fresh ceremony completes. Is this handled at the existing channel-activation/registration selection owner, or must the existing reconciliation owner first retire the stale learned executable publication?

I have not added a target-only override, last-writer preference, credential-presence filter, new registration authority or automatic replay. Choosing between retained operator identity and executable registration authority without a recorded disposition would silently change the lifecycle contract. The fix must use existing owners and keep the one-complete-PR ceiling.

### Required Additional Proof

M57: both-store public connect -> missing/rotated predecessor provider -> reconnect reservation -> fresh retry -> signed fresh claim -> confirmation -> completion -> restart. Independently assert stale predecessor effects never execute, no declaration-key fallback, exact new admissions/authority, retained predecessor identity/history, revision/retirement fences, sibling isolation, no duplicate registration or uncertain-effect replay. Include healthy reconnect and missing signing controls so this cannot be fixed by deleting every learned pair.

Tracker decision: amend this same issue and its gate record before changing registration selection. No separate issue, compatibility layer, migration or framework proposed. The existing two watchlist nodes already cover exact credential/currentness and recovery; refine them only if the independent disposition changes their recorded consumer obligation.

The existing audit must not claim chosen-class elimination or a review-ready PR while M57 remains red. M55's 24 interruption, 36 revision/phase/retirement and six inherited/observed real-store cells now pass; M56's typed mapping and negative classification controls pass, but its expanded positive reconnect case revealed this residual. Final core qualification and the consolidated post-implementation audit remain outstanding.
