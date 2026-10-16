# Startup abandon fixture authority

Track: FULL, consolidated repair #2698/6205327685. The two original startup
abandon proofs previously stopped at first claim before testing startup ordering.
Their storage fixture had no issued normal execution and used an unrelated
literal source scope for delivery claims.

The existing shared fixture now resolves its complete descriptor before the
native source-aware lifecycle owner hashes/persists it. Empty incidental Config
is explicitly the same canonical {} representation read from storage. After
the complete actor/source set is prepared, the existing agent fixture issues
the generation and normal delivery authority. Managed admission references that
actual authority and source-set revision; publication and claim share the exact
original context. The process capability still releases before the serve boot.

The now-unused literal-scope helper is deleted. Run/session/event identities,
original in-progress claim/session cuts, AbandonActiveRuns flags, no-mutation
controls, startup readiness ordering and final quiescence assertions remain.

RED: pr2698-served-recovery-receiver-original-ledger.jsonl (original missing
admission), pr2698-serve-abandon-native-resolved-actor.log (descriptor/config
round-trip discriminator). GREEN: pr2698-serve-abandon-native-canonical-config.log,
both complete original roots, 2.040s, GOMAXPROCS=2/count=1/non-race as hosted.
The served conversation-fork source fixture independently had only an author
scope, despite running inside an already admitted live runtime. It now consumes
CurrentStartupGrantEvidence from that exact runtime, checks admitted/normal/
source/runtime identity, and carries the actual grant/generation/source-set
receipt into its existing work lease. It never acquires a competing process or
alters the running actor, historical session/turn clocks or public API oracles.
Both stores PASS the complete original TestServedParityHarnessConversationForkLifecycle
root (31.743s), pr2698-served-conversation-native-current-generation.log. Expected
credential refusals are still asserted, not converted to successful execution.

Final-head guards, history composition and preflight remain outstanding.
