# Cohort 135: explicit manager unit delivery configuration

newTestAgentManagerWithOptions created newManagerDeliveryTestStore whenever an
otherwise pure manager fixture omitted delivery persistence. That fallback
opened a hand-authored raw SQLite schema and fake mutation/authority owner.
The default affected 69 direct call references plus the plain constructor's
51 callers; supplied-options/persistence-store roles still take precedence.

The existing production constructor NewAgentManagerWithOptions is the semantic
owner of explicit delivery configuration. The test helper now passes absence
through unchanged rather than silently creating SQL persistence and projecting
its fake runtime authority. Explicit opts.DeliveryStore and existing
ManagerPersistence providers implementing deliverylifecycle.Store retain the
exact original role. No new capture implementation, delegating grant, memory
framework, compatibility fallback or inferred acknowledgment is introduced.

The finite whole-function recipe changes exactly the fallback block. Posture,
work ownership, semantic source, session roles, optional supplied-store role
projection, explicit delivery authority/continuation wiring, topology admission,
hydration and joined shutdown are unchanged. The existing raw-authority guard
now closes this shared constructor and the new pure-unit ownership controls,
including hostile raw parameters/future sibling cases.

An unchanged-source overlay first proved removal against seven actual unit,
source-admission, budget/recovery, authored-mock/restart and safety-pause roots.
Committed focused race proof adds plain/options absence checks and exact explicit
owner identity, plus the existing quiescence failure, cancellation settlement
and failure-envelope persisted-delivery roots. These distinguish configuration
units from real delivery behavior; the embedded unconfigured role in the pointer
identity unit is never used to claim or settle work.

The native logger/gate both-store proofs are separate and are not credited here.
The 26 remaining explicit newManagerDeliveryTestStore call references and that
owner's raw schema, implicit claim-time setup, direct adapter/fake transaction
protocol, selected-execution seed and physical read/fault helpers remain parent
debt. This family removes ambient SQL ownership from otherwise unconfigured
units; it does not claim that explicit fake delivery fixtures are native.

No production runtime policy/spec change, new issue, retry, skip, budget/timeout
or assertion weakening. Existing #2151/#2542 scope/watchlist covers the residual
native delivery-fixture migration. Census, guards, finite/hostile/type proof,
unused and committed-head complexity are required before incremental submission.
