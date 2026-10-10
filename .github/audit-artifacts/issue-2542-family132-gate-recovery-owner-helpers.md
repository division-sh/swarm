# Cohort 132: shared gate lifecycle and receipt helpers

Seven helpers now receive only the original gateRecoverySelectedStore role,
not the gateRecoveryStoreCase containing database and dialect capabilities.
Every currently known caller propagates that original .events role: 38 function
consumers across gate, accumulator, join, receiver, selection, pause and timer
contexts. Whole-function finite snapshots preserve all other caller work and
assertions. Earlier activity/reply recipes are extended, not duplicated.

insertGateRecoveryRun consumes existing RequireRun with the exact original
selected lifecycle owner, context, scenario origin and run identity. Physical
pipeline receipt count consumes CountPipelineEventReceiptStorage; exact success
and quarantine outcome/reason consume ReadExactPipelineReceiptOutcomeReason;
decision obligation status consumes ReadDecisionRouteStatusStorage. Their fixed
event ID, platform/pipeline predicates, COALESCE reason, counts and status are
unchanged. These are existing canonical owners, not new SQL delegates.

Activity requests consume ReadProposedEffectRunExecutionStorage.Requests, retaining
the original exact run/platform.activity_requested predicate. All attempt states
still count through existing ReadActivityAttemptStorage; successful-only count
is not substituted for total attempts. Request identity consumes the bounded
ReadRunNamedEventIdentityStorage and complete canonical event reader. Missing
identity remains an error; ambiguity now refuses rather than selecting a row.

Focused both-store race execution covers all nine original gate roots, then the
typed handoff in frozen-input approval, foreground quarantine/publication forms,
startup recovery, accumulator join/restart and acknowledged cleanup failure,
constructed descendant admission, timer idempotent recovery, selected-rule
readback and pause/continue. Caller propagation itself is a finite AST selector
edit; the oracle checks every unchanged statement, candidate types and hostile
workload/owner changes. No exhaustive unrelated join matrix is claimed.

The native raw-authority guard explicitly closes these seven helper declarations
and adds negative raw-parameter controls. Other gateRecoveryStoreCase consumers,
raw faults, reconstruction/close seams and observation helpers remain parent debt;
the raw-bearing case is not renamed or declared canonical. No production logic,
compatibility path, new abstraction, retry, timeout/budget or tier change.
