# Cohort 125: scenario setup acknowledgment-loss fixture

Binding: #2542/#2151 bounded exact-storage observations through the ORIGINAL
selected read coordinator. No public operation, scenario lifecycle, idempotency
or source policy changes. Both constructors lose the entire raw tuple/getter.

Existing API fixture still injects the exact postcommit cleanup diagnostic after
the real SetupScenarioEntities owner. First call returns committed run/entity plus
error; replay returns the identical result and never repeats domain setup. The two
readbacks still require run=1, exact run/entity=1, exact setup-writer mutations=3,
and exact resource completion=1, plus complete stored response decoding/identity.

Bounded ScenarioSetupAckStorage returns only those original physical counts and
response bytes in one original read snapshot, with canonical run/entity validation.
UUID columns use exact canonical-text representation to avoid PostgreSQL inferring
one shared run parameter as UUID for UUID columns and TEXT for resource_id.
No table/query/callback/transaction selector escapes. Missing completion, foreign,
nil, raw, invalid, cancelled and closed owners return no partial facts. The native
both-store control persists sibling receipts using the existing API owner and
proves resource isolation and original read commits. Nonzero run/entity/mutation
proof is the unchanged actual both-store API acknowledgment/replay root.

Finite complete-function codemod and hostile source/count/response/owner checks
compare every old physical predicate and preserve all workload/assertion cuts.
Only exact new private observation occurrences receive fixture-2151 disposition;
no guard relaxation. Other scenario fixture families and parent closure remain
open. No production spec change or measured flake-rate improvement is claimed.
