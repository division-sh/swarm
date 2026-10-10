# Cohort 112: shared external runtime coordinator authority propagation

Binding: original selected-store ownership under approved #2542/#2151 and
platform-spec selected_runtime_store_projection.raw_sql_policy. No new owner.

newExternalRuntimeTestPipelineCoordinator already consumes the selected workflow
roles and original bus. Its SQL parameter is unused. Remove that parameter and
all 20 call arguments across 18 complete functions: artifact callbacks, configured
channel replay/reload/fencing, template construction/outbox/rollback, canonical
node recovery, and Slack/Telegram supported-surface coordinator setup. This stops
the shared recipe from propagating authority it cannot and must not use.

One recovery caller then has no SQL consumer at all: replace its PostgreSQL
opening/admission pair with the native owner and drop the backend SQL return and
SQLite getter. Preserve source, lifecycle, delivery, cleanup order and assertions.
Other callers' still-live physical/fault handles remain counted; no claim of full
caller migration, public parity expansion, or removal by unused-variable disguises.

Finite whole-function recipes plus AST propagation compare every statement other
than the specified parameter/call deletion and exact recovery construction. The
oracle accounts for 18 functions/20 calls and checks actual source; hostile owner,
failure, temporal and assertion cuts cannot normalize away. Focused race proofs
exercise artifact callbacks, both-store configured channel lifecycle, both-store
canonical recovery, and template receipt scope. Candidate type overlay covers all
other callers. Census must decrease, no identities added, all guards unchanged.

Value: confines a high-fanout runtime/admission construction owner and removes its
stale raw argument from the entire measured family. No measured flake-reduction
claim. Fake pipeline protocols, remaining callers' reads/faults, zero debt and
final integrated qualification remain open. No framework/spec change/new tracker.
