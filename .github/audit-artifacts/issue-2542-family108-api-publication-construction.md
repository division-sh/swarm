# Cohort 108: API publication native construction fan-out

Approved #2542/#2151 original selected construction/raw_sql_policy govern. The
original PostgreSQL native fixture is consumed by ten publication roots whose
observations were already moved to canonical selected owners. No new owner/SQL/port.

Measured AST selection: 20 top-level roots have exactly two db identifiers (pool
declaration and selected admission only); 17 are API roots. This cohort closes the
ten publication consumers: bundle-hash admission, receipt/completion uncertainty
replay, durable-ack absence, explicit-run terminality, invalid operator reference,
private flow, missing recipient checker, caller entity refusal and paused queue.
The other four replay and three run/control consumers are next coherent families.

Replace the exposed pool/admission pair with the existing native constructor at
the original opening point. No consumer/context cleanup is allowed to precede it.
Remove the duplicated pool cleanup; source/setup/API/workload/receipt/replay/count
and refusal assertions otherwise remain unchanged. AST inspection confirms no
intervening use of the later selected variable. Existing comments are preserved.

The committed finite codemod extends all ten existing recipes, not duplicate rows.
Its constructor normalization applies only to the exact named migrated roots and
the existing workload comparison still checks the entire remaining body. Separate
actual-source completion controls require precisely one native constructor and
reject exposed/reconstructed/foreign construction; normalization alone cannot
bless a regressed current source. Complete type overlay and hostile controls remain.

Focused race proof runs all ten original PostgreSQL roots; this is native construction
coverage, not a new backend parity claim. Original backend-neutral observation owners
retain their existing independent both-store proof. Census must decrease, zero added
identities; no classifier/permission/guard weakening or hidden callback/generic getter.

Value: finite fan-out closure of native construction after earlier owner-consumption
migration. Confinement/resource-owner value, not measured flake-rate reduction.
No production behavior/spec change/framework/compatibility/architecture split or
new issue. Parent generic getters/protocol debt and final completion remain open.
