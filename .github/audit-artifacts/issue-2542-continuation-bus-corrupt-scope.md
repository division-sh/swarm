# Cohort 89: corrupt replay scope and exact pipeline receipt

All corrupt-scope startup, bounded-continuation and acknowledged-decision
consumers use the original selected coordinator. A named pipelinepersistence
fault deletes only the exact event's committed scope in one unrevisioned
fixture transaction, requiring exactly one row. Missing/cancelled faults cannot
silently succeed or affect a sibling. This is hostile fixture evidence, not
production scope migration, purge or compatibility machinery.

The receipt projection preserves its exact event/platform/pipeline predicate,
outcome/reason columns and COALESCE default. It neither chooses a latest receipt
nor admits a broader pipeline role, decodes an unrelated failure or masks absence.
Detached evidence is discarded on error; the caller retains its same error
diagnostic and startup/aftermath/decision assertions.

Two whole-function codemods compare every original DELETE/SELECT and exact
owner/context consumption. Three unchanged real journeys run under race on
SQLite/PostgreSQL. Native controls prove original write/read counts, the ordinary
missing-scope cut, retained event and receipt, sibling conservation, wrong-key
rollback, cancellation and closed/missing authority.

This closes this fault/read family, not the complete-event constructor/card/
live-lock and other surrounding fixture debt. Existing broad #2542 approval and
raw_sql_policy govern. No runtime/spec/framework/architecture/tracker change.
