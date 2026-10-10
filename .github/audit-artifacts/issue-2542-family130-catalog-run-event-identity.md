# Cohort 130: catalog run/name identity observations

Owner: ReadRunNamedEventIdentityStorageForTest in the existing private native
observation boundary, consuming the original selected read coordinator. Its
public port exposes only canonical run identity and an exact event name; no raw
connection, transaction, query, table name or SQL callback escapes. The fixed
physical predicate is unchanged: events.run_id plus events.event_name. Absent
rows retain sql.ErrNoRows and zero result for existing polling. Multiple matches
now fail closed rather than choosing an arbitrary physical row.

All 11 currently known copies of this exact catalog query move to the shared
runtimeHarness read bridge and canonical port: historical guard fork, rules
selected actor, pending root input, downstream preparation, pending receiver
restart, ordinary receiver dependency, sibling/nested geometry, source-local
agent control, child-to-root, and both source/fork reads in selected receiver
execution. Ten finite whole-function recipes preserve every other statement,
including exact source/child run parameters, event names, polling deadlines and
sleep intervals, publication/claim/restart cuts and original assertions. The two
selected receiver roots share the same migrated helper. No Tier 12 backend scope
or unsupported-capability contract changes.

Native owner proof on both stores requires exact physical identity despite a
same-name sibling run and other-name event; one original read commit and no
writes/active transactions. It rejects ambiguous matches, absent run/name,
invalid identity/name, raw/nil/uninitialized owner, cancellation and closed
stores with no partial identity. Real affected catalog roots run under race,
including restarted pending work, receiver dependencies, selected fork execution
and original-source exclusion. This is not a claim that the entire catalog
harness is native: its raw opening/reopen and unrelated observation/fault seams
remain parent debt. No SQL bypass, guard waiver, retry, timeout or budget change.
