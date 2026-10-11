# Cohort 73: standalone run joined witnesses

Three bus helpers no longer receive a pool. They consume detached exact run,
agent-delivery/run and completion-candidate facts through the original selected
read coordinator, existing lifecycle owner and closed executable-delivery owner.
Both PostgreSQL journeys use native construction and the original lifecycle
fixture, preserving event origin, source identity, all six platform signals,
internal delivery, pending/running before final receipt, delivered/completed
after it, and the unchanged actual completion executor/terminal-eligibility check.

The three SQL joins retain their exact event and agent predicates, columns,
null defaults and lack of ordering/limiting/active-state filtering. Candidate
readback retains stored source, revision and due time; it neither enumerates a
different candidate nor synthesizes eligibility. The SQLite reader consumes the
existing storage time decoder and refuses missing due time. Read errors return
no partial detached facts. The bounded private operations expose no handles,
query selectors, callbacks or reconstructed coordinator.

Finite snapshots include all three helpers and update both complete original
callers. Query preservation compares each entire SQL statement, allowing only
equivalent UUID/text syntax, not predicate or projection changes. Both-backend
native controls use semantic event/delivery publication and real candidate
request operations; they verify exact run, subscriber, source/revision/time,
one original read per witness, no writes, wrong subscriber/event refusal and
invalid/canceled/closed/missing ownership. The two actual PostgreSQL bus roots
exercise unchanged production publication, final settlement and completion.

This remains the approved #2542 fixture/observation class and existing
selected-runtime raw_sql_policy. No production behavior or spec amendment,
new tracker or compatibility path is needed. Other bus/parent migration and
zero-debt/strict-guard/fork-deadline/integrated-full closure remain open.
