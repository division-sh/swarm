# Cohort 95: native PostgreSQL run-read lock and retirement causality

Both live origin-read journeys and their three observer helpers now consume fixed
runlifecycle SQL through the original selected coordinator. The opaque named
PostgreSQL barrier owns a writer transaction, reports readiness only after the
ACCESS EXCLUSIVE run-table lock succeeds, and joins rollback/connection cleanup
before Close returns. No SQL handle, caller callback or reconstructed coordinator
escapes. Marker-only release is success; joined cleanup errors are never masked.

The exact current-database/other-PID/Lock predicates and truncated run-header
prefix are unchanged. Unrelated-only, origin-only and overlapping reader cases
retain their independent counts. The same five-second/one-millisecond observation
cuts remain. A five-second SET LOCAL lock_timeout bounds only acquisition of the
named hostile fixture lock; no runtime query, assertion, package or CI timeout is
raised. These are explicitly PostgreSQL probes; SQLite refuses these fixture ports.

The actual continuation cut still proves an admitted origin query holds a runtime
occurrence, cannot be cancelled by retirement, and drains before owner release;
the unowned control still requires native PostgreSQL 57014. Every no-mutation,
no-carrier-after-retirement and active-lease assertion remains. Failure cleanup
starts the existing coordinator fence, releases the physical barrier and reader,
then joins through the existing generation/process close owner. Partial setup
records each acquired owner immediately. No independent shutdown owner is added.

Five finite whole-function recipes pin every original cut and assertion. Native
controls prove one live original writer, independent original metadata reads,
one joined rollback/no commits, idempotent close, cancelled/closed/missing refusal
and SQLite non-exposure. A deliberately failing overlay challenges cleanup while
the real origin SQL is blocked, requiring joined teardown in both owned/unowned
cases rather than a timeout or a fixture-only mock.

Existing broad #2542 approval explicitly permits named live faults through the
exact selected coordinator. No production runtime/spec, capability compatibility,
new framework or architecture split. Card/construction and other remaining debt
and the final parent completion obligations remain open.
