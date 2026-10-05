# Native Contention Observer Repair

Authorized test-only scope: #2353 comment5969435322, absorbed in #2496/#2525.
The first full53a88a60f invocation remains RED: SQLite equal handler selection
exhausts the unchanged5s retry budget. The65-unit plan stops after broad-01;
64 units did not execute. Raw/primary receipts and the original failure remain
archived by source and invocation in swarm-docs.

The fixture previously required the evidence observer to consume native BUSY
and rollback signals and commit the held writer after the retry owner began its
budget. A new deterministic observer-suspended schedule waits for contender
completion before consuming that evidence. With the original release boundary,
both SQLite equal/conflict cases fail under the existing5s budget; PostgreSQL
controls pass. This proves the fixture dependency, not sole causation or a
performance waiver for the original loaded failure.

The corrected schedule commits the held native writer synchronously after the
failed contender's acknowledged native rollback, before returning control to
the existing transaction retry owner. Its rollback and held-commit evidence is
buffered independently of observer scheduling. The contender must complete
before the observer consumes it. No detached cleanup, invented busy error,
test-owned retry loop or serialized-away contention. Native SQLITE_BUSY,
PostgreSQL blocking, equal/conflict outcomes, exact two/one attempts, per-attempt
INSERT/SELECT counts, original fact, rollback and one committed row remain
mandatory. Failure cleanup still cancels, releases and joins owned work.

All five existing canonical-read/rollback/native-contention roots pass managed
race3 on the byte-identical pre-commit repaired inputs,20.291s. Both stores and
the separate native COMMIT-retry control execute. Neither the10s fixture bound,
SQLite busy_timeout1000,5s production retry budget nor policy is changed.
Fresh exact-head complete qualification, CI and final audit remain required;
the earlier full RED and deterministic red-first probe earn no green credit.
