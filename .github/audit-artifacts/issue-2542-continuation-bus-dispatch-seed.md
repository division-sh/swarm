# Cohort 79: complete-event dispatch run seeds

The complete-event fixture already retains the exact native selected store.
Its shared run-seed helpers and all four callers now consume that store's
existing lifecycle/completion fixture role, not its pool plus a backend label.
The local store interface declares the required role; no reconstructed store,
new fixture framework, raw accessor or compatibility path is introduced.

The finite seven-function codemod permits only owner propagation and replacing
the old dialect-specific fixture calls with RequireRun. Run IDs, origins,
timestamps, default canonical artifacts and source hashes stay unchanged.
Standing-service reconciliation stays on its existing production path.
Foreground/recovery exclusion, unrelated-run progress, targeted continuation,
decision-route fairness and mixed paused/running assertions are preserved.
Four affected roots execute under race on both SQLite and PostgreSQL.

This is a run-seed family, not closure of the complete-event fixture. Its pool
construction, corruption, decision-card fault, storage observations and live
lock barriers remain counted debt until individually migrated. No raw handle
is inferred from the new owner or exposed through the lifecycle fixture role.
The broad approved #2542 boundary and selected runtime raw_sql_policy govern;
there is no runtime semantics/spec change or new tracking decision.
