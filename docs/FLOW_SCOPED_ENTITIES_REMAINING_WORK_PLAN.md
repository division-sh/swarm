# Flow-Scoped Entities Remaining Work Plan

## Goal

Finish the 1.6 flow-scoped entity cutover by removing the remaining brittle
seams and stabilizing the verification surface.

## Priorities

1. Stabilize the catalog harness.
   - Fix the `driver: bad connection` issue in `cataloge2e`.
   - Make runtime-harness startup and first publish reliable before trusting the
     remaining tier11 verdicts.

2. Remove the read-time subject gate merge seam.
   - Replace opportunistic gate merging across subject rows with a more explicit
     cross-flow visibility model.

3. Formalize cross-flow handoff semantics.
   - Stop relying on “output pin implies parent entity target” as a loose
     heuristic.
   - Route output handoffs through an explicit subject/parent entity model.

4. Simplify persistence ownership.
   - Reduce the `entity_state` plus `flow_instances` projection seam.
   - Keep one clearly authoritative representation for flow-local state and
     subject linkage.

5. Sweep remaining legacy assumptions.
   - Remove leftover `flow_states` and shared-row assumptions from runtime,
     tests, tools, and operator surfaces.

6. Strengthen invariant tests.
   - Child-flow output targets parent/root entity.
   - Cross-flow writes are rejected.
   - Sibling flows do not auto-materialize.
   - Child-scoped gates are visible exactly where intended.
   - `subject_id` propagates across `create_entity` handoffs.

7. Rework tier11 assertions for the final model.
   - Make assertions explicitly subject-aware and flow-aware.
   - Avoid single-entity expectations for multi-flow behavior.

8. Improve operator visibility.
   - Expose subject lineage and per-flow entities cleanly in dashboard/status
     views.

9. Final cleanup pass.
   - Remove dead helpers and temporary seams.
   - Rerun focused runtime suites and full `cataloge2e`.

## Definition of Done

- Catalog harness is reliable.
- No read-time subject gate merge hack remains.
- No heuristic cross-flow entity targeting remains.
- No active pre-1.6 or shared-row assumptions remain in runtime paths.
- Tests and operator tooling clearly reflect the flow-scoped model.
