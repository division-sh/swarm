# Native Closed Engine Owner Family

Parent #2542 / #2151; base eefa1ec68. The two historical engine state and
accumulator error roots now execute on SQLite and PostgreSQL. The existing
native selected owner and opaque workflow persistence remain authoritative;
no coordinator reconstruction, replacement store or new fault writer exists.

Each fixture requires the original run before probing, proves its original
persistence is readable, closes that same native owner with no active work,
and independently obtains the failed read. The unchanged state/accumulator
entry point must propagate exactly that error. All original candidate,
module, event, path and payload statements and the original error assertion
remain. Despite their historical mutation-error names, these roots prove
initial-read error propagation, not final mutation failure or rollback.
No event bus or coordinator worker is created by this component fixture.
Native cleanup remains registered and probes require zero writes and active
operations. Earlier native consumers are unchanged.

The entire engine_adapter_test.go source is now raw-free and whole-file
guarded, replacing its per-function inventory. Both new native fixture files
share that guard. Hostile unlisted engine and close-fixture siblings fail;
there is no exemption or new ownership classification. The old raw opener,
closed SQL pool reconstruction and unsupported mutation-cut interpretation
are invalid for this family.

Two finite recipes bring the codemod to 128. Whole-body AST comparison permits
only owner/setup/context substitution and addition of the exact-cause check;
it retains original workloads and error assertions. Removing an original
error assertion fails the independent oracle. A compiled overlay omitting
only the original owner's close fails both roots on both stores with
"closed original native owner remained readable", not a build/timeout error.

Both roots under race pass (11.242s package). The initial compile failure from
the obsolete testutil import and the corrected receipt are retained. Complete
128-recipe/idempotence/hostile and combined candidate-overlay type controls
pass (36.351s). Fresh ratchet, actual 12562-fact registry, shared 16-child native
census and raw-parameter hostile controls pass (80.782s). All 78 structural
roots and partition/envelope/timing/spec API validation pass.

Debt decreases 14655 -> 14647 findings and 10873 -> 10867 raw-operation sites;
zero identities or multiplicity added. The collector and 67 unresolved
excluded-source occurrences remain unchanged. Governing reference:
platform-spec.yaml selected_runtime_store_projection.raw_sql_policy.
workflow_closed_owner_component_fixture. Existing watchlist mapping suffices;
no new issue, compatibility seam, vendoring, tier run or PR. Parent migration,
zero-debt guards, SQLite fork qualification and integrated closure remain open.
Exact-head complexity and native unused analysis remain increment requirements.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment10-closed-*.
