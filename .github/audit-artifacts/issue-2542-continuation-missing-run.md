# Native Missing-Run Refusal Cohort

Stacked on a20fb4464, approved increment3 base43f3bed66. One complete #2542
refusal family, no new PR, aggregate tier or production semantic change.

The original prepared instance and missing-run assertion now reach the existing
native activation command using the existing opaque WorkflowActivity fixture.
Its typed unscoped author context is explicitly asserted to have no run identity
BEFORE admission. Existing source/execution facts are retained. No synthetic
transaction context, alternate correlation key or fallback owner is introduced.
The native readiness owner returns its exact missing-run diagnostic; the assertion
pins the complete canonical error rather than the old adapter's partial spelling.

The external bridge exposes the original native owner only through semantic ports.
Its original transaction probe requires zero writes and zero active operations,
and the fixed whole-header observation requires zero physical headers. A false
refusal or a committed row cannot satisfy the proof. The original positive
same-entity/two-run isolation sibling remains unchanged and execution-proven.

| Manifestation | Disposition | Exact executed proof |
| --- | --- | --- |
| Missing-run setup still reconstructing a raw workflow store | reproduced and fixed | TestWorkflowInstanceStore_RequiresRunContext under race on sqlite/postgres; exact native readiness refusal, zero original writes, zero headers |
| Run-scoped sibling ownership bleed | execution-proven through the same corrected path | TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed under race on both stores; original two-run/source_state/fork_state assertions unchanged |
| New root/unlisted sibling raw authority | execution-proven through the same corrected path | Fresh mandatory NativeStorageIdentity child, whole run-scope file plus new external bridge; TestNativeStorageIdentityGuardRejectsRawParametersAndCallbackEvidence explicitly probes root and unlisted sibling |

Both runtime roots PASS12.187s under race, no failures/skips. One finite recipe
preserves UUIDs, supplied instance and refusal workload; independent AST equality
allows only the named owner substitution and exact canonical diagnostic. It also
pins the explicit run-free context assertion and rejects weakened nil/error
checks. Combined95-recipe snapshots/controls and actual overlay type preflight
PASS3.461s; fresh read-only ratchet/registry/shared15-child census plus hostile
controls PASS67.647s. All78 structural guards, partition/envelope and timing
contract PASS before this self-contained commit.

Disclosed failures: supplying an empty WithRunID value to a pre-scoped fixture
was a no-op, correctly caught by actual admitted-header/zero-write checks. Replaced
that mistaken test setup with the existing unscoped native fixture, not a new
context protocol. The actual native error uses "requires run_id" rather than the
old helper's "run_id is required"; the test now checks the complete native
diagnostic. An AST oracle placement error was also refused and corrected. No
production fix, retry, skip, compatibility classifier or timing change was needed.

Debt14757->14754 findings;10952->10950 raw-operation sites. Three/two removed,
ZERO additions/multiplicity growth. Collector,67 excluded uncertainties and12550
registry facts unchanged. The whole run-scope file is now raw-free and protected
by the existing mandatory child, with all15 census children and all six venue/tier
memberships unchanged. Receipts: increment4-run-scope-* under the existing
20261007 local evidence directory, including all failed preparations.

The exact fixture contract is recorded in platform-spec.yaml's selected-runtime
raw-SQL policy. Existing parent/watchlist mapping remains sufficient; no new
architecture smell or handoff owner. This cohort and its run-scope file are
canonicalized, not the remaining #2542/#2151 lifecycle/helper/getter population.
Global zero debt, strict completion guards, SQLite fork deadline and final
integrated full remain owed.
