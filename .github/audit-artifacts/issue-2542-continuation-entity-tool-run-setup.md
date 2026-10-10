# Selected Entity Tool Run Setup

Parent #2542 / #2151; increment16 after63c7a26e7. Shared seedEntityToolSourceRun
and the inline sparse-generated-tool bootstrap now consume the original selected
run lifecycle/source owner through storetest.MaterializeRun. No SQL getter,
concrete backend classification or reconstructed run coordinator supplies run
authority. Full origin/run/artifact or bundle-hash facts, all author/effect/source
context bindings, scenario import owner and returned context are unchanged.
Missing typed run capability fails closed; no fallback or memory owner is added.

The source-seed helper's eight direct calls are exhaustively inventoried across
seven consumers; the acknowledgment root has separate SQLite and PostgreSQL
calls. The shared harness remains raw for OTHER construction/observation uses,
but its run creation reaches this same corrected helper. All caller bodies are
unchanged. The sparse root changes only run bootstrap; its complete source,
initialization, import, generated tool workload, refusal/unchanged-state and
readback assertions remain. Existing explicit source admission stays in place.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Imported entity contract run setup | reproduced and fixed | TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores, both stores under race. |
| SQLite entity tools | execution-proven through the same corrected path | TestEntityTools_SQLiteBackendNeutralEntityPersistence; original bookkeeping/no-leak, read/write/query/metrics and mutation-count assertions. |
| Structured filter source/run | execution-proven through the same corrected path | TestSQLiteEntityPersistence_MarshalsStructuredFilterValues, original structured payload/filter assertions. |
| Current-entity source/run | execution-proven through the same corrected path | TestRoleScopedEntityTools_SQLiteCurrentEntityPersistence, unchanged generated-context checks. |
| Acknowledged error consumer | execution-proven through the same corrected path | TestSaveEntityFieldAcknowledgedErrorReturnsCommittedToolResponseOnBothStores, both native branches; all original receipt/error/revision assertions. |
| Retired constructor refusal | execution-proven through the same corrected path | TestRetiredCreateEntityCannotMutateEitherStore, both stores and both existing internal-legacy flag values; original no-write checks. |
| Shared harness run bootstrap | execution-proven through the same corrected path | TestEntityTools_HappyPath executes newEntityToolTestHarnessWithBundleAndLegacyAccess and the corrected source seed; the rest of the harness's raw surface is NOT claimed removed. |
| Inline sparse run bootstrap | reproduced and fixed | TestEntitySparseGeneratedToolMutation on both stores under race; complete input/tool/unchanged-state/readback workload retained. |
| Capability/context/fixture regression | reproduced and fixed | Two finite whole-function recipes, complete AST workload and literal-fixture equivalence, changed identity/artifact/context/assertion controls, exact eight-call inventory and raw-parameter hostile guard. |

Focused PASS: eight roots /20 passing records, zero failures/skips, both backend
branches where the original root supports them. No exhaustion of every unrelated
harness workload or integrated matrix is claimed. Recipes153-154 cover both
complete changed functions; no source serialization or additional codemod
framework. The grouped154-recipe/overlay, census/registry/16-children, 78-guard,
partition/spec, exact-head complexity and unused sweep is required before push.

Closure level: complete claimed source-run setup family, not all entity tools or
their remaining raw fixtures. Old raw run calls are invalid within these two
seams. Physical observation SQL, hostile field/bookkeeping setup, raw harness
return handles and unrelated run helpers remain explicit parent debt. Existing
watchlist/tracker mapping remains sufficient; no new issue or production semantic
repair. Parent zero-debt/global guards, SQLite fork-deadline and integrated proof
remain OPEN. No PR or aggregate tier is opened/run for this increment.

Spec: selected_runtime_store_projection.raw_sql_policy.entity_tool_run_fixture.
Receipts: /home/youmew/.cache/swarm-2542-local-20261007/increment16-tool-seed-*.

Grouped census/registry PASS: 14,570 -> 14,564 findings / 10,800 -> 10,794
raw-operation sites, six/six removed, ZERO added/increased identities. All16
mandatory native children and hostile guard controls pass. Collector,67 excluded
uncertainties and12,745 registry facts unchanged. Complete154-recipe/overlay
qualification passes. Final78-guard, partition/spec, exact-head complexity and
definitive unused evidence is recorded with the increment before push.

Validation repair before push: the codemod's existing explicit package list now
includes internal/runtime/tools, so external tool-test candidate overlays are
actually typed. A deliberate int/string type contradiction in that exact external
test file must fail the same checker, while the complete real candidate passes.
The initial negative control expected detailed error text from a checker that
intentionally returns a fixed refusal; it is corrected to assert that fixed type
refusal, with the actual compiler diagnostic retained. No loader error is swallowed,
no source package is excluded and no new scheduling/rewriting framework is added.
The migration code is unchanged; final tooling complexity/unused are requalified.
