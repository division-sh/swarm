## PLAN / pre-implementation audit: delete the effective-provenance ledger

Read-only baseline `fe9fb54200cff883ce337b377c02e791d6d195bd`. This follows the [user's ruling, relayed by LEAD/orchestrator](https://github.com/division-sh/swarm/issues/2728#issuecomment-6253279779), not an agent product decision. No production/test edits or tests run for this audit. Implementation remains after #2735 and #2741 land and reviewer-d gates this plan; not on #2614.

### Complete direct storage/builder census

20 production files directly reference the ledger. Under `internal/runtime/contracts/`:

| Files | Cut / retained responsibility |
| --- | --- |
| `effective_provenance.go` | Delete types, ledger lookup/copy/builder, eager population, handler/gate lookup and derived event/connection bookkeeping. |
| `pack_platform_provenance.go`, `workflow_contract_policy_rules_provenance.go` | Delete installed-pack/platform/policy/rules traversal and all per-key rows. Retain original artifacts, pack identity, catalog/body admission and literal policy/rules semantics. |
| `workflow_contract_nodes_provenance.go` | Delete recursive position collection. Keep the real `_note` occurrence/whole-document expansion checks and exact source-path formatting where still needed; no exemption broadening. |
| `workflow_contract_types.go`, `tool_schema_entry_value.go` | Delete bundle ledger plus `admissionProvenance` on HandlerRuleEntry, FlowSchemaDocument, SystemNodeContract, EventCatalogEntry, AgentRegistryEntry and ToolSchemaEntry. Presence/semantic fields are not replaced by another provenance map. |
| `workflow_contract_loading.go` | Remove eager `populateEffectiveProvenance` from load; preserve all source, grammar, semantic and expansion admission. |
| `event_catalog_admission.go`, `workflow_contract_agents_value.go`, `workflow_contract_schema_value.go`, `workflow_contract_tools_value.go` | Remove provenance assignments/traversals only; preserve canonical typed projection, defaults, shape/requiredness, source expansion and negative controls. |
| `workflow_contract_nodes_root_value.go`, `workflow_contract_nodes_rules_value.go` | Remove declaration/nested/ordered-row fact maps and transfer loops, not rule lowering, exact positions in existing refusal messages, `_note` handling or semantic checks. |
| `workflow_contract_paths.go`, `tool_input_schema.go` | Remove ledger-map deep copies; preserve all remaining defensive copies and retained source identity. |
| `flow_ingress_alias.go` | Remove compiled alias's provenance sidecar; derive/validate the alias from typed presence, preserving the public alias string. |
| `event_schema_ownership.go` | Replace declaration-location reads only, preserving producer ownership, restatement and conflict refusal. |

The other three direct production files are `internal/runtime/authoringview/view.go` (sole `effective_provenance` JSON emitter), `internal/runtime/bootverify/workflow_expression_diagnostics.go` (handler/gate expression diagnostics), and `internal/runtime/pipeline/workflow_executable_readers.go` (exported provenance type alias to retire). Four dedicated provenance files total 545 lines before retaining their non-ledger annotation/path functions; per-entry assignments/copies are additional deletion.

### Every live ledger reader / replacement

- **Describe:** `authoringview.Build` copies `bundle.EffectiveProvenance().Entries()`. Delete that field/section and installed-catalog rows; no replacement JSON section, filtered ledger, position cache or retained explainability sidecar.
- **Verify:** `authoredExpressionError` uses HandlerValueProvenance; `authoredGateContextError` uses GateContextProvenance. Resolve the same node/event/slot or flow/stage/context field against retained admitted bytes when forming the error. Preserve escaped keys, rule/on_complete positions, alias/merge introduction and existing `at file:line:column` / position-less shapes. Preserve inherited/specialized source-slot ownership rather than guessing from a displayed label.
- **Ingress admission:** `compileFlowIngressAliases` uses `admissionProvenance["ingress.alias"]` to distinguish omitted from empty. Replace this with an optional typed Alias, decoded by `workflow_contract_schema_ingress_value.go`; absent derives, present requires exact nonempty valid text. Existing decode already rejects null/empty; preserve those refusals and never use a side table for presence. No other live admission reader uses the ledger for presence: schema/tool default, timer-id and event optionality map accesses found are provenance-row construction, not semantic authority. Existing real presence flags/optional fields stay.
- **Event-declaration diagnostics:** `eventDeclarationLocation` reads the declaration map for restatement/multiple-producer errors. Supply its existing owner-flow/event identity to the same lookup helper. Preserve mapping-field introduction (declaration key) versus value introduction so multi-line declarations and merged keys keep the current location. Missing retained source stays `unknown source` / file-only, not a new refusal.

### One on-demand retained-byte helper

Proposed shape: a bundle-owned `IntroductionLocation(member, exact key/index path, occurrence) -> (yamlsource.Location, bool)`. Occurrence distinguishes the existing declaration-key diagnostic from a value diagnostic; it is not a second parser/authority. Exact logical member and owner scope come from the existing admitted declaration/flow, not a host-path probe. Node/event keys are exact segments, not split on punctuation; existing SourceSlot key/index syntax remains internal.

Read `SourceArtifact.Entry(label).Bytes()` or the exact matching retained pack inventory `EnvelopeBody`/`ManifestBody`. Parse with an invocation-local `yamlsource.NewStore(yamlsource.Limits{})` (zero retention), walk Document/Value/MappingField APIs, and return introduction location by value. No raw yaml.Node, host reopen, YAML reserialization, global parser/position cache, retained Value, new ledger or ambient fallback. Missing/programmatic/unresolved input returns `found=false`; lookup failure never replaces or prevents the original refusal. No live facts or semantic result are recomputed into authority.

`_embed` is not implemented in this baseline. Do not add embedding in this deletion PR. The [#1745 requirement](https://github.com/division-sh/swarm/issues/1745#issuecomment-6207714184) is superseded only as ruled: when that expansion exists, compute its explanation at describe time from the expansion/retained types source and print one text line (`derived from _embed[i], types.yaml:LINE`), never store per-field positions. Coordinate this requirement with A's expansion owner.

### Scope confirmation needed before code

The broad phrase “nothing records a position upfront” also encounters **separate existing owners**, not this ledger: `CompiledFlowPinProvenance` and FlowInput/OutputEventPin sourceLine/sourceCol (`workflow_contract_connect.go`, `workflow_contract_schema_routing_value.go`, `workflow_contract_types.go`), plus FlowConnect.SourceLine/AuthoredLocation. Pin provenance is consumed by `authoringview` pin views; connection locations feed `compiled_connection_input.go`, `core/pinrouting/connect_route_plan.go`, `routingtopology/topology.go` and bootverify's connect diagnostics. Their coordinate evidence is outside semantic digests, but connect admission has a distinct fail-closed source-location contract. Separately, AgentRegistryEntry.EffectiveFieldSources / EffectiveSourceForField (`workflow_contract_effective.go`) feeds the existing per-agent field `source` readback, not the ledger section; it stores classification, not coordinates.

**Please confirm the boundary for these adjacent owners rather than silently retaining or deleting them:** does this slice leave their independent contracts intact, or does the ruling require their readbacks to use the same on-demand helper too? If removing them changes an independent output/authority contract beyond the ruling, that is a LEAD/user question, not an implementer decision. I have made no edit to them.

Some event-coordinate fixtures directly seed the soon-deleted map (notably `TestConnectedEventSchemaOwnershipRejectsDistinctProducersIndependentOfConnectOrder`). They must use real retained admitted fixture bytes with the same coordinates, keeping ownership/order/refusal assertions. This is fixture wiring, not permission to keep the map or weaken the oracle.

### Normative spec diff / output artifacts

Reword all ten ruled clauses by logical owner (original line anchors below; later anchors shifted on current master):

| Original anchor | Same-PR wording change |
| --- | --- |
| 218, 227 | Pack handoff/consumed law: exact retained bodies/identity and typed values, no composed describe ledger. |
| 465 | Authoritative source: retained-byte on-demand coordinate lookup, never reopened host source. |
| 494-504 | Node provenance: refusal coordinates on demand; retain exact `_note` occurrence and shared expansion budget; delete ledger composition/wire claims. |
| 535 | Schema source admission: same typed projection/expansion bound, no eager provenance projection/ledger. |
| 3335 | Policy/rules: retained admitted source and same expansion/identity owners, not composed rows. |
| 10646 | Tool admission: preserve recursive bounds and alias/merge diagnostics without a stored ledger. |
| 13918 | Ingress alias: immutable compiled value plus optional authored presence; no provenance sidecar dependency. |
| 14535 | Event migration: immutable semantic/schema ownership and retained bytes, not an enumerable provenance ledger. |
| 27878 | Describe JSON: remove effective_provenance; keep structural source identity, graph/routing, diagnostics, own content and literal policy/rules projections. |

The existing coordinate contract at 492 is unchanged. #1745's future display requirement is stated above, not implemented as another stored field.

Ten JSON/graph goldens across barrier, create-minted-key, golden-workload, select-or-create and telegram-agent regenerate through `SWARM_UPDATE_DESCRIBE_CORPUS=1` / `TestReadProofFactoringCompiledDescribe`, then update-OFF comparison. Telegram's current JSON is 34,619 lines; remove its ruled ledger content, not unrelated bundle fields. Remove only the dead normalization branch for that section. Keep `.github/audit-artifacts/issue-2496-describe-baseline-migration.json` and all historical corpus/Before/After evidence immutable; any affected live pin gets an exact reviewed successor, never a historical repin.

### Test-first / invariant and qualification map

**Proof cost: adds 0 roots in lifecycle; extend existing retained-source/coordinate/presence reaches.** No invariant-comment census. Retire tests existing solely to pin the deleted ledger, not repair them to green: `TestBuildExposesComposedNodeAndEventProvenance` and `TestEffectiveProvenanceWireFormat` have no surviving invariant after this ruling. Mixed semantic/retention tests keep their independent assertions; the census finds 26 test files / 72 direct reference sites, not 72 independent proof roots.

Invariant/reach proposed for the retained coordinate roots, for reviewer-d's one-line confirmation before any RED repair: invalid rule/completion expressions identify the exact authored occurrence; aliased/merged event diagnostics identify introduction rather than anchor coordinates; competing producer schemas are refused independent of connect order and name both declaration locations. Keep `TestScalar2556RuleDiagnosticCoordinatesUsePositions` and its nested/escaped-key/inherited-source assertions unchanged. Retained-source tests add host mutation/deletion, exact pack bodies and programmatic/missing-path fallback through existing roots; alias absent/empty/null/wrong-kind controls remain. Extend existing authoring-view checks to require the removed JSON section's absence, not a substitute ledger.

Local is **focused**: affected contracts/bootverify/authoringview/retained-pack roots, both-store retained-source/tool/selected-fork consumers, generated describe comparisons, source/expansion/presence/closed-owner guards and exact-head preflight. No duplicate local tier or wall-clock threshold; race x3 only if the eventual diff changes concurrency (none planned).

Proposed hosted selection: **CI-Tier lifecycle; CI-Units catalog-replay-2-3, codemod-candidate-overlay; Local-Tier focused.** Current real policy/inventory PR-body emission accepts this selection with exactly-one ownership. Path map: contracts admission/clone/source/type removal -> broad, runtime/store and catalog runtime/replay families; describe/diagnostic/presence changes -> catalog-required-verify plus cliapp/served/public proof; removed exported fields/shared test fixtures -> codemod-owner-guards plus full-only candidate-overlay; retained reconstruction -> the two additional replay roots. Lifecycle already includes catalog-runtime/replay-1 on this master; the named additions cover replay 2/3 and overlay without requesting blanket full or changing selection infrastructure. Reviewer-d sets the final selection after the boundary answer.

Landing overlap: #2735 shares the ten goldens, workflow_contract_paths.go, spec and census-related consumers; #2741 shares the spec/generated census. Rebase on their actual delivered heads before implementation/final receipts. #2614 receives no edits and regenerates its goldens on its own rebase. No implementation or hosted dispatch in this checkpoint. Request Gate D, including the adjacent-owner boundary above.
