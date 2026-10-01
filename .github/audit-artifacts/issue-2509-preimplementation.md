# Pre-Implementation Coverage Audit: #2509

## Status And Binding Boundary

Agent-g, 2026-10-01. Audit-only source base:
`e110bfb368369d701d3e7784f418addf02d2f4ec` (merged #2513/current master).
The entire #2509 body/thread was read; there was no independent gate comment.
`IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md` were re-read from docs.
No production code, existing test, fixture or authoritative spec is changed by
this audit. Baseline tests below are not extraction or closure proof.

This is one explicitly lead-batched PR for **two distinct complete admission
families**, not one common validator. Preserve behavior and every existing test;
add and commit characterization before extraction. Use only private helpers in
the existing owners. No new semantic owner, exported framework, compatibility,
schema dialect, credential policy, provider branch or vendor dependency.
The recorded batching/1,500-gross/net-neutral waivers do not waive proof,
semantics, functions below cyclo25 or the #2407 3,000-production-line cap.

**Implementation is not authorized.** #2482 is still OPEN at
`0f27aa11041e573fa05a2128cdccba96b1a5e564`; it changes adjacent channel packs,
contracts and their supported proof. Its current diff does not edit the four
target functions, but that does not waive the merged-baseline condition in
#2509/#2447. Consume its actual merged result, repeat this census/measurement
and characterization against that result, and obtain reviewer-g's independent
recorded gate before coding. A conditional design disposition now is not an
unconditional implementation gate. If integration changes the class, repair and
re-gate it. No work from #2482's candidate is copied or credited as merged.
Open #2514 changes adjacent policy/boot/source consumers, not these target files;
integrate its merged contract rather than freeze obsolete grammar expectations.

## Class Model And Intended Closure

Category: **high-risk behavior-preserving maintenance**. Observed symptom:
admission functions have intertwined branch/error/recursion/status behavior,
making later edits and review difficult. No new production defect is inferred
from their scores or a green baseline. The named functions are entry points,
not the audit boundary.

| Working class, entirely closed by the proposed PR | Immediate parent | Broadest plausible parent / stopping reason |
| --- | --- | --- |
| S: monolithic definition and semantic-value admission within the immutable ToolInputSchema owner | Complete tool-schema admission, exact projection and value acceptance across consumers | #2447 keep-zone decomposition / #2407 R1.4. Lexical syntax, directional assignability, event acceptance and execution lifetime have different named owners; do not rewrite them to lower these scores. |
| N: monolithic normalization of every supported capability-subject kind, including installed/effective trigger admission and derived status | Complete provider capability readback admission and projection | #2447/#2407; broader model/phase architecture remains #2250. Credential observation, immutable pack compilation and delivery/onboarding stay their own owners, including F's #2241/#2482. |

The issue is broad enough for its lead-approved batch; it is not symptom-shaped
runtime triage. No first-slice exception inside S or N is proposed. Chosen-class
closure commitment: eliminate **both entire maintenance classes** in one PR,
with every S/N/Q row execution-proven. Intended closure: failure class
eliminated for these two maintenance classes only, not runtime/corpus/parent
closure. Currently achieved closure: pre-audit submitted, no runtime closure.

Exact independent pinned snapshot reports:

| Callable | File | Cyclo | Cognitive |
| --- | --- | --- | --- |
| validateAdmittedToolInputSchemaActive | internal/runtime/contracts/tool_input_schema_value.go | 69 | 95 |
| validateToolSchemaValue | internal/runtime/contracts/tool_input_schema.go | 55 | 100 |
| normalizeSubject | internal/packs/capability_surface.go | 50 | 74 |
| normalizeProviderTriggerSubject | internal/packs/capability_surface.go | 54 | 75 |

The exact 19,083-callable snapshot has 274 cyclo>=30 / 54>=50 and
586 cognitive>=30 / 193>=50, maxima184/302. Both target families have direct
branch tests, not just AST/spec assertions. Estimated gross production change
900-1,800 lines, low confidence until extraction; actual head accounting must
stay <=3,000, with helper-by-helper invariant justification and separate family
scores. Do not merely move the high score into another helper or modify the
pinned analyzers/admission policy to obtain a pass.

## Exact Governing Context And Spec Dispositions Requested

Authoritative `platform-spec.yaml`, not issue prose or review drafts:

- `tool_model.authored_admission` (9272): shared recursive schema constructor,
  closed keyword/presence/numeric boundary; omission differs from explicit `{}`.
- `tool_model.hitl_channel_pack_interface.owners` and `.contract` (10241):
  immutable ToolInputSchema/ToolSchemaEntry, recursive admission, exact projection,
  eventschema acceptance, directional relations and direct/durable output checks.
- `tool_model.provider_capability_surface` (9716): subject kinds, identity,
  sources, applicability, status rollup, requirements, guarantees and projections.
- `tool_model.provider_trigger_adapters.compiled_admission_authority`,
  `.pack_envelope_source_authority` and `.secret_binding` (10968 onward): exact target,
  accepted pack identity/generation and signing credential authority.
- `engine.boot_verification.structural_validation` (8730) and
  `cli_specification.foundations.output_contract` (verify output contract near
  24405): structural verify/describe do not acquire operational readiness or
  publish retired verify capability_subjects.

Adjacent binding context: #2509's complete two-family/unchanged-test requirements,
#2447's current keep-zone allocation and waivers, #2407's cap and ratchet, merged
#2505 grammar retirement, #2512 strict schema/MCP ingress, and #2513 structural
read/output proof. #2482's final merged contract must constrain the channel half.

Three contradictory/ambiguous prose points require explicit independent
disposition, **not a semantic change inferred by this audit**:

| Gate item | Exact conflict and evidence | Proposed bounded spec plan, subject to approval |
| --- | --- | --- |
| G1 source versus kind | provider_capability_surface.subject.source_values names `channel_outbound_binding`, but OutboundBindingPlan.CapabilitySubject and normalizeSubject canonically produce/require source `channel_binding`. The kind is separately `channel_outbound_binding`. Existing channel readback tests pass with that distinction. | Correct the stale source enum entry to `channel_binding`; retain existing wire bytes and kind, no alias or runtime rename. |
| G2 structural versus live readership | provider_capability_surface.description/status.AVAILABLE still describe verify as operational capability/readiness output. engine.boot_verification.structural_validation and the newer verify output contract explicitly retire that output and credential observation. Existing verify readiness-separation controls pass. | Align those stale prose claims with structural verify/describe, preserving source/pack validation and inventory. Live preflight/doctor/serve keep their existing distinct operational observations. Do not restore retired fields or add a command. |
| G3 base versus evaluated trigger requirement | requirements.trigger_rule says signing requirements have no evaluated tuple/read query, whereas status.rollup_rule explicitly permits canonical evaluated BOUND/UNBOUND tuples. Runtime target evaluation already uses credential owners before NormalizeSubjects. | Clarify that the no-observation rule applies to the immutable base/installed projection; existing evaluated live readback consumes exact-target credential evidence. No global binding inference or new evaluator. |

If those corrections are approved, update the authoritative spec in the
implementation PR together with unchanged-output proofs. If the reviewer finds
a genuine semantic contradiction rather than stale prose, stop and repair the
class/tracker before coding. No spec change is made in this pre-audit.

## Ordered Execution Paths And Gates

**Schema structural path:** authored/generated/remote source -> lexical/JSON
presence admission -> immutable schema constructor -> ToolSchemaEntry/pack
admission -> exact projection/structural compilation -> verify/describe or boot
declaration checks -> provider-visible tool registration. The failing/rejected
schema must never reach indexing, generation hashing or registration.

**Schema execution path:** accepted contract -> exact actor/catalog/permission
selection -> projected input acceptance -> effect dispatch -> mapped provider
output -> admitted output-schema validation -> journal/publication/settlement.
Direct output validation and durable activity output validation call the same
typed schema owner; projected tool/event input acceptance calls eventschema.

**Subject path:** accepted pack/effective source -> kind-specific subject producer
-> installed inventory or compiled exact target/binding -> optional credential
observation for the relevant live surface -> NormalizeSubjects -> cloned sorted
typed readback -> JSON or common human renderer. Standalone doctor is source-free
installed inventory; it is not evidence of effective contract-bound readiness.
Structural verify/describe use admitted declarations, not operational subjects.

| Gate on those paths | Classification | Named proof / owner |
| --- | --- | --- |
| Invocation/config and effective-source admission | Different semantic concept, with proof | Existing W5 verify/describe and channel admission tests; merged #2505/#2512 source semantics remain authoritative. |
| Authored keyword presence, null/alias/document limits; MCP HTTP/stdio JSON token fidelity | Different semantic concept, with proof | W5Tool/W5MCPSchemaIngress tests; tool_schema_admission/yamlsource/MCP discovery already lead into S. Preserve them unchanged. |
| Typed schema definition and direct semantic-value admission | Same chosen class S | S01-S18 below; malformed graphs must fail before projection/registration. |
| Event/projected tool input acceptance and directional assignability | Different semantic concept, with proof | eventschema structured violations and exact schema tests; ToolSchemaAssignable/channel relation controls. Preserve acceptance parity and each owner's existing error ordering, not one shared interpreter rewrite. |
| Exact actor permissions, catalog/source lease and effect/lifecycle settlement | Different semantic concept, with proof | Existing tools/direct/durable and selected-store journal/replay tests plus unchanged golden proof. No lifetime or authorization edit. |
| Subject normalization/status and output projection | Same chosen class N | N01-N18 below, all four kinds and every current producer/reader. |
| Static/managed/signing credential observation | Different semantic concept, with proof | Existing configured-channel and exact-target requirement tests; credentials/managedcredentials remain sole observation owners, N validates tuples. |
| Channel activation, transport/onboarding and physical delivery | Explicitly split / tracked separately | #2241/#2482; candidate execution is not #2509 proof or permission to change this owner. |

## Systematic Owner-Consumption Audit: S

Real semantic owners, not the first local helper: immutable `ToolInputSchema`
constructor/ValidateDefinition/Project in contracts; `ToolInputSchema.Validate`
and recursive `validateToolSchemaValue`; canonicaljson/semanticvalue conversion
and equality; `ToolSchemaEntry` admission; eventschema for canonical projected
runtime acceptance. All production constructor/validator/projector references
and wrapper consumers were searched, not only the two target declarations.

| Consumer family and complete relevant production files | Consumption / disposition |
| --- | --- |
| contracts: tool_input_schema_value.go, tool_input_schema.go, tool_schema_admission.go, tool_schema_entry_value.go | Already consumes the canonical owner. Factor definition and instance branches locally; retain constructor/Project/ValidateDefinition/Validate entry authority. Lexical admission is not refactored. |
| contracts: workflow_contract_tools_value.go, workflow_contract_yaml_schema.go, workflow_contract_activity.go, workflow_contract_paths.go, workflow_contract_types.go, tool_module.go, platform_event_catalog.go | Already consumes admitted schema/ToolSchemaEntry; authored, module, activity and generated catalog schemas get no alternate admission. Preserve absence/default/explicit-any and generated emit rejection. |
| contracts: tool_http_execution.go | Already consumes definition/value owner for static inputs, mapped outputs and compiled results. Preserve direct/durable fail-closed validation before success; execution contract remains untouched. |
| packs: channel.go, channel_generation.go, channel_registration.go | Already consumes definition, value and projection owners across interfaces/operations/context/normalized events/connector output. Preserve generation hashes and all accepted schema bytes. |
| providerconnectors: catalog_generator.go, mock_response_compiler.go, mock_response_plan.go | Already consumes canonical admission/projection/value and inhabitants; invalid mock outputs remain refused before publication. |
| providertriggers: normalized_events.go, providertriggers.go | Already consumes admitted exact schemas; descriptors/catalog fields retain complete recursive shape, not only primitive types. |
| runtime: activity_validation.go; bootverify/workflow_executable_reader_census.go and workflow_expression_checks.go | Already consumes typed tool schemas/projection and declared expression checks; no boot-only copy of S admission is introduced. |
| runtime/tools: registry.go, platform_builtin_catalog.go, role_scoped_entity_tools.go; runtime/mcp/client.go | Already consumes ToolSchemaEntry and exact schema projection; static/native/generated/discovered catalog ingress remains admitted, immutable and publication-atomic. |
| runtime/pipeline/activity_engine.go; runtime/registration/provider_http.go; runtime/tools/executor_http.go and executor.go | Already consumes typed input/output admission and canonical ExecutionTool. Retain connector/channel mapping and selected-store journal/replay proof. |
| contracts/tool_input_schema_relation.go and packs/channel_relation.go | Different semantic concept, with proof: directional containment/path/cardinality, not instance validation. Preserve recursive schemas and existing relation tests; do not combine owners. |
| runtime/eventschema/{registry,validate,inhabit}.go; runtime/tools/{validator,emit}.go; runtime/llm/mock_runtime.go; runtime/scenarioderivation/plan.go | Different projected-acceptance/inhabitation concept, with proof: schema projection comes from admitted owners; eventschema owns structured violations and its existing precedence. ToolInputValidator's existing native-input normalization/pruning and mock/scenario inhabitant behavior stay unchanged. Q/S parity probes exercise these independently, not by asserting they call validateToolSchemaValue. |

No currently known unsupported definition-admission bypass was found. There
are multiple deliberately named acceptance contexts, not permission to invent
one error policy: e.g. typed array value validation checks length before items,
whereas projected eventschema checks items before length. Preserve each context
and compare acceptance, not falsely demand identical multi-error messages.

Old non-authoritative paths/removal candidates: only the monolithic branch
bodies are replaced by private helpers under the same owner. No retired runtime
reader or compatibility shim was found in this chosen class. No deletion credit
is invented for supported lexical/relation/event owners or untouched test code.

## Systematic Owner-Consumption Audit: N

Real semantic owner: `packs.NormalizeSubjects`, typed `Subject`, `Requirement`,
`TriggerAdmission`, admitted PackSource/PackIdentity and the guarantee/remediation
registries. Producers derive body facts; NormalizeSubjects alone validates
identity/applicability/requirements, derives status and canonicalizes order.
RenderSubject/RenderEffectiveTriggerReadiness reconsume that owner. No move of
credential or pack compilation authority is proposed.

| Producer/consumer and relevant production files | Consumption / disposition |
| --- | --- |
| packs/capability_surface.go: NormalizeSubjects, CloneSubjects, normalizeSubject, normalizeProviderTriggerSubject, both renderers, requirement and guarantee validators | Already consumes canonical normalization. Factor common admission, each closed kind and trigger phases locally; preserve cloned input, status derivation and validation order. No new public normalizer or mutable accepted carrier. |
| providertriggers/providertriggers.go and admission.go: accepted manifest subject, InstalledCapabilitySubjects, compiled effective admission subject | Already consumes normalization, exact descriptor and accepted pack identity. Installed and target-scoped effective rows must both survive with distinct IDs. |
| providerconnectors/providerconnectors.go: CapabilitySubjects and installed/effective construction | Already consumes normalization. Installed action/import remediation remains AVAILABLE and never becomes a callable effective tool; effective flow-local/imported readiness retains credential tuples. |
| packs/channel.go: SatisfactionPlan.CapabilitySubject and OutboundBindingPlan.CapabilitySubject | Already consumes normalization. Structurally admitted installed plan versus exact effective binding remain distinct; source/kind discrepancy G1 must be explicitly disposed. |
| runtime/standing_targets.go: base creation, exact-target evaluation, EffectiveTriggerCapabilitySubjects and aggregate evaluation | Already consumes canonical normalization at base and evaluated stages. Existing exact signing/static/managed credential owners supply state; no inferred global binding. |
| runtime/workflow_validation.go | Already consumes compiled subjects for execution admission. Structural purpose does not perform operational readiness; G2 preserves that boundary. |
| runtime/context_manager.go: installed/context base publication, comparison, bundle-scoped projection, BaseCapabilitySubjects and EvaluatedCapabilitySubjects | Already consumes normalization/cloning for revision-scoped snapshots and evaluated readback. No source/lifetime mutation or new registry. |
| serveapp/main.go and serve_lifecycle_presentation.go | Already consumes installed/evaluated subjects and concise trigger renderer. Preserve publication/readiness order, redaction and public refusal; do not edit F's activation owner. |
| cliapp/local_preflight.go, provider_trigger_packs.go, provider_connector_tools.go, channel_packs.go, doctor.go | Already consumes normalized subject append/rollup/rendering and installed inventory. Source-free doctor and source-bound serve/preflight receive only facts available at their actual boundary. |
| cliapp/verify.go and describe.go; runtime authoring/structural readers | Different semantic concept, with proof: admitted effective declarations and pack inventory, not operational subject evaluation. G2 corrects stale prose only; no retired capability_subjects or readiness output returns. |
| credentials / managedcredentials and accepted trigger/connector/channel inventories | Different semantic concept, with proof: keyed deployment observations or immutable admitted body facts. Existing configured-channel/target tests and #2482's final merged proof qualify these prerequisites; normalization does not replace them. |

No separate live subject/status normalizer was found. No consumer is moved to a
new owner: all already consume this one. Old branch bodies are removed, not
retained alongside private helpers; existing public normalization/cloning and
rendering entrances remain authoritative. Supplied contradictory status still
fails; body adapters gain no independent status ownership.

## Manifestations And Exact Planned Proof

Names prefixed `Factoring` below are **planned new characterization tests**, not
existing execution receipts. Commit expected pre-extraction outcomes first on
the merged dependency baseline; rerun unchanged afterward. Existing test names
are retained with their existing assertions, not rewritten to match extraction.
Their exact planned Go test names are `TestFactoringSchemaDefinition`,
`TestFactoringSchemaValue`, `TestFactoringSchemaProjectionIsolation`,
`TestFactoringCapabilitySubject`, `TestFactoringTriggerSubject` and
`TestFactoringCapabilitySubjectAggregateBothStores`; slash suffixes name table
subcases. They are not new runners or independent semantic interpreters.

| Row | Manifestation | Exact planned proof |
| --- | --- | --- |
| S01 | Zero/missing/unsupported definition, nil option, inappropriate constraints and first-error order | FactoringSchemaDefinition/shape-order table plus existing TestValidateToolInputSchemaRejectsMalformedRecursiveSchemas. |
| S02 | Cycle versus shared DAG and depth64 boundary across properties/items/additional schemas | FactoringSchemaDefinition/graph table plus TestValidateToolInputSchemaRejectsCyclesAndExcessiveDepthBeforeProjectionOrClone. Retain active-map defer through all recursive/enum phases. |
| S03 | Finite/safe numeric bounds, negative zero, min/max order and applicability | FactoringSchemaDefinition/numeric table plus W5 shared bounds; exact errors and safe-range edge acceptance. |
| S04 | Nonnegative length/item bounds, integer tokens, min/max relations | FactoringSchemaDefinition/bounds table plus W5 schema and MCP ingress matrices, HTTP and stdio. |
| S05 | Pattern UTF-8/regex, formats and inapplicable string constraints | FactoringSchemaDefinition/string table plus malformed-recursive and direct-format tests; exact projected pattern bytes. |
| S06 | Array items required, recursive item definition and additional-schema applicability | FactoringSchemaDefinition/array table and recursive malformed schemas; exact nested path and order. |
| S07 | Canonical property/required names, duplicates, absent required property, object additionalProperties alternatives | FactoringSchemaDefinition/object table and projection tests; no trim/widen. Multi-invalid map traversal is not assigned a new sorted first-error contract. |
| S08 | Equality property-only scope and declared sibling target after recursive property validation | FactoringSchemaDefinition/equality-order plus TestToolInputSchemaDirectValidationEnforcesFormatAndEqualTo and W5 equality presence matrix. |
| S09 | Explicit empty enum, recursive member validity, semantic duplicates, authored order | FactoringSchemaDefinition/enum table; TestToolInputSchemaRejectsExplicitEmptyEnum and TestToolInputSchemaProjectPreservesAuthoredEnumOrder; exact nested revalidation. |
| S10 | Omitted schema versus explicit Any, constructor/modifier/YAML/map paths | W5ToolAnyVersusOmittedObjectAcrossConsumers, explicit-null/alias/provenance tests and FactoringSchemaDefinition/entrance parity. No new syntax/default. |
| S11 | Exact projection, mutation isolation and generated/registry readback | Existing projection/admitted opaque/connector registry tests plus FactoringSchemaProjectionIsolation; normalized pre/post JSON bytes and generation equality. |
| S12 | Value enum before type, semantic JSON/equality and invalid carriers | FactoringSchemaValue/enum-kind table plus admitted cyclic-JSON tests; valid nested enum and wrong-kind errors. |
| S13 | String type, uuid/date-time, pattern and Unicode-length precedence | FactoringSchemaValue/string-order table plus direct validation and projected structured-violation controls; parity on single-invalid inputs. |
| S14 | Integer/number/boolean/null/Any acceptance and exact bounds | FactoringSchemaValue/scalar table, safe numeric tests and projection parity; no coercion or widened unsafe value. |
| S15 | Array cardinality before items in typed validation; recursive item path | FactoringSchemaValue/array-order table and eventschema's separate retained items-before-length control. Acceptance agrees; context-specific error order stays distinct. |
| S16 | Required before members; known versus schema-valued/open/closed additional keys | FactoringSchemaValue/object-order table plus typed enum/additional-schema and closed tool-input tests; exact error/path. |
| S17 | Optional missing equality source, missing target, same/different semantic siblings | FactoringSchemaValue/equality table plus semantic equalTo tests for both typed/projected owners; no string comparison fallback. |
| S18 | Accepted/rejected schema through compile, register, tool call, direct output and durable output/journal/replay | Existing generated emit/catalog rejection, tools direct/durable HTTP and TestChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStores; Q01-Q03 public paths. |
| N01 | Subject required fields, trimming and unsupported kind before status/output | FactoringCapabilitySubject/identity-order matrix; existing NormalizeSubjects ordering/rejection tests. |
| N02 | Static/managed/import requirement closed kinds/statuses/satisfaction/scope and first-error order | FactoringCapabilitySubject/requirement matrix across every status, empty/partial/contradictory tuples and wrong remediation; no local status policy. |
| N03 | Installed connector AVAILABLE/import requirement versus effective local/imported READY/NOT_READY | Existing NormalizeSubjectsOwnsConnectorReadinessRollup and connector multiplicity tests plus FactoringCapabilitySubject/connector table. |
| N04 | Installed channel plan AVAILABLE with no runtime requirement | Configured-channel test plus FactoringCapabilitySubject/channel-plan valid and wrong source/applicability/requirement cases. |
| N05 | Effective outbound binding READY/NOT_READY and exact channel_binding source | Configured-channel test plus FactoringCapabilitySubject/channel-binding table; Q01/Q02 wire comparison and approved G1 spec assertion. |
| N06 | Supplied status contradiction precedes capability/guarantee normalization | FactoringCapabilitySubject/status-order multi-invalid cases for all four kinds; existing registry failures unchanged. |
| N07 | Capability trimming, unknown-code verbatim output and guarantee enforcement registry | Existing RenderSubjectUsesRegistriesAndPreservesUnknownCapabilityCode / GuaranteeAndRemediationRegistriesFailClosed plus FactoringCapabilitySubject/facts table. |
| N08 | Subject sort/dedup, fact sort, unsatisfied-first requirement ordering | Existing NormalizeSubjectsOrdersDeterministicallyAndRejectsDuplicates plus exact pre/post canonical bytes with reversed input and repeated normalize. |
| N09 | Full nested clone, evidence/headers/scopes/pack/admission isolation; caller unchanged | Existing CloneSubjectsDoesNotExposeNestedCapabilityState plus FactoringCapabilitySubject/readback-isolation and error-path input unchanged. |
| N10 | Trigger event/field required identities, raw/normalized kind, ordering and duplicate event refusal | FactoringTriggerSubject/descriptors table plus TelegramInstalledAndEffectiveSubjectsCarryExactSelectedDescriptors. |
| N11 | Installed trigger source/applicability, no admission, no evaluated requirement | FactoringTriggerSubject/installed table and existing global-readiness refusal. |
| N12 | Effective exact target ID, bundle/alias/provider/catalog/event coordinates and missing admission | Existing NormalizeSubjectsOwnsEffectiveTriggerAdmissionShape plus FactoringTriggerSubject/coordinate table; every malformed coordinate fails. |
| N13 | Authentication and policy-source enums, verified pack identity versus raw no-pack | FactoringTriggerSubject/policy table and typed PackSource/PackIdentity controls; reject foreign/partial identities, retain exact canonical readback. |
| N14 | Authenticated exactly-one target secret, unsigned zero requirements | FactoringTriggerSubject/auth-count-scope table plus current admission/readiness tests; no invented global secret. |
| N15 | Effective wholly unevaluated AVAILABLE base versus evaluated BOUND/UNBOUND READY/NOT_READY | FactoringTriggerSubject/evaluation matrix plus existing exact trigger text/JSON facts tests; approved G3 spec clarification and real evaluator control. |
| N16 | Installed/effective multiplicity across import, context aggregation and target readback | Existing CLI trigger multiplicity, connector inventory/effective and configured-channel tests; FactoringCapabilitySubjectAggregateBothStores through current context/credential owners. |
| N17 | Deterministic redacted human/JSON/concise output and exact registry phrases | Existing EffectiveTriggerTextAndJSONProjectTheSameTypedFacts / RenderEffectiveTriggerReadinessIsConciseAndRedacted plus FactoringCapabilitySubject/output pre-extraction capture. No secret-value output. |
| N18 | Structural verify/describe versus doctor installed inventory and serve evaluated readiness | Existing readiness-separation/W5 readers and Q01-Q03 compiled proof; approved G2 preserves retired-output absence, not operational-output parity on a structural reader. |
| Q01 | Real compiled structural consumers | New TestAdmissionFactoringCompiledStructuralSurfaces in CLI integration, using actual authored current pack/tool sources: verify and describe text/JSON/quiet, valid nested/equality and malformed admission exits. Capture source-path-only normalized bytes before extraction. No fake-RPC or in-process command credit. |
| Q02 | Real public operational presentation and store parity | New TestAdmissionFactoringCompiledCapabilitySurfacesBothStores using existing compiled-process/CLI integration composition: source-free doctor text/JSON installed inventory and source-bound serve readiness/refusal; compare exact subject identity/status tuples and redacted renderer bytes. Use fresh current SQLite/PostgreSQL stores and canonical credential owners, local fixtures, no external provider calls. Doctor --target/schema-inventory are different readers, not effective-subject proof. |
| Q03 | Runtime schema and subject consumer composition | New TestAdmissionFactoringRegisteredToolExecutionBothStores through the existing compiled lifecycle/registered-tool path; existing real direct/durable HTTP output and both-store channel journal/replay tests, new context aggregate table N16, and existing MCP HTTP/stdio discovery admission with nested valid/hostile schemas. Compiled tool invocation input must reach the registered validator, not merely test a standalone schema. |
| Q04 | Unchanged broader supported workload | Existing compiled golden smoke and restart/forced-kill dual-store controls plus both heavy burst iterations in their actual shared test process; preserve workload, deadlines and exact receipts, no performance attribution or parent closure. |
| Q05 | Exact structural integrity/complexity/test preservation | Unchanged API/spec/source/retirement/import guards, independent base/head pinned checker, unchanged-existing-test diff and actual production-line/helper inventory. Update only generated complexity baseline by its canonical command; no suppression/score movement. |
| Q06 | Final-head qualification and complete closure accounting | Default go run ./cmd/swarm-test, separately selected supported proofs, exact-head hosted CI and Post-Implementation Proof Audit with distinct S/N tables; no --full unless requested or skipped backend credited. |

Total planned rows: **18 S + 18 N + 6 Q = 42**. G1-G3 are gate dispositions,
not fabricated runtime manifestations or earned proof. If a public checkpoint
cannot exercise the actual owner as specified, or a characterization detects a
contract contradiction, stop for a corrected gate instead of substituting a
mock helper or silently widening code.

## Parent Probe, Watchlist Promotion And Tracker Decision

Mapped existing nodes: `maintenance-and-cleanup.yaml`'s
`boundary_owned_decomposition` and `invariant_suite_coverage`. They track
oversized owner seams, duplicate paths, lost defer/lifetime boundaries and
issue-local rather than executable proof. This audit refines them with the
two distinct families, dependency hold, exact owner map, G1-G3 and all42 rows.

The existing semantic node
`pack_interface_structural_satisfaction_and_channel_adapter_ownership` was
consulted as active sibling evidence: it names shared recursive schema admission,
exact projection/typed enums, registry immutability, directional cardinality,
trigger generation and capability readback. Those live consumer families are
all included in this census/proof plan; they do not authorize changing their
already-canonical owners or F's onboarding architecture. Its stale structural
verify readership is noted with G2, not copied as permission to resurrect it.

Parent sibling probe: #2510's Claude continuation/resource reads and #2511's
durable-data aggregate admission remain separate owned families; boot composition
and readiness are #2411/#2250 rebuild-zone; channel delivery/activation remains
#2241/#2482. No omitted same-S/N production interpreter was found. These are
not small unowned copies of S/N which could honestly be absorbed now. Parent
action: keep the lead-batched whole-family boundary and leave #2447/#2407 open;
no parent/corpus or broader architecture closure. If #2509 closes, the currently
named maintenance tail is **two child containers / three distinct families**
(#2510 two, #2511 one), low confidence until their own censuses; R1.3/R1.5 and
architecture transfers remain independently open. This is not a two-PR promise
to close #2407 or every historical keep-zone target.

Tracker-state decision: **update current #2509 body before coding** with this
measured scope, full S/N/Q map, G1-G3 and explicit #2482 hold; refine existing
watchlist nodes now. Existing parent ledgers remain correct; post the audit link
to #2447 without rewriting other agents' allocations. No new child/umbrella,
superseded issue, POTENTIAL_ISSUES entry or taxonomy is required. No historical
closure is reopened. The issue label already assigns agent:G.

Architecture feedback: long branch chains inside otherwise correct immutable
owners obscure error precedence and recursion/status invariants. Promote now
via private phase/kind helpers, not another schema/status/lifecycle framework.
Track the bounded work in #2509/existing maintenance nodes; broader phase/type
debt stays #2250 as watchlist context, not new closure-bearing work. Estimated
2-4 engineering days for characterization, extraction, public/dual-store proof
and qualification after a stable dependency; medium-confidence effort, high
review/regression ROI, no performance improvement promised.

Closure feasibility: yes, the two finite closed families can be completed in
one bounded PR after prerequisites/gate. Fixing only the definitions or only
the trigger kind would leave named same-class branches live and is prohibited.
All recursive phases, all subject kinds and every identified consumer must
retain canonical consumption. Stop for another live interpreter, required
semantic/ownership change, missing/exceeded size eligibility, changed merged
dependency, G1-G3 disagreement or unexecutable supported proof.

## Baseline Proof Actually Run (Not Closure)

At e110bfb36, no characterization or extraction implemented:

- Canonical `go run ./cmd/swarm-complexity -base origin/master -head HEAD
  -evidence /tmp/agent-g-2509-preaudit-complexity`: PASS, exact independent
  base=head; head.json/delta.json measured the scores above.
- Focused contracts/packs/providerconnectors/providertriggers admission,
  normalization, W5/schema/enum/projection, channel compilation and accepted
  registry/mock controls: PASS, count1, package times .382/.234/.018/.024s.
- Focused tools/MCP/CLI projected schema, HTTP output, both MCP schema ingress,
  subject multiplicity/configured-channel and readiness-separation controls:
  PASS, count1, .020/.196/4.737s. Existing in-process CLI tests are not credited
  as the planned new compiled-command proof.
- eventschema exact enum/pattern/additional-schema and structured violation /
  stable precedence controls: PASS .005s, count1. The simultaneously selected
  APIspec package matched no tests and earns no API-spec execution credit.
- CLI duplicate-owner/target-binding and spec target-authority/hard-coded
  inventory guard controls: PASS .266s, count1.
- Selected-store channel result journaling/replay and output-schema rejection:
  PASS 1.188s, count1, canonical host PostgreSQL DSN supplied. Separate verbose
  channel journal/replay execution PASS 1.512s: SQLite and PostgreSQL leaves both
  ran and passed, with no skip. No golden or full-suite credit follows.

No complete suite, post-extraction
public journey, fresh characterization, live provider, mutation qualification or
merge approval has run in this phase. Request reviewer-g's independent semantic
gate, with the dependency/spec dispositions explicit, and stop here.
