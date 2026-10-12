# Pre-Implementation Coverage Audit: Provider JSON-Schema Subset Family

Agent-g, 2026-10-12. Integrated source `730c17b988303b84c1a82bb6345fa25d600784d8`
(actual Go1.26 master after2622); separate worktree/branch from pending PR2626. **Gate requested,
not approved. No production/test/spec edits.** A PR2626 finding takes priority.
The original intake source wascc369e5eb5.2622 actually merged; this audit-only
branch is rebased, not speculatively stacked. Target provider-schema blob is
unchanged across both masters and PR2626. Old Go1.25 controls remain historical
baseline evidence, not new Go1.26 execution credit.

## Binding Context And Proposed Class

Read2447/2407 issue bodies/threads, their current first-slice/parent dispositions,
IMPLEMENTER_GUIDELINES and SEMANTIC_DRIFT. No exact spec section prescribes this
helper's JSON-map dialect, nil behavior or private phases. Binding maintenance
contract: preserve the existing admitted behavior, branch/error ordering,
provider payload bytes and tests. Adjacent authoritative constraints reread:

- engine.agent_session_management.llm_provider_adapter_contract.required_contract_dimensions.tool_schema
  (platform-spec.yaml7600): translate and validate provider-visible input before
  provider execution; provider results are a different normalization dimension.
- engine.persistence_model.role_scoped_entity_tools.generated_schema_closure
  (2687), tool_model.agent_tool_filtering.generated_schema_closure (10502), and
  engine.boot_verification checks.generated_tool_schema_closure (9867): generated
  closure/required/enum laws and the static-versus-turn inventory distinction.
- The2509 audit explicitly split llm/provider_tool_schema.go from immutable
  ToolInputSchema definition/value and capability normalization. Responses SSE's
  independently recorded contract question stays separate and untouched.

Category: high-risk behavior-preserving maintenance, not a runtime/parity bug fix.
Observed symptom: one162-line provider JSON-subset family has a29/52 recursive
entry, mixing container/type admission and eight ordered recursive constraints.
No execution failure is inferred from that score.

Chosen working class: **monolithic provider JSON-schema subset admission in its
existing owner**. Complete family: ValidateProviderToolSchema,
ValidateProviderToolDefinitions, validateProviderSchemaNode,
validateProviderSchemaType and validateProviderRequired. Exact current
cyclo/cognitive:3/2,3/3,29/52,9/15,5/6 respectively. The target blob is identical
on master and PR2626; the independent pinned analyzer receipt binds that blob.

Broadest semantic parent concept: **provider-facing schema validity before
provider execution**, including systematic admission by every applicable adapter.
Tracker map:2447 owns this maintenance class;2407 owns regression/merge proof;
2349 owns the cross-cutting type-model debt;2627 owns the confirmed Anthropic
validation bypass.2250 explicitly excludes schema/variant validators and is NOT
a semantic parent or closure tracker for this family. Framing is acceptable only
as a behavior-preserving first slice of provider-facing schema validity. The
29-score helper was an entry point, not the audit
boundary; all five functions, direct consumers and sibling interpretations were
inspected. Intended closure: eliminate the chosen monolithic maintenance class,
not universal JSON Schema support or all-backend schema-admission closure.

## Existing Owners And Systematic Consumption

No new semantic owner, shared validator, exported type or schema dialect.
Private extraction must remain inside provider_tool_schema.go. No PR2626 file
is touched. Constructor, transport, fork authority, storage and generation policy
remain unchanged.

| Consumer / sibling seam | Owner consumption and named proof |
| --- | --- |
| tools/emit_runtime.go ValidateGeneratedEmitToolSchemasForSource | Already consumes ValidateProviderToolSchema after canonical actor/event admission and generated closing. Existing precision-ref, unresolved-type and atomic-publication tests plus new P22/C01 controls. |
| tools/generated_schema_closure.go validateGeneratedToolDefinitionSchema | Already consumes provider subset before its independent closed/generated law. Existing GeneratedToolSchemaClosureRejectsUndeclaredRequiredAndInvalidEnums and boot mapping; P21/C01 distinguish the two laws. |
| llm/openai_compatible_runtime.go buildRequest | Already consumes schema entry for each definition before returning an invocation request. New C02 exact schema/error/first-definition/no-send control; existing canonical execution-frame HTTP proof. |
| llm/openai_responses_runtime.go buildRequest | Same canonical subset consumer, separate Responses JSON request projection. New C03 exact request/refusal/no-send control and existing frame proof; no SSE credit. |
| llm/mock_runtime.go StartSession | Already consumes definition-list owner after actor admission and before startMemory. New C04 failure-order/no-worker control plus real compiled host MCP proof. |
| llm/fork_chat_transport.go newForkChatToolDispatch | Already consumes list owner after exact fork/actor/executor validation and before freezing JSON. New C05 malformed/frozen-definition controls; retain existing exact authority/foreign-coordinate test. |
| contracts ToolInputSchema definition/value, ToolSchemaEntry, directional subset | Different semantic concepts: immutable typed definition/instance/entry/relation, not delivered JSON-map syntax.2509 matrix and unchanged canonical schema/relation tests; no factoring here. |
| tools validator and runtime/eventschema acceptance | Different concept: payload inhabitance, not validation of the delivered schema document. Existing nested typed-enum and runtime extras-refusal proofs; no shared owner introduced. |
| Anthropic buildRequest/continueSession | **Still bypasses the canonical validator; explicitly split to2627.** Reviewer6117588660 proves the builder accepts the exact unsupported type rejected by ValidateProviderToolSchema, despite its ValidatesInputSchemas claim. No public-route or provider-call proof; upstream sufficiency is NOT inferred. Maintenance must not add Anthropic validation without2627's own gate. |
| Claude MCP catalog/managed-capability admission | **Different transport law**, with concrete owner evidence: projectClaudeInvocationTools in claude_invocation_tools.go selects native/MCP definitions through exact managed capability binding; Gateway.mcpToolsForCapabilitySurface/acquireToolDefinitionsInContext publishes the admitted actor/fork catalog. TestClaudeToolArgumentsProjectExactManagedSurface and TestBuildMCPHTTPBindingRequiresConstructionProvenanceBeforeRegistration prove that projection/authority boundary, not another local JSON-subset interpreter. No blanket syntax-admission credit or MCP transport change. |

Full relevant path: authored/imported source admission -> typed registry/event
lowering -> generated closed-schema check and provider-subset check -> actor/tool
selection -> provider request construction or Mock/fork startup -> exact execution
authority -> transport -> settlement. The subset check is the same chosen class;
typed source, generated closure, actor/fork/effect authority and settlement are
different concepts with the named controls above. The Anthropic adapter gate is
the same broad schema-validity concept and currently bypassed;2627 must audit
supported reachability and own its repair. This proposed refactor changes no gate
placement and does not claim that bypass absent.

## Characterization And Manifestation Plan

Commit new characterization BEFORE extraction, then keep every existing test
unchanged. Proposed new root TestProviderSchemaFactoringCharacterization uses
these exact subtests. These are planned proofs, not assertions of execution:

| Row | Manifestation | Exact planned subtest/oracle |
| --- | --- | --- |
| P01 | Name trimming/default and error wrapping | name-and-wrapper: exact full error string, blank/default name and surrounding whitespace |
| P02 | Nil and typed-nil object distinction | nil-schema: nil root accepted, typed nil map accepted, non-map nil-shaped values refused |
| P03 | Non-object schema | object-shape: exact %T and full nested path |
| P04 | Seven types and trim | supported-types: all accepted JSON types, input bytes unchanged |
| P05 | Unsupported/lowerable type spelling | unsupported-type: exact error for unknown/precision contract references |
| P06 | Any-array type union | type-any-array: recursive order/index, wrong element type and empty union behavior |
| P07 | String-array type union | type-string-array: exact supported/unsupported/index/empty behavior |
| P08 | Properties container shape | properties-container: map required, exact refusal precedence |
| P09 | Sorted recursive property traversal | property-order: two invalid names, deterministic first lexicographic error; nested nil unchanged |
| P10 | Items recursion | items: nil acceptance, non-map refusal and exact nested type error |
| P11 | propertyNames recursion and nil asymmetry | property-names: nil refused, exact object/nested-type paths |
| P12 | oneOf container and empty branches | oneof-container: exact array/type/empty refusal |
| P13 | Ordered oneOf child recursion | oneof-branches: nil child, non-object child, nested error and branch-index order |
| P14 | Boolean additionalProperties | additional-bool: true/false unchanged |
| P15 | Schema-valued additionalProperties | additional-schema: nested type/required error and typed-nil map behavior |
| P16 | Invalid additionalProperties | additional-shape: exact unsupported representation %T |
| P17 | Required string/any arrays | required-arrays: accepted forms, empty arrays and no new content constraint |
| P18 | Required bad element/container | required-error: first invalid index and exact message |
| P19 | Enum representation | enum-shape: []any including empty is syntax-admitted; []string/scalar is not silently converted |
| P20 | Mixed-invalid branch precedence | constraint-order: type -> properties -> items -> propertyNames -> oneOf -> additionalProperties -> required -> enum |
| P21 | Unknown keywords vs stricter generated law | dialect-boundary: ignored metadata and syntax-valid empty enum/open object contrasted with generated-law refusal |
| P22 | Nonmutation and alias/repeated use | input-unchanged: exact schema JSON before/after valid and invalid admission; same child reused across paths |
| P23 | Definition-list order | definition-order: exact first failing tool and no mutation of name/schema |
| C01 | Generated/static consumers | Existing emitter precision/unresolved/closure/boot roots plus a mutation making property/oneOf/type walks bypass their precise failing assertions |
| C02 | Compatible provider constructor | New TestProviderSchemaFactoringConsumers/compatible; exact parameter bytes and malformed-schema refusal before HTTP send |
| C03 | Responses constructor | Same new root/responses; same oracle, JSON transport only, no SSE claim |
| C04 | Mock list admission | Same root/mock; actor gate remains earlier, schema refusal before memory/worker invocation |
| C05 | Frozen fork-chat admission | Same root/forkchat; exact authority remains earlier, invalid schema refuses before freezing; existing freeze/foreign actor tests unchanged |
| Q01 | Real public supported surface | TestWorkspaceMCPCompiledHostConformance runs actual compiled swarm test, real host worker, HTTP MCP/read/emit and settlement, private SQLite only. Separately run named SQLite/PostgreSQL generated-schema/runtime-tool tests through their existing selected-store fixtures; those are component/store proofs, NOT a second compiled public backend. Direct Compatible/Responses local HTTP controls and fork-chat fakeToolExec authority controls are not public or genuine-provider proof. No paid provider/Docker credit. |
| Q02 | Whole-family complexity/size | Exact independent base/head ratchet; every changed/new callable <25, same metric policy, gross/net production counts reported |
| Q03 | Complete current qualification | Full census/inventory/registry and core structural guards locally; per6117588660 hosted lifecycle/no extra units and Local focused, no duplicate local tier; exact final head on the now-integrated Go1.26 master |
| A01 | Anthropic accepts a canonically invalid delivered schema | **split / escalated as separate class to2627**. Independent6117588660 direct red probe: validator rejects bad={type:unsupported-provider-type}; Anthropic buildRequest returns nil error and the same bad schema. This proves the adapter-boundary mismatch only, NOT public-route reachability or a provider response. No repair/closure credit in this maintenance PR. |

Generic failing proofs: separately omit recursive property walk, oneOf recursion,
type validation and enum-shape refusal in disposable candidates; each must compile
and fail its named assertion, then restore. No "same seam" proof credit. Compare
provider request JSON and generated schema bytes before/after, not just acceptance.

## Parent, Watchlist, Architecture And Gate Conditions

Parent sibling probe found live directional relations27/25, ToolSchemaEntry30,
trigger/catalog validation and four provider continuation orchestration seams.
Their consumer/concept boundaries differ from the chosen syntax-maintenance
class; they are not closed by2509 or this slice. Responses SSE remains an explicit
separate product question. Keep2447/2407/2349/2627 OPEN. After this slice the
monolithic syntax-maintenance family tail is zero; provider-facing schema validity
still includes the live2627 bypass and unproved public reachability. Remaining
maintenance parent tail has at least relation and entry
families plus peer continuation/catalog groups (medium grouping, low PR-count
confidence; no honest finite whole-parent closure estimate).

Tracker decision: refine existing2447/2407 tail and boundary_owned_decomposition /
pack-interface watchlist nodes, not create a new issue or reuse2509's closure
language. The nodes already track multiple separately owned admission and
provider families. Promotion check does not justify combining a pure syntax
extraction with provider lifecycle, transport or a typed-model redesign.
Reviewer6117588660 falsified the Anthropic admission predicate and opened2627.
That same broad concept is now explicitly split, not called "different" or left
as an undecided upstream question. Absorbing it would change behavior and violate
this maintenance gate.2627 needs its own pre-audit/independent gate; no new node
or POTENTIAL_ISSUES entry. Public reachability remains unproved. The watchlist
promotion decision is retain this pure first slice and track the semantic bypass
separately, not silently combine a redesign or weaken the provider contract.

Type-model smell: arbitrary map/any inputs can represent shapes outside the
delivered subset; longer-term admission could produce one immutable validated
provider-schema value. Existing2349/pack-interface watchlist ONLY for now, no new
framework/constructor/type work. Rough2-4 days across producers/consumers if
promoted, medium ROI, low-confidence effort until the transport boundary census.

Proposed small implementation: private properties and ordered-composition phases
inside the existing five-function owner; preserve every type switch, nil rule,
sorted-property/error order and recursion exactly. No fresh validator, generic
walker, callback engine, compatibility seam, vendor or behavior/spec migration.
Can close the chosen maintenance class in one PR; other same-parent interpreters
remain explicitly tracked, not silently absorbed or claimed eliminated.

Per6117588660, original2447 one-family/1500-gross/net<=0 rules are NOT waived by
2509's historical batch exception. No compression to game the count. If a small
positive net is genuinely needed, pause with the exact proposed diff/cost and
obtain the user-as-lead waiver before that edit; reviewer approval alone is not
a size waiver. Hosted CI-Tier lifecycle, no CI-Units; Local-Tier focused.
Existing tests are preserved; any needed test edit or semantic change stops.
There is no governing runtime/spec delta planned for a behavior-only extraction.

Executed baseline only: unchanged provider-schema, emitter precision/closed/
unresolved/nested-enum, boot mapping and compatible/Responses frame controls,
race x3,36 pass terminals, no failure/skip: llm1.189s, tools2.483s,
bootverify1.064s. Evidence next-provider-schema-baseline.jsonl. No new
characterization, mutation, public/dual-store candidate, tier or closure credit.
After the actual730c17b98 integration, the same roots plus the named Claude
projection/construction and fork-authority controls pass race x3 under effective
GOVERSION go1.26.8:81 pass terminals, zero fail/skip; llm1.248s, tools2.631s,
bootverify1.081s. Evidence next-provider-schema-go126-baseline.jsonl. These are
unchanged component/local-HTTP/stub controls, NOT new public or genuine-provider
proof. No production/test/spec edits were made to obtain them.
No server2 request. Current PR2626 has code-positive cycle2 review and green
Go1.25 hosted lifecycle38200300122 attempt1 atdb3d74011. Per the user, its branch
stays frozen and the orchestrator started ONE fresh hosted38203162730 on the new
Go1.26 merge ref; do not toggle/push it or treat the old version as new-version
credit. Findings there preempt this audit-only branch.

Overlap: paginated inventories2612/2614/2616/2617/2622/2624/2626 and A642 local
branch803e2c691 show no target/direct-test edit.2614 changes nearby tool catalog
consumers,2612/2617/2624 share spec/planner; do not edit those files. Proposed
new characterization file is llm/provider_schema_factoring_test.go. Recheck
actual lane heads and Go version at coding gate; tier decisions remain reviewer-owned.

## Audit-Only Gate Amendment (6117588660)

Independent outcome was **insufficient; widen class**, not coding approval. This
one-pass repair supersedes the old2250 mapping and undecided Anthropic labels in
77c052858. P01-P23/C01-C05/Q01-Q03 remain31 maintenance proof rows; distinct A01
adds the already-proven semantic manifestation explicitly split to2627, total32
accounted rows. Correct existing2447 issue/watchlist entries before requesting
the short re-gate. No production/test/spec edit, constructor rewrite, validation
change, budget waiver, vendor or type-model implementation is authorized here.
