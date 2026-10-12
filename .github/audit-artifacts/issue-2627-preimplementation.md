# Pre-Implementation Coverage Audit: Provider-Facing Schema Enforcement

Agent-g,2026-10-12. Source actual merged master
`fc76c8b40e7b7d4adf9a0b7985a6e926939b9e49`, effective Go1.26.8.
Audit-only branch, separate from PR2628. **Coding CLOSED pending independent
gate.** Only audit Markdown is committed; disposable probe Go files are removed.
PR2628 CI/review findings preempt this work. No production/test/spec repair,
provider call outside local simulators, external message or vendor change.

## Binding Context And Class

Implementation addendum,2026-10-12: independent gate6118463764 approves the
whole widened five-consumer/four-family enforcement class. Coding is now OPEN
under its five binding conditions. Actual implementation base568dd9ab0 integrates
merged2628 without editing its canonical validator or characterization files.
The historical audit-only status above records the pre-gate source, not current
permission. Hosted lifecycle/no extra units, local focused race/dual-store/
compiled-host proof; all32 rows and five call-removal mutations remain required.

Read the full2627 issue/thread, including6117796296's additional MCP seam, the
2447/2628 split/gate, IMPLEMENTER_GUIDELINES and SEMANTIC_DRIFT. Exact binding spec:
`engine.agent_session_management.llm_provider_adapter_contract.required_contract_dimensions.tool_schema.rules`
(platform-spec.yaml7601): tool definitions translate before invocation; provider-
facing schemas validate before provider execution. Adjacent contract dimensions
require exact native/capability truth, canonical memory/authority, response and
effect settlement. Generated closure and immutable ToolInputSchema admission
remain separately owned laws, not substitutes for final provider admission.

Category: high-risk semantic-drift / validation-parity repair. Observed symptom:
Anthropic declares `ValidatesInputSchemas:true` but `buildRequest` copies the
exact unsupported type rejected by `ValidateProviderToolSchema`. The entry point
was not the audit boundary. Probe all request/catalog/evidence consumers and
upstream authored/discovered/generated sources before choosing scope.

Chosen working failure class: **missing enforcement of the existing canonical
provider JSON-subset admission at final request, owned MCP presentation and
target-observed MCP schema-confirmation boundaries**. Known gaps: Anthropic
request projection, Mock request projection, the two live gateway projection
arms, and target-observed schema evidence. This is not a new JSON Schema dialect,
universal provider acceptance guarantee or an immutable-schema redesign.

Immediate parent concept: provider-facing schema validity across source
admission, translation, request presentation and observed delivery. Broadest
parent: the provider adapter's complete declared adjacency contract. #2627 owns
this enforcement class; #2349 owns representable-invalid/type-model architecture
debt, #2407 proof/gating, #2447 maintenance. #2250 excludes schema-variant
validators and is not a semantic parent here. Anthropic-only issue framing is
**too narrow** after the census: update this issue, not silently fix one arm or
create a speculative follow-up. One bounded PR can consume the existing owner
at every missing boundary without a new framework or general refactor.

Intended closure: **chosen enforcement failure class eliminated**, not all
provider semantics/dialects. Commitment: eliminate every currently known same-
class bypass in this PR. Parent action: absorb all four boundary families now;
retain wider provider/dialect and model obligations explicitly open. This is
not a first-slice excuse for leaving Mock or MCP unchecked.

## Executed Counterexamples And Limits

Reproducer source is in `issue-2627-counterexamples.md`. Each negative is a
deliberately RED refusal assertion on unchanged production, race x3:

- `TestIssue2627AuditBuilderCounterexamples/anthropic`: canonical unsupported-
  type refusal; Anthropic returns nil and the same schema bytes.
- Same root `/mock-request`: `buildMockRequest` returns nil and the same bad
  schema. Mock StartSession already validates; this proves per-request omission,
  not a currently discovered public post-start mutation producer.
- `TestIssue2627AuditAnthropicManagedHTTPRefusal`: valid UUID, exact run/event/
  actor/session/lifetime, production capability compilation and completed frame,
  resolved model and local cached credential; actual continuation sends the bad
  schema once to a real local HTTP server and settles its simulated valid reply.
  This is component/real-local-HTTP evidence using effecttest authority, not a
  compiled public route, selected database or genuine Anthropic response.
- `TestIssue2627AuditMCPToolsListSchemaRefusal`: production capability compiler
  accepts explicit visible/callable definition inputs; exact managed context
  registration, bearer/context admission and identity comparison succeed; actual
  HTTP handler tools/list returns the unsupported schema. Executor calls0. This
  uses an executor double and recorder, not a real TCP/Docker/public CLI journey.
- `TestIssue2627AuditWorkspaceSchemaEvidenceRefusal`: exact expected definition
  hash and valid object JSON suffice for target-observation confirmation of the
  canonically invalid schema. Identity is not syntax admission.

GREEN countercontrol, race x3: `TestIssue2627AuditExternalDiscoveryRefusesUnsupportedType`
uses the real HTTP and stdio discovery readers; both refuse with
`mcp_tool_catalog_invalid`, zero published tools. Authored/tool-entry/builtin
definitions likewise flow through immutable ToolInputSchema admission; generated
emits already consume provider admission/closure. **No supported authored or
discovered malformed-type exploit is claimed.** The confirmed contract defect
is at the request/presentation boundary; real source reachability is constrained
by the earlier owners and will be demonstrated, not inferred from identity.

Initial probe UUID/run/effective-capability errors are retained as invalid fixture
attempts, not counterexample credit. Corrected final probes reach the named
branches above. Logs in `/home/youmew/.local/state/agent-g-2509-postmerge-preaudit-20261012/`:
`issue2627-audit-counterexamples-final.jsonl` SHA256
52055d26e5d8d4590f99197f818b9acac026240ca408015c2a27824bbd42cb0f;
`issue2627-audit-workspace-evidence.jsonl`
616f0a76d0d2cd419bda82bb342a70cfce547408e68759afd597f8e7f79cf22e.
Unchanged schema, Anthropic/Mock frame, fork freezing, MCP identity/context,
HTTP/stdio schema ingress, immutable schema and generated/builtin controls pass
race x3:198 test terminals, zero fail/skip; llm8.767s,mcp1.290s,contracts1.151s,
tools1.262s. These are baseline controls, not repair or whole-tier proof.

## Full Execution Paths And Gates

Normal API path: source load/import -> immutable schema/ToolSchemaEntry admission
-> actor-scoped execution definitions/generated emits -> exact runtime/provider
contract -> StartSession/memory lease -> per-turn actor/session/execution authority
and capability plan -> completed frame -> continued memory/active-delivery/model/
credential gates -> provider request projection -> exact request-definition
identity observation -> request JSON -> completion target/attempt -> HTTP launch
-> response/usage -> durable settlement/projection/release.

CLI/Mock path: the same source/actor and session/turn gates -> workspace execution
target -> exact MCP registration -> target-local tools/list over HTTP -> selected
owned/frozen executor catalog -> capability filtering/definition identity -> MCP
schema projection -> observed definition object/name/hash comparison -> gateway
evidence -> model launch. Mock separately constructs its model-input tool array
before workspace/attempt/model invocation. Fork-chat first validates and freezes
schemas under its exact fork/actor/executor authority; selected-contract startup
uses the existing startup authority/catalog path, not a runless compatibility arm.

| Gate | Classification and concrete proof |
| --- | --- |
| Authored/imported ToolInputSchema and ToolSchemaEntry admission | Different semantic concept: full immutable authored execution value, rejects unsupported kind/constraints before projection; unchanged ToolInputSchema malformed-recursion/null/depth and tool-entry controls. Planned S01/S02 extend projection-to-provider checks. |
| External MCP catalog admission | Different semantic concept: remote catalog shape and immutable contract admission; executed HTTP/stdio unsupported-type zero-publication control plus W5 atomicity/range/enum controls. |
| Generated emit lowering/closure | Already consumes same canonical provider owner, plus different closed/generated law; unchanged precision/unresolved/closure controls and S05. |
| Actor, permission, native capability and definition identity | Different concept, not schema-validation credit: managed compiler/identity accepts the same bad schema in the corrected red. Existing exact-actor/native/identity-drift tests protect these gates. |
| Session memory, frame, lifecycle and effect authority | Different concepts with existing exact acquire/frame/grant controls; actual local-HTTP red crosses them. Q01 repeats against both selected stores and verifies no launched attempt after repair. |
| Anthropic/Mock request projection | Same chosen class: A/M counterexamples, before model/effect launch. |
| Gateway normal/startup/selected/frozen catalog projection | Same chosen class: G rows retain token/identity/presentation gates and validate only selected/delivered schemas. |
| Target-local observed schema confirmation | Same chosen class: O rows; JSON-object/hash validation is not canonical subset validation. |
| Provider-owned native descriptors/inventory | Different vendor capability/initialization law, not platform input-schema forwarding; Claude exact native/tool arguments and inventory controls. No provider-native dialect rewrite. |
| Transport, response, budget, settlement and shutdown | Different concepts: existing canonical frame/settlement/worker guards; no retries, timeouts, lease, effect or completion-owner changes. |

Earlier gates must stay real: a test stopped at actor/credential/identity admission
cannot claim final schema refusal. Bad source/discovery cases are upstream proof,
not fictitious repaired request-path reachability. Paid-provider/Docker proof is
not provisioned or claimed; compiled host/private-SQLite, selected-store and
local simulated-provider evidence retain their different proof surfaces.

## Canonical Owners And Exhaustive Consumption

Schema syntax owner: existing `llm.ValidateProviderToolSchema` and list wrapper,
not the first Anthropic helper. Its current nil/typed-nil, eight ordered constraints,
required/union/enum representation and error paths bind the admitted subset.
PR2628 only factors that owner and must remain semantically disjoint. No edit to
its files is planned. No alternative validator, new dialect, generic walker or
historical producer/reader compatibility path.

| Sibling seam / consumer | Consumption decision |
| --- | --- |
| tools/emit_runtime.go generated static schemas | Already consumes canonical owner; precision/unresolved/generated tests. |
| tools/generated_schema_closure.go generated definitions | Already consumes canonical owner before independent generated law; boot closure mapping. |
| Compatible buildRequest | Already consumes canonical schema after default translation, before returning request; preserve its existing exact bytes/error/no-send controls. |
| Responses buildRequest | Already consumes same owner; preserve JSON request/HTTP controls, no SSE credit. |
| Mock StartSession | Already consumes list owner after actor/mode/performance checks. Earlier validation alone is not per-request proof. |
| fork_chat_transport newForkChatToolDispatch | Already consumes list owner before immutable freeze. No unvalidated schema can be inserted through the owned fork constructor; preserve exact foreign-authority/refusal/freeze controls. |
| Anthropic buildRequest | **Moved to canonical owner in proposed work**, after default projection and before tool append/invocation; first-error/name/schema bytes otherwise preserved. |
| Mock buildMockRequest | **Moved to canonical owner in proposed work**, before request construction/attempt/worker/model; do not rely on StartSession or shared-map stability. |
| Gateway mcpToolsForCapabilitySurface | **Moved to canonical owner in proposed work**, exact identity checked, selected projected schema checked before append or confirmation evidence. Validate selected definitions, not an unrelated hidden catalog entry. |
| Gateway mcpToolsForActorInContext | **Moved to canonical owner in proposed work**, after visibility/allowlist/default projection, including frozen fork path; preserve leases/order/defaults and fail the whole list without partial publication. |
| llm compareWorkspaceMCPDefinitions | **Moved to canonical owner in proposed work**, retaining object/name/duplicate/exact-hash gates before admitting target-observed schema; fail before model launch/evidence confirmation. |
| CompileManagedCapabilityAdmission, ToolDefinitionIdentity, API ObserveAPIRequestCapabilitySurface | Different identity/authorization law, explicitly execution-proven to accept a matching bad schema; not final syntax interpreters and not credit for validation. Final request/presentation owners consume canonical syntax. No speculative shared compiler rewrite. |
| tools immutable schema/value/entry/registry, packs/channel compiled schemas | Different full semantic contract owners; producer projection-to-provider controls planned S01-S07, no replacement/ownership move. |
| mcp/client.go remote discovery | Different remote ingress/immutable catalog owner; current real HTTP/stdio refusal already proven. No new parsing or discovery framework. |
| Claude invocation/native inventory and toolgateway HTTP reader | Different exact execution-target/name/protocol law; canonical gateway presentation plus observed-schema owner supply syntax enforcement before model launch. Preserve existing target-local/foreign-ID/worker cleanup controls. |
| Gateway MCPToolsForActor convenience facade | No non-test production caller found; delegates to actor projection and returns no list on errors. Not a second live schema interpreter or an excuse to swallow a public-path error. No unrelated API deletion proposed. |

Old non-authoritative decisions invalidated: copying a schema into Anthropic/Mock
input without admission; exact catalog/definition hash implying valid schema;
target observation confirming syntax merely because JSON is an object and hashes
match. Those paths must no longer admit a request/list/confirmation. Existing
source/identity/lease/effect owners survive unchanged, not as dual schema owners.

## Manifestation And Named Proof Plan

All rows are PLANNED repair proof, not earned green credit. New component root
`TestProviderSchemaBoundaryAdmission` carries A/M/G/O subtests; exact negative
controls remove each consumer's canonical call to ensure a discriminating failure.

| Row | Manifestation | Exact planned proof/oracle |
| --- | --- | --- |
| A01 | Anthropic unsupported top-level type | `/anthropic/unsupported`; exact canonical error, zero sends |
| A02 | Nested properties/items/propertyNames | `/anthropic/nested`; lexical/indexed path and full error |
| A03 | oneOf/union/required/enum malformed representations | `/anthropic/shape`; exact existing canonical rules, no new dialect |
| A04 | Mixed errors/definition order | `/anthropic/order`; earlier message gate and first tool/schema error, later model gate retained |
| A05 | Nil/default/typed-nil and valid schema bytes | `/anthropic/valid`; existing default/wire bytes and no mutation; no provider acceptance guarantee for the owner's nil policy |
| A06 | Normal/continued/selected/fork API calls | Q01 exact authorities, malformed no send/attempt; valid request/usage/settlement controls |
| M01 | Initial Mock request | `/mock/initial`; canonical error before request/worker/model |
| M02 | Later malformed mutable session projection | `/mock/continued`; valid StartSession then bad request fixture, no reliance on initial admission |
| M03 | Mock actor/performance/memory precedence | `/mock/gates`; named earlier gates still refuse and valid schema reaches intended request boundary |
| M04 | Valid Mock input/nonmutation | `/mock/valid`; exact input tools/round/message bytes and real host MCP continuation control |
| G01 | Normal managed MCP catalog | `/gateway/normal`; matched bad hash refuses with no returned tools/evidence/tool calls |
| G02 | Selected-contract managed catalog | `/gateway/selected`; exact selected authority, same refusal/no partial confirmation |
| G03 | Startup catalog | `/gateway/startup`; actual startup token/catalog gates, schema refusal before model probe |
| G04 | Frozen fork-chat catalog | `/gateway/fork`; original frozen-schema admission and actual valid list path; malformed owned fork constructor refuses before freeze |
| G05 | Hidden/unplanned/foreign/mismatched definitions | `/gateway/selection`; existing identity/token refusal earlier; unselected malformed entry never changes selected list semantics |
| G06 | Ordered list/lease cleanup/failure | `/gateway/lease`; one release on success/refusal, no partial list/evidence, retain original error and actual inherited-presentation controls |
| O01 | Canonically invalid matching target observation | `/observation/schema`; exact hash and valid JSON object no longer confirm malformed syntax |
| O02 | Missing/null/non-object/duplicate/foreign/hash mismatch | `/observation/hostile`; earlier protocol/identity gates unchanged and typed refusal retained |
| O03 | Valid-empty/translated/typed number observation | `/observation/valid`; exact payload/hash/default acceptance, no alternate normalization |
| O04 | Pre-model target-local refusal | Q02 host worker launch counter and unchanged target-local gateway failure/cleanup tests; no outcome-uncertain/retry/paid-model invocation |
| S01 | Authored tools/immutable values | New `TestProviderSchemaSourceAdmission/authored` plus malformed/null/depth and exact projection controls; invalid source refuses before provider boundary |
| S02 | Imported/pack/connector/channel projections | Same root `/imported`, actual admitted ToolSchemaEntry projections; wrong kinds refuse upstream, no caller reconstruction |
| S03 | Remote HTTP discovery | Executed red-source GREEN control extended with valid projected schema and zero atomic publication on invalid catalog |
| S04 | Remote stdio discovery | Same real stdio entrance; cancellation/process join unchanged, not fake RPC credit |
| S05 | Generated emits/flow-local refs | Existing precision/unresolved/generated closure and boot mapping; canonical lowerings accepted, malformed producer refuses |
| S06 | Builtin/native fallback/role-scoped schemas | Actual admitBuiltinExecutionTool/actor registry projections, valid full catalog canonical check; malformed draft refuses upstream |
| S07 | Typed enums/required/oneOf/propertyNames | Exact admitted projection-to-canonical controls, preserving stricter typed/generated law and provider-subset distinctions |
| Q01 | SQLite/PostgreSQL effect boundary | New `TestProviderSchemaEffectRefusalBothStores` using existing completion/native-store fixtures and canonical `storetest.ReadExternalAttemptStorage`/`ReadManagedTurnEffectStorage`: malformed request has zero launched model attempts/turns/spend, no provider sends; valid local-HTTP request settles once and retains exact identities; real lease/release gates, no raw SQL/new delegate |
| Q02 | Real public host/MCP supported surface | Existing compiled `TestWorkspaceMCPCompiledHostConformance`, plus source/discovery malformed compiled verify/serve controls; negative source admission is upstream credit, not final-boundary exploit. Host/private SQLite distinct from Q01 component databases; Docker/paid provider not claimed |
| Q03 | Reconstruction/restart/fork | New `TestProviderSchemaReconstructionBothStores` through existing admitted source load/selected construction and exact normal/selected/frozen authorities; identical admitted schema reaches the repaired boundaries after reconstruction. Existing `TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends` supplies distinct compiled H/Mock restart settlement evidence, NOT public or Anthropic provider credit. No old-store migration or fake persisted invalid schema |
| Q04 | Generic failing controls | Remove Anthropic/Mock/MCP projection/observation canonical calls separately in disposable candidates; each compiles and fails its exact no-send/no-confirmation assertion, then restore |
| Q05 | Exact final qualification | Complete changed-owner guards/census, independent complexity, focused race x3, supported/selected proofs; reviewer chooses hosted tier/Local plan, no duplicate tiers |

Total32 distinct rows: A6/M4/G6/O4/S7/Q5. The four boundary families are one
enforcement concept, not shared semantic ownership of lifecycle/transport. Every
known manifestation has named execution proof; no same-seam-only credit.

## Spec Delta, Tracker And Architecture

Proposed authoritative spec delta in the eventual runtime PR: add a rule under
the exact tool_schema clause that existing canonical provider-subset admission
governs each projected API/Mock request, selected owned MCP catalog and target-
observed MCP schema before delivery/confirmation/model launch. Earlier source
admission and definition identity are not replacements. Preserve protocol/root
shape, exact authority and effect gates; no dialect/normalization expansion,
schema-aware retry, provider-call replay or compatibility path. The PR must
update platform-spec.yaml, not rely on this draft or issue prose as merge artifact.

Tracker-state decision: **current issue must be updated before coding** to include
Mock/MCP/observation siblings; existing watchlist node refined and issue assigned
to G. #2349 remains the type-model umbrella; #2447/2628 is maintenance only and
#2407 proof parent. No new child/umbrella issue or POTENTIAL_ISSUES entry. No older
closed stream reopened. All required repair is completed with this audit/gate
request, not deferred until implementation.

Recorded repair:2627's title/body now name the widened request/MCP/evidence
class and its source-reachability limits; G is assigned. Existing semantic and
maintenance mappings are published at `swarm-docs@d47865b`, with YAML parsing,
unique node/issue IDs and explicit2627/node-reference checks passing. No code is
authorized by these tracker repairs; the independent gate is still requested.

Watchlist-backed promotion check: mapped
`pack_interface_structural_satisfaction_and_channel_adapter_ownership` already
tracks schema projection, immutability, provider/capability consumers and2627.
Its earlier copied-schema/identity evidence suggests the MCP and Mock consumer
families beyond the Anthropic symptom. Promote those now; leaving them to an
unspecified future slice would be dishonest. After the proposed PR, remaining
tail inside this enforcement class is zero if proofs/gate accept. Broader model/
adapter contract tail is separately owned typing/relations, native lifecycle,
response/usage and unresolved Responses SSE work, with medium grouping and low
finite-PR confidence; no whole-parent closure estimate fabricated.

Architecture smell: mutable raw ToolDefinition.Schema plus identity-only evidence
can represent an invalid schema after earlier admission. Long-run direction is
one immutable validated provider-schema value, not stronger heuristics or another
runtime framework. Existing2349/watchlist ONLY, not promoted into this repair;
rough2-4 days across producer/DTO/consumer ports, medium ROI, low confidence until
a dedicated audit. No type/schema/storage redesign or new semantic owner.

Closure-feasibility: yes, one bounded PR through existing validation and four
forwarding/evidence owners. Fixing only Anthropic would leave three same-concept
families live. No broad duplicated validation implementation was found, so this
is not a new framework/broad-refactor request. A new live interpreter, ambiguous
gate placement/dialect, source/effect ownership change or failing supported
fixture that demands such a change stops for a repaired gate.

## Overlap And Gate Request

Proposed production files: llm/api_runtime.go, llm/mock_runtime.go,
mcp/gateway.go, llm/workspace_mcp_admission.go, plus authoritative spec. New tests
use separate issue-specific files. Do not edit2628's provider_tool_schema.go,
provider_schema_factoring_test.go or provider_schema_consumers_test.go. Reuse its
public owner unchanged; merge order2628-before2627 is preferred for integrating
the factored owner, not required to discover/fix a duplicate validator. Paginated
active2612/2614/2616/2617/2624/2628 inventories show no proposed runtime-file edit;
several share platform-spec.yaml, so exact clause integration must be reviewed at
the eventual base, not blindly regenerated/hand-merged. Spec overlap is unavoidable
with active architecture work but no semantic clause overlap is currently found.

Request reviewer-g independent coding gate for the **whole widened enforcement
class**, proposed hosted lifecycle/no extras and Local focused plus the named
both-store/compiled supplements (not a Local full). Tier is reviewer-owned; newly
added selected-store root placement must be proved before qualification. No code
before the explicit gate; no server2 slot or hosted tier run for this audit.
