# Post-Implementation Proof Audit: Provider Schema Boundary Enforcement

Issue #2627, gate [6118463764](https://github.com/division-sh/swarm/issues/2627#issuecomment-6118463764).
Integrated source base `568dd9ab0`, effective `go1.26.8`; production repair
`860db22de`, subsequent exact guard classification and proof-only refinements.
Cycle-1 amendment6119665740 now authorizes the bounded shared-owner and obsolete
characterization corrections below. This artifact records local proof, not an already-green hosted run.
Hosted lifecycle qualification and independent final approval are outstanding.

## Class, Contract And Closure

Category: high-risk semantic-drift/validation-parity. Exact concept changed:
**truthful recognized provider wire-syntax admission and its enforcement on
selected/defaulted request and MCP presentation projections, and target-observed
schema confirmation**, plus Anthropic's provider-specific object-root law.
Chosen working class: all five currently known missing consumers in four boundary
families, not just the Anthropic symptom. The original issue was symptom-shaped;
the repaired pre-audit and gate widened it before coding. Commitment: eliminate
this chosen working class entirely, not preserve another unchecked sibling.

Immediate parent: provider-facing validity from source admission through observed
delivery. Broader parent: the provider adapter's full adjacency contract. The
boundary excludes distinct immutable/generated laws, vendor-native inventory,
transport/response/usage, and type-model redesign. Existing #2349 tracks mutable
representable-invalid schema architecture; #2407 proof and #2447 maintenance
remain open. #2250 explicitly excludes this schema class and is not its parent.

Binding spec, updated in this PR:
`engine.agent_session_management.llm_provider_adapter_contract.required_contract_dimensions.tool_schema.rules`.
Earlier immutable admission or matching definition hashes cannot stand in for
final schema admission. Only delivered/selected definitions are checked. Refusal
returns no partial request/list/evidence and precedes model execution/its worker;
target-observation workers earn probe evidence, not model-execution credit.
No dialect, normalization, identity, timeout, retry, persistence, migration,
legacy compatibility, paid-provider call, vendoring or new semantic owner.

Achieved locally: all five consumers enforce the existing owner; every planned
manifestation has named execution evidence and all five call-removal mutants are
discriminating. Intended final closure: **chosen failure class eliminated**, subject
to hosted lifecycle and independent acceptance. Not whole-parent closure. Remaining
tail inside this class is zero if accepted; no new consumer appeared in the final
sweep. Broader parent tail still has immutable provider-value/model relations,
native lifecycle/capability and response/usage/SSE families. Rough three-to-four
families, no defensible finite PR count, low confidence; implementation did not
change that estimate. Responses SSE is neither repaired nor credited here.

Watchlist: existing
`pack_interface_structural_satisfaction_and_channel_adapter_ownership`, refined at
`swarm-docs@12bad63`; its wider producer/consumer families justified absorbing
Mock/MCP/observation now. No new node, issue or `POTENTIAL_ISSUES.md`. Architecture
feedback attaches to existing #2349: one immutable validated provider-schema value
is the longer-run direction, not an identity heuristic or another framework.
Estimate 2-4 engineering days across producer/DTO/consumer ports, medium ROI,
low confidence pending its dedicated audit. Parent remains open; no type redesign
was silently folded into this repair.

## Canonical Owners And Exhaustive Consumption

Syntax owner: existing `llm.ValidateProviderToolSchema` and its ordered list
wrapper `ValidateProviderToolDefinitions`. This is the real provider-subset owner,
not a local Anthropic helper. All relevant production sibling seams found by
request/catalog/observation construction and validation searches are listed below.
Cycle1 corrects its recognized wire syntax rather than introducing another
validator. Truly absent optional definitions retain their presentation default;
declared typed-nil maps, nested null schema nodes and recognized null containers
refuse. Exact type tokens and flat/unique arrays replace trim-only checks;
required members are unique. Legitimate empty required/enum arrays and enum null
values remain allowed; unknown-keyword behavior is not reinterpreted. Anthropic
requires an explicit object root after its existing absent-schema default.

| Consumer / surrounding owner | Status after change | Systematic-consumption proof |
| --- | --- | --- |
| Anthropic `buildRequest` | moved to canonical owner | After default projection, before append/send; exact-error/no-partial/byte controls, real managed HTTP refusal and both-store zero attempts. Call-removal mutant fails. |
| Mock `buildMockRequest` | moved to canonical list owner | Every request, including after valid StartSession; before input/model worker. Actor/performance/memory precedence and exact bytes pass; call-removal mutant fails. |
| MCP `mcpToolsForCapabilitySurface` | moved to canonical owner | Exact selected capability and definition identity first, projected schema second, before list/evidence. Normal/startup/selected context controls and release1/1; call-removal mutant fails. |
| MCP `mcpToolsForActorInContext` | moved to canonical owner | Visibility/allowlist/default first, atomic list second. Actual frozen fork dispatch lists over TCP without consulting live mutable catalog; call-removal mutant fails. |
| `compareWorkspaceMCPDefinitions` | moved to canonical owner | Object/name/duplicate/exact hash first, syntax before confirmation; invalid matching hash retains typed cause. Real host observation worker refuses and unregisters once; call-removal mutant fails. |
| Compatible `buildRequest` | already consumes canonical owner | Existing factoring-consumer HTTP refusal/no-send and valid bytes, race3. |
| Responses `buildRequest` | already consumes canonical owner | Existing factoring-consumer HTTP refusal/no-send and valid bytes, race3. Not SSE proof. |
| Mock StartSession | already consumes canonical list owner | Existing actor/mode/performance and malformed-schema admission; per-request check is separately proved, not inferred from this one. |
| Fork `newForkChatToolDispatch` | already consumes canonical list owner | Malformed constructor refusal, exact-authority freeze controls; real frozen list ignores later source-map mutation. |
| Generated static `emit_runtime` and `generated_schema_closure` | already consume canonical owner | Precision-ref lowering and malformed closed/generated schema controls pass, race3; generated closure remains separately authoritative. |
| Immutable ToolInputSchema/ToolSchemaEntry, tool registry, pack/channel/connector admission | different semantic concept, with proof | Stricter immutable source and execution contracts; admitted projections pass canonical provider validation, malformed source refuses upstream. Real pack registry mutation-isolation controls pass. No second provider-subset interpreter. |
| HTTP/stdio MCP discovery | different semantic concept, with proof | Remote ingress/immutable catalog admission; real transports preserve valid projection and publish zero tools on invalid mixed catalog. Not final-boundary reachability credit. |
| Capability compiler, ToolDefinitionIdentity, API request capability observer | different semantic concept, with proof | Authorization/identity owners accept a matching malformed definition in the pre-code counterexample; final presentation now supplies syntax admission. No compiler/hash redesign. |
| CLI native inventory, Claude invocation, tool argument admission, toolgateway protocol | different semantic concept, with proof | Native descriptors/arguments/protocol identity are not provider schema interpreters. Existing target-local per-launch, HTTP/stdio number and managed normal/selected chronology controls pass. |
| MCPToolsForActor convenience facade | already delegates to canonicalized actor projection | No non-test production caller; no independent walker or error-tolerant public compatibility path introduced. |

Related owners are unchanged and consumed as before: immutable contracts/source
registry for schema producers; managedcapabilities and exact fork/turn context for
selection/identity; workspace/toolgateway for observed transport; completion,
session lease, delivery and worklifetime for effects/settlement. Touched consumers
are only the five boundaries above. Their sibling-authority proofs below protect
these independent laws rather than claiming ownership of them.

Old decisions now invalid: copying Anthropic/Mock schemas without admission;
definition identity implying schema validity; confirming a target schema just
because it is an object with a matching hash. No old unchecked branch survives
inside this class. Earlier immutable/generated/native/identity gates survive as
different laws, not alternate or compatibility provider validators.

## Execution Paths And Proof Surfaces

API: immutable source/registry -> actor/exact lifecycle and memory -> selected
delivery/lease -> capability/frame/model/credential -> projected request schema
admission -> request identity -> attempt/HTTP -> response/settlement/release.
Mock/CLI: same earlier authority -> selected target and exact token -> owned
catalog -> filtered/defaulted schema admission -> target object/name/hash/syntax
confirmation -> model input/worker -> real gateway tools -> settlement. Fork
construction freezes admitted definitions under exact fork authority; its list
uses that frozen executor, not the live source. No earlier gate is bypassed.

Q01 real gates: bootstrapped/admitted SQLite and PostgreSQL, durable session and
memory acquisition, lifecycle token, actual delivery claim, canonical frame,
real capability compiler and completion controller. Credential map and capability
executor are controlled fixtures; the executor panics if any tool executes.
Provider is a real local HTTP simulator via a scoped transport, not Anthropic.
Event publisher is absent in this component fixture; memory delivery-marking is
therefore not public publication credit. Malformed schema reaches the intended
request error with zero HTTP, external-attempt and managed-turn/effect facts.
A fresh valid successor preserves session identity and settles exactly once.

Q02 separates actual compiled public `swarm test` + host workspace + real HTTP
MCP/private SQLite read/emit settlement from direct gateway Recorder fixtures and
the native host observation worker/registration fixture. Compiled verify/describe
and actual authored/discovery malformed-source refusal are upstream proof, not
an authored invalid schema delivered to the final boundary. No Docker, Telegram,
paid provider or real provider-native acceptance claim.

Q03 independently credits two actual selected-store reconstructed Anthropic
simulator turns using distinct durable event/claim identities and the same memory
session, selected-fork authority issuance/recovery controls, real frozen fork
TCP tools/list, and compiled H/mock both-store graceful restart/forced kill.
The H harness is not public serve/test and is not an Anthropic process restart.
No persisted-invalid-source or old-store compatibility fixture is invented.

## Manifestation Coverage

Receipt aliases below name exact tests; receipt ledger follows. Every disposition
is exactly one of the mandated categories. Baseline reds and mutation reds are
retained; a passing sibling is not used to erase them.

| Row | Known manifestation | Disposition | Exact proof used |
| --- | --- | --- | --- |
| A01 | Anthropic unsupported type | reproduced and fixed | BoundaryAdmission/anthropic/unsupported exact canonical error, empty request; managed-http and Q01 zero sends/attempts. R1/R2. |
| A02 | Nested properties/items/propertyNames | reproduced and fixed | BoundaryAdmission/anthropic/{properties,items,propertyNames}; canonical lexical/indexed error and no partial tools. R1. |
| A03 | oneOf/union/required/enum shapes | reproduced and fixed | BoundaryAdmission/anthropic/{oneOf,union,required,enum,additionalProperties}; exact existing owner error. R1. |
| A04 | First-error and earlier/later gates | execution-proven through the same corrected path | Each Anthropic case has bad first/later definitions, message-first refusal, bad schema before missing-model; valid case still requires model. R1. |
| A05 | Genuinely valid/default object bytes | execution-proven through the same corrected path | Corrected BoundaryAdmission/anthropic/valid exact full JSON uses a valid tool name and exact object/string tokens, absent-schema object default and explicit object root, without mutation. Prior padded/null wire assertions were invalid and superseded; W01/W02/W05 refuse them. R10/R17. |
| A06 | Normal/continued/selected/fork request authority | execution-proven through the same corrected path | Q01/Q03 real normal and continued requests; selected-fork issuance/current-generation/recovery no-redispatch and exact fork-source mode/bundle controls; malformed fork constructor refuses upstream. R2/R5/R8. No direct selected Anthropic invalid-source exploit claimed. |
| M01 | Initial Mock request | reproduced and fixed | BoundaryAdmission/mock malformed cases return empty input; existing StartSession consumer refuses malformed schema. R1. |
| M02 | Continued mutable session projection | reproduced and fixed | BoundaryAdmission/mock/continued: valid actual StartSession, mutate projection, per-request exact canonical refusal. R1. |
| M03 | Actor/performance/memory precedence | execution-proven through the same corrected path | BoundaryAdmission/mock/gates uses actual continuation: missing actor/live/missing performance before memory; counting memory fixture before schema; memory-disabled path reaches canonical refusal. R6. Memory store here is a double, not Q01. |
| M04 | Valid Mock input/nonmutation/transport | execution-proven through the same corrected path | BoundaryAdmission/mock/valid exact copied messages/tool-results/round/schema bytes; compiled host public MockOnly HTTP MCP read/emit/settlement. R1/R3. |
| G01 | Normal managed catalog | reproduced and fixed | GatewayAdmission/managed/normal_agent and http/normal: matching bad definition refuses, no tools/evidence/calls/confirmation; lease1/1. R1/R8. |
| G02 | Selected-contract catalog | reproduced and fixed | GatewayAdmission/managed/selected_contract_fork through production selected surface compiler; same atomic refusal and lease1/1; existing real chronology normal+selected. R1/R5/R8. |
| G03 | Startup catalog | reproduced and fixed | GatewayAdmission/http/startup actual startup token/surface registration and HTTP handler refuse without confirmation. R1/R8. Recorder, not public TCP startup. |
| G04 | Frozen fork catalog | execution-proven through the same corrected path | GatewayAdmission/fork/frozen constructs actual fork dispatch/registry and TCP tools/list; later mutation/live malformed catalog never consulted, exact frozen bytes, zero dispatch/lease. FactoringConsumers/forkchat malformed constructor and authority-freeze controls. R1/R5/R8. |
| G05 | Hidden/foreign/mismatched definitions | execution-proven through the same corrected path | GatewayAdmission/managed/selection-and-identity and actor/selection-and-defaults; hidden malformed ignored, identity mismatch wins; existing missing/invalid token controls. R1/R5/R8. |
| G06 | Order/leases/atomic refusal | execution-proven through the same corrected path | Managed/actor refusal has nil list/evidence and acquire/release1/1; selection success+failure total2/2; exact ordered bytes/defaults. R1/R5/R8. |
| O01 | Matching invalid observation | reproduced and fixed | WorkspaceObservation matching unsupported/nested/oneOf hashes refuse with typed invalid_input_schema and exact unwrapped canonical cause. R1. |
| O02 | Hostile protocol/identity observations | execution-proven through the same corrected path | WorkspaceObservation/{missing,null,array,noncanonical,duplicate,changed,foreign}; original reason precedence retained. R1. |
| O03 | Valid empty/translated/numeric observation | execution-proven through the same corrected path | WorkspaceObservation valid description/number/schema hash and empty catalog pass; absent-definition defaults and explicit non-null MCP object schemas retain exact bytes. Typed-nil wire success is retracted, W02 refuses. R10. |
| O04 | Target-local pre-model refusal | reproduced and fixed | WorkspaceTargetRefusal actual host observation worker+HTTP refuses malformed matching hash, empty confirmation and unregister1; ClaudeEveryLaunchRechecksGatewayBeforeModelInvocation keeps first/resumed/tool-result zero-model-launch controls. R4. Observation is not a model worker. |
| S01 | Authored immutable schemas | execution-proven through the same corrected path | SourceAdmission/authored and upstream-refusal actual yamlsource/immutable admission; compiled StructuralSurfaces verify/describe malformed source refuses earlier. R1/R9. |
| S02 | Imported/pack/connector/channel projections | execution-proven through the same corrected path | SourceAdmission/imported actual admitted ToolSchemaEntry projections, nonmutation; NewPackRegistryAdmitsSchemasAndReadbacksAreMutationIsolated; compiled source tools. R1/R5/R9. Does not claim new live channel onboarding proof. |
| S03 | Remote HTTP discovery | execution-proven through the same corrected path | DiscoveryAdmission HTTP valid projection and invalid mixed catalog zero publication; W5 range/enum/integer/field-name/atomicity controls. R1. |
| S04 | Remote stdio discovery | execution-proven through the same corrected path | Same DiscoveryAdmission stdio actual child transport, canonical valid projection and atomic invalid refusal, not fake RPC. R1. |
| S05 | Generated emits/flow refs | execution-proven through the same corrected path | GenerateEmitToolsForActor_ProviderSchemaNormalizesPrecisionRefs; GeneratedToolSchemaClosureRejectsUndeclaredRequiredAndInvalidEnums; existing structural boot mapping guard. R1/R7. |
| S06 | Builtin/role-scoped projection | execution-proven through the same corrected path | BuiltinProjectionAdmission actual execution/actor catalogs pass owner; malformed draft refuses upstream; ScheduleBuiltinContractDeliversValidatesAndDispatches. R1. |
| S07 | Typed schema representations | execution-proven through the same corrected path | SourceAdmission/imported required-enum/oneOf/propertyNames/items, immutable OneOf/PropertyNames and factoring constraint-order/typed-form controls. R1. Stricter generated law not replaced. |
| Q01 | Real both-store effect boundary | reproduced and fixed | EffectRefusalBothStores race3, each store: exact final error, zero HTTP/external attempts/effect facts, valid successor same session, one settled/consumed provider attempt. R2. |
| Q02 | Compiled supported host/MCP | execution-proven through the same corrected path | WorkspaceMCPCompiledHostConformance compiled public host/private SQLite HTTP MCP read/emit/settlement; StructuralSurfaces verify/describe. Direct malformed target/handler controls separately scoped. R3/R4/R9. |
| Q03 | Reconstruction/restart/fork | execution-proven through the same corrected path | ReconstructionBothStores two distinct durable events/claims and fresh runtimes, identical immutable-source identity, count2 settlements/consumed/attempts; GoldenAgentWorkloadRestartAndForcedKillOnBothBackends compiled H/mock; selected/fork controls and frozen TCP list. R2/R3/R5/R8. |
| Q04 | Generic discriminating counterexamples | reproduced and fixed | Five separately compiled call-removal mutants fail intended admission assertion; no compile/no-test false credit, exact restored diff. X1-X5. |
| Q05 | Qualification/coverage | execution-proven through the same corrected path | Complete structural guard unit, census/inventory/registry, independent complexity, race3 and supported receipts pass locally; lifecycle selects all ten added roots exactly once. R10-R17 and final-head addendum. Hosted lifecycle remains a required pending merge gate, not credited as passed. |
| W01 | Padded raw type tokens | reproduced and fixed | WireShapeAdmission padded root/nested/union members; both MCP wire projections and both observed comparison wrappers; actual host activation/Mock startup and both-store no-send refusal. R10/R11. |
| W02 | Null/typed-nil roots and recognized schema/property containers | reproduced and fixed | WireShapeAdmission nil root/properties/property/items/propertyNames/oneOf/additional nodes; MCP returns nil list/evidence, observed decoded null containers refuse. Root null observation remains earlier protocol refusal. Actual host startup/activation and both-store zero attempts. R10/R11. |
| W03 | Non-flat/duplicate/empty/nil type arrays | reproduced and fixed | WireShapeAdmission typed and untyped arrays plus corrected canonical characterization; both MCP arms and matching target wire refuse nested/duplicate/null unions without mutation. R10/R11. |
| W04 | Duplicate required and null recognized arrays | reproduced and fixed | WireShapeAdmission required duplicates/nil typed forms and enum nil; MCP/observed comparison and selected-store no-send controls. Characterization retains valid distinct exact property names, empty permitted arrays and enum null values. R10/R11. |
| W05 | Anthropic non-object/missing root type | reproduced and fixed | WireShapeAdmission/anthropic_root uses generically valid string/array/union/empty schemas to reach the provider-specific refusal; both-store string-root refusal then one valid object settlement. No new generic provider root law. R10/R11/R17. |

Total37 rows A6/M4/G6/O4/S7/Q5/W5. No known manifestation is left behind a
"shared owner" or "same seam" assertion. No semantic split emerged beyond the
already-defined adjacent laws. Baseline and mutation failures provide exact
counterfactual proof, not merely cleaner architecture.

## Receipt Ledger And Qualification

All local logs are retained under
`/home/youmew/.local/state/agent-g-2509-postmerge-preaudit-20261012/`.
Race3 means `-race -count=3`; no selected proof was skipped. Compiled/release and
structural/census execution uses the canonical swarm-test admission wrapper;
short focused component tests use the gate's permitted direct package commands.
No `go test ./...`, no duplicate whole Local core/lifecycle/full run, no server2.

| Alias | Receipt | SHA256 / result |
| --- | --- | --- |
| R1 | issue2627-final-focused-race3.jsonl | ff0e691c9854257b5dc5684f9b73cb1190fbc5acf6f88b2649b92c5d6e1be650; LLM/MCP/contracts/tools race3 pass. |
| R2 | issue2627-store-final-race3.jsonl | 1b33393c99480a48d3efa84d1fc88c379112cfffada20bc48114b85f071c41f9; Q01/Q03 both stores race3 pass,31.241s. |
| R3 | issue2627-compiled-host-restart.jsonl | 179d4ac50b8ba3b3f888fbcb00876b71f4e084c92c4b6acef4e1598796354e81; public host10.87s, distinct H restart48.62s pass. |
| R4 | issue2627-target-race3.jsonl | 09315bb0373ef2e20e7c6066a8f18ffb15194ec3e2110c1c6ccd175584e8a846; target+every-launch/schema-consumer race3 pass43.648s. |
| R5 | issue2627-sibling-authority-race3.jsonl | ddb71e45cd60e932b0c12e79a4bf5bff4074e3e7389c582bcf54e5c41a0eed76; MCP, pack and selected/fork both-store race3 pass. |
| R6 | issue2627-mock-gates-race3.jsonl | d5483465fa172136c6e91627bce20b10f24c5aa34583fd8fbef70f448e73d549; M03 added actual continuation controls pass1.031s. |
| R7 | issue2627-final-census.jsonl; issue2627-final-structural-guards.jsonl | feafce70df51b4fd60c6cebfa5372365644bc9a186d6214ca5105deb1d806b5a; c2bd2602ea4530dd78f7fab9f0a85d2e287e6ed3e9ae75bec9964c01f66964d8; complete census and structural unit pass. |
| R8 | issue2627-frozen-catalog-race3.jsonl | 78fe901c10f0bf1ff1de1f1f457156d7431784a41e601f0b39ac5c1a7713ad4e; complete GatewayAdmission including actual frozen TCP list race3 pass1.062s. |
| R9 | issue2627-compiled-source.jsonl | 7b074ef85dd652cb4dba06db71f365f3f1cc178db42f93286aa74fdd018532e1; compiled verify/describe/source refusal pass23.937s. |

X1-X5 are `issue2627-mutation-{anthropic,mock,managed-catalog,actor-catalog,observed-definition}.log`:
each compiles and exits1 at its exact BoundaryAdmission, GatewayAdmission or
WorkspaceObservation refusal assertion. Disposable worktree removed after exact
restoration. Earlier `issue2627-audit-counterexamples-final.jsonl` and workspace
evidence reds remain linked in the pre-audit. Initial invalid UUID/lifetime and
reused-event fixtures were corrected without production changes; neither earns
runtime defect credit. The original structural red from historical Markdown
probe names remains recorded; two exact unrelated audit references are explicitly
counted by the existing text-classification guard, no broad exception or rename.

Independent complexity at original repair `860db22de` passes against base568dd9ab0;
all five consumers remain below the hotspot threshold, canonical validator
was unchanged before the independently gated cycle-1 correction. Fresh exact-head
complexity, guards and coverage are required for that correction and appended
before review. No baseline or guard weakening.

The lifecycle plan places all ten new roots exactly once after cycle1:
MCP2 in broad-03, tools1 in broad-05, LLM5 in api-llm-bus-full, store2 in
store-runtime-full-04. No CI-Units or proof-plan membership change required.
Final plan must bind the pushed source; a green hosted merge-ref receipt must
also retain actual execution source/GOVERSION, not infer identity from PR label.

Non-closure-bearing feedback: master onboarding's intermittent PostgreSQL
reconnect error was independently reproduced on old Go1.25 last-green source
and current master. Its exact cause and the passing-run40P01 sibling remain
unclassified under #2353; no ungated repair or waiver is included in this PR.
That diagnostic is not #2627 qualification credit.

## Cycle-1 Correction And Retained Red

Original d0ea hosted lifecycle38215894501 is RED at
TestVerifyBootStartupPredicateCensus; timing/summary are consequences of
cancelled proof, not a measured overrun. The error is this PR's omitted observer
census refresh, not inherited. The original accepted padded/null characterization
is invalid evidence; it is explicitly replaced rather than grandfathered.
Signed880d31e99 records amended pre-audit and red counterexamples before production
owner edits; watchlist amendment `swarm-docs@4f6b7e0` precedes those edits.

`internal/cliapp/testdata/issue_2627_admission_predicate_accounting.md` refreshes
A11/A18/A25 owner, both direct wrappers and every startup/activation/continued/fork
consumer, with actual P09/P15/P21 witnesses and read-only verify limits. Existing
generator changes only the measured observer body; normal census and all three
hostile ratchets qualify with update mode absent. No registry-only or checksum-only
closure. The before-owner WireShapeAdmission counterexample has27 failing leaves
plus its parent; fresh canonical/all-consumer, real both-store and compiled-host
controls provide the corrected evidence. Receipt addendum will bind the final head.

### Fresh Cycle-1 Receipt Ledger

Production repair is signed969c12511. Only the final valid-name fixture refinement
and this receipt documentation follow; that fixture has its own passing race3
receipt. All listed execution has zero failure/skip. A11/A18/A25 witnesses run
with census update mode absent. Exact current plan and complexity are re-bound
to the pushed head in the PR addendum. No duplicate Local tier/server2/full.

| Alias | Receipt / result | SHA256 |
| --- | --- | --- |
| R10 | issue2627-wire-components-969c-race3.jsonl; corrected owner/characterization, all request/MCP/observation/source/frozen/generated controls, race3 | 584cd2afe466ad0d7792db87320b9abb31ad4ca5149c23383fd6f1369ca076cd |
| R11 | issue2627-wire-store-race3.jsonl; both stores seven malformed cuts each, zero attempts/effects/HTTP, one valid same-session settlement; reconstructed two distinct turns, race3/31.969s | 66511ca69a0c44f3d11aaaef3c0424557b85f4106f35c8cd73142305fd00fe33 |
| R12 | issue2627-admission-rows-969c-race3.jsonl; inherited A11/A18/A25 profile/retained/prepared/read-only verify/census/hostile ratchets race3 | 380e07552b92420636808efd0d8a27c1fdcde1126dff1529fb81e42deab1a432 |
| R13 | issue2627-compiled-host-969c.jsonl; real compiled public host/private SQLite HTTP MCP and compiled verify/describe/source refusal/28.117s | ebafc906bc485582c2ec98687567c11d724bc1f93ca9836ffc9404bd20642027 |
| R14 | issue2627-structural-969c.jsonl; whole unchanged four-package structural guard unit | b61080a0fc0feada4fee2d3186e8aee3be8f7d316f8ccb7ccb4ef34a653a47d5 |
| R15 | issue2627-census-969c.jsonl; CLI startup census plus planner/catalog/codemod census/inventory/registry controls | 26be98a80d5ac8b48e193a8121ffdb4371557311b9a0dac37f9dd399a1fe6cd2 |
| R16 | issue2627-restart-969c.jsonl; distinct compiled H/mock SQLite/PostgreSQL restart/forced kill/34.177s | 2f71a6a9d4c8ca3d2e2e006242b344f207963e3a6e286379791c654e46aac288 |
| R17 | issue2627-valid-anthropic-final.jsonl; corrected exact full object-schema request bytes, race3 | 4a707680953d4e52ca756de6ab76512fe091a6b96a3b5f5c212709151daaa81b |

Fresh call-removal mutants compile, fail the intended wire assertion, restore
exactly and their disposable worktree is removed. Anthropic uses a proper object
root with invalid nested type, so its root guard cannot mask a removed shared
validator. Managed omission publishes bad evidence; actor omission returns bad
tools; observed omission wrongly confirms through both wrappers.

| Consumer | Fresh counterfactual receipt | SHA256 |
| --- | --- | --- |
| Anthropic | issue2627-wire-mutant-anthropic.log | 0ebcc62e1cfc33e42c4cde5293bd8e48338d720e2a8ffc8e8e9b07846c3136cf |
| Mock | issue2627-wire-mutant-mock.log | 47d854525b6b5db0a70cd6ca06bf9ab323a158f6926acf25537d702f6e703bf8 |
| Managed MCP | issue2627-wire-mutant-managed.log | fd8dd4cfef31ad725dec87c16e39daed6f653efbfd2db67371e100836fcb4975 |
| Actor MCP | issue2627-wire-mutant-actor.log | 507722e188c0a5433f2ceaa38dd61910b6a6ae118a6f209e80ffbb6cfa64b616 |
| Observed MCP | issue2627-wire-mutant-observed.log | 1ca2b9150dcd5b4aaa2ca62cb56cbb460a30db5c04add8e065e5d5a3658993fa |

Recognized-array shape references: [JSON Schema validation6.1.1/6.5.3](https://json-schema.org/draft/2020-12/json-schema-validation),
[validation meta-schema](https://json-schema.org/draft/2020-12/meta/validation);
provider root: [Anthropic input schema](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools).
These support the bounded approved recognized laws, not a new vocabulary or
blanket promise of complete vendor acceptance. Parent and architecture estimates
remain unchanged; no newly discovered sixth consumer or new semantic owner.

## Cycle-2 Fixture Construction Correction

Hosted lifecycle run38221994209 at0636c9512 is retained RED evidence: the debt
ratchet reports10689 actual sites against10688 recorded sites. The one added
site is providerSchemaStoreFixture constructing PostgresStore from a raw DB;
this is this PR's fixture debt, not an inherited failure or a semantic waiver.

The PostgreSQL arm now consumes openPostgresAuthorActivityReceiptFixture, the
existing native receipt fixture owning construction, schema admission, payload
admission, catalog registration and cleanup. It exposes the same admitted store
through completionSettlementTestStore, not a recovered concrete PostgresStore.
The existing completion fixture still owns lifecycle/session/claim setup, and
the existing typed storage observations still prove refusal and settlement.
SQLite construction, all seven malformed cuts per store, zero-send/attempt/effect
assertions, exact successor identities and two-turn cardinality are unchanged.
No new constructor, observation port, collector exception or baseline edit.

The new signed head requires fresh both-store provider effect/reconstruction
proof with race3 and the actual debt ratchet against568dd9ab0 before the one
batched push. Results and receipt digests are recorded in the PR-level addendum;
hosted lifecycle must then qualify that head. The37-row semantic accounting,
five production consumers and parent/watchlist decisions remain unchanged.
