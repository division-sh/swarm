# Pre-Implementation Coverage Audit: #2508

## Implementation Status

The approved A/D/C extraction and A14 repair are implemented. The separate
`issue-2508-postimplementation.md` maps all 45 original/additive rows to named
execution proof, including compiled public consumers on both stores. Default
managed qualification passed all 14 required units on 2026-10-01; exact-head CI
and independent merge review remain required. Historical stops below are
retained as audit history, not the current implementation disposition.

## A14 Bounded Absorption Approved / Pre-Code Amendment

Binding independent re-gate `issuecomment-5928433996` approves absorption in
#2508. This section supersedes the historical stop below. Category is now
high-risk semantic maintenance for A14, in addition to the separately governed
behavior-preserving A/D/C extractions. No new issue/PR, owner, framework,
compatibility path or vendoring. This permission is not merge approval.

Chosen added working class: all four operational CLI read entrances fail to
carry exact run authority required by the canonical API. Immediate parent:
CLI/API exact operational identity authority; broader lifecycle/model debt stays
in #2250. #2447/#2407 stay open for their other maintenance/proof families.
The endpoint failures were entry points, not the class boundary. This complete
four-entrance class is feasible in one bounded repair with existing owners.

The semantic owner remains `resolveOperatorAgentIdentityParam` and the typed
selected-store `ResolveOperatorAgentIdentity`; CLI input validation/forwarding
only supplies explicit authority. Each entrance below moves to correct
consumption of that existing owner, without changing API/store interpretation.
Full means exact supplied opaque run ID, never prefix-resolved/latest/ambient.
Agent prefix resolution remains declaration-backed presentation only; retries
must retain the already supplied run and optional concrete flow instance.

| Separate manifestation | Consumer / change | Exact planned proof |
| --- | --- | --- |
| A14-V agent view | `newAgentViewCommand` / `runAgentViewCommand` adds required explicit --run-id; validates before client construction and forwards run_id with agent_id/optional flow_instance. Old no-run producer is invalid. | Compiled real SQLite/Postgres text/JSON/quiet success; missing/blank/malformed scope exit2 and zero RPC/client-side token read; foreign run and ambiguous-target refusal; exact same-run prefix retry; refs-only output controls retained. |
| A14-D agent diagnose | Existing `agentDiagnoseCommandOptions.params` adds required explicit --run-id before API construction; queue validation/order and output/error taxonomy retained for valid scope. Old no-run diagnosis request is invalid. | Both-store compiled text/JSON/quiet success/refusal; local zero-RPC scope tests, queue bounds/cursor precedence, wrong-run/ambiguous refusal and same-run prefix retry; diagnosis privacy controls retained. |
| A14-L agent deliveries | Existing --run-id becomes required exact operational authority, not optional history filter; retain status/limit/cursor/flow-instance behavior. Old unscoped request is invalid. | Both-store compiled success; missing/blank/malformed/wrong run, same-run prefix retry and ambiguous-target refusal; existing lifecycle output/pagination/cursor tests with explicit valid scope. |
| A14-C conversation list --agent-id | Existing run option becomes conditionally required for agent_id (flow_instance already requires agent_id); retain unfiltered and run-only enumeration. Unscoped agent-filtered request is invalid. | Both-store filtered compiled text/JSON/quiet success; missing/invalid/wrong-run/ambiguous refusal; unfiltered/run-only positive controls and session-ID readers stay executable; exact option forwarding. |

Exhaustive relevant siblings: effective `agent.frame` and mutating
restart/replay/directive already consume explicit run selectors; their owner is
unchanged. Conversation view/turn consume exact session/turn identity, a different
concept with passing real compiled controls. `agent.list` is declared inventory
(#1562), not operational run selection; no run filter is added. API usage/richer
delivery reads already require exact authority and have no additional CLI
entrance in this class. Repo-wide method call sweep found only the four producers
above; no surviving same-concept producer is intentionally retained.

Authoritative spec plan in this PR: update `cli_specification.commands`
agent_view, agent_diagnose, agent_deliveries and conversations_list syntax,
behavior and proof expectations; add view/diagnose run input rows to the existing
identifier registry with `full_only`, annotate required/conditional authority;
clarify `conversation.list`'s conditional run_id rule in OpenRPC description.
API parameter requiredness and strict store resolver remain unchanged. Correct
only directly contradictory existing CLI test scope expectations, the sole
waiver to the prior existing-test immutability rule; preserve their prior valid
output/error assertions. Test-edit inventory must be explicit at final review.

Tracker decision: absorb all four rows into existing #2508; no prerequisite or
new tracker. Watchlist: refine existing boundary/coverage mapping to approved
absorption, retaining baseline counterexample and C11 evidence. Architecture
feedback is explicit authority carriage through existing owners, not a new
selection abstraction; no broader parent closure is claimed. Stop only if a
new same-concept production consumer or API/store semantic change is needed.
All original A01-A13/D01-D11/C01-C12/Q01-Q04 proofs, final ratchet/line cap,
managed suite, exact-head CI and PR proof audit remain binding. A14 has four
named child proof rows rather than one undifferentiated success claim.

## Historical A14 Spec Contradiction / Implementation Stop

The C11 correction below is approved and its required public proof now passes.
The subsequent compiled A14 proof hit a different, explicit original-gate stop
condition. Implementation and closure qualification are paused pending a bounded
lead/reviewer ruling; the maintenance scope has not silently absorbed CLI semantics.

`TestReadProofFactoringCompiledSurfaces` runs real public RPC and compiled CLI
reads over the existing retained MockOnly lifecycle harness on both stores.
All nine RPC reads with exact run/session authority succeed. Agent view and
diagnose fail in text, JSON and quiet because their CLI requests carry no run_id.
Both commands also expose no run-id flag. The same probe on disposable untouched
master `1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f` reproduces the failures.
Explicit-run agent deliveries, unfiltered/run-filtered conversation list,
conversation view/turn and declared-agent list succeed on both stores.
The follow-on probe confirms the same refusal for default agent deliveries and
agent-filtered conversation list without run authority on both stores. Each
fails in all three supported output modes. Both candidate and untouched-master
complete probes report the same 24 failed / 26 successful compiled cells,
plus 18 successful explicit-authority RPC reads. Probe times are
24.967s / 24.334s. Authority must not be inferred from declaration names,
sessions or ambient CLI state.

Binding contradiction: `cli.commands.agent_view` and `agent_diagnose` describe
agent-id-only requests (platform-spec.yaml near 27167/27222), while OpenRPC
`agent.get`/`agent.diagnose` require run_id (near 35981/36066), enforced by
`resolveOperatorAgentIdentityParam`. The API's fail-closed exact-run owner is
not a defect. Existing fake-RPC CLI fixtures cannot prove the integrated success
originally promised by A14. #1562 is a different declared-agent inventory
decision, not authority to fix operational detail selection here.

Requested disposition: independently classify and track the operational CLI
run-authority contract before authorizing a bounded repair or corrected
behavior-preservation proof. Do not weaken API admission, add ambient selection,
use dashboard/store fallbacks, or silently add a feature to this maintenance PR.
No new tracker or semantic fix is claimed approved. The four read entrances,
explicit-run controls, session-scoped readers, effective frame and mutating
agent consumers are part of the focused sibling census.

Earned local proof, not full closure: compiled C11 three authored fresh public
MockOnly scenarios and both unsupported flags pass (14.796s). C10 human/proposed
real 201-card/ambiguity/foreign-codec reads pass on both stores (8.154s). D10
matches the committed pre-extraction normalized output hashes for all 45 cells
and 90 public invocations (81.88s); repository path is the only normalization.
Focused API/CLI controls pass (7.567s/20.465s). Existing tests stay unchanged.
Golden/default-suite, final complexity snapshot and normal review PR remain
outstanding. The red A14 counterexample is not a passing coverage row or a
full-suite failure caused by extraction. Preserve it until the ruling resolves
the actual contract; no review-ready or parent closure claim is made.

## C11 Supported-Mode Correction Approved

Independent ruling `issuecomment-5927851651` approves the bounded correction
and resumes implementation; no other proof or scope condition is waived.
`newTestCommand` binds no shared output flags: public `swarm test`
has text output, but neither `--json` nor `--quiet` is supported. C11's original
wording must not be interpreted as authorizing either new feature. The requested
bounded correction is successful compiled MockOnly text execution for all three
anchors, plus exact nonzero exit/diagnostic and no-session-acquisition proof for
both unsupported flags. Describe and operator JSON/quiet proofs remain required
where supported. No owner, production behavior, class boundary or proof waiver
is proposed. Future JSON/quiet support remains separately tracked in #1712.
Real authored scenarios must exercise the compiled fresh-session command for
all three anchors; fake RPC or a retained served harness earns no C11 credit.

Characterization was committed before production extraction at `8d2ad0f2f`,
against unchanged merged production. New A capability/order/independent-capability,
D full/sparse transcript/current-field, and C all-anchor field/admission/cancel
tests pass, as do unchanged operator/describe/complete-match-set controls.
Local extraction head `7c2dfec74` also passes those controls and unchanged
userfacing, API-spec, authoringview and routingtopology suites. It is an incomplete
implementation head, not review-ready or a closure claim. In particular, the
flow-detail extraction still needs its original inline body removed in favor of
the new helper; do not credit unused extraction as complete factoring.

Exact measurements currently show cyclo >=30 279 -> 276 and cognit >=30
589 -> 588; all new extracted callables have cyclo <=22. The checked-in
complexity snapshot still requires regeneration after completing extraction;
the exact-head admission command therefore does not yet pass. Supported
compiled/both-store additions, golden, full managed suite, PR and proof audit
remain outstanding. Existing tests are unchanged; no vendoring occurred.

## Approved Implementation Boundary (2026-10-01)

Independent gate `issuecomment-5927450842` approves all three complete A/D/C
maintenance classes on `1c4cce209`, superseding the dependency hold below.
Existing tests remain byte-unchanged; commit new characterization before
production extraction. Every extracted callable must remain below cyclo 25,
the pinned base/head ratchet must pass, and the separate 3000 production-line
cap remains binding. No common facade, semantic owner, compatibility path,
framework or vendoring is authorized. Permission to code is not merge approval.

### D11 Owner-Map Addendum

The `expand_minimize_tooling` paragraph incorrectly requires output-pin
`key`/`carries`, contradicting the newer compiled-pin ownership contract.
Current `OutputPinView` deliberately exposes neither retired field. Existing
event-schema/business-key and compiled-connection owners remain authoritative;
`authoringview.Build`, full describe and routes consume their projection.
Correct only that obsolete spec phrase in this PR; do not restore declarations,
JSON fields, pin-owned selection, or renderer-side interpretation. This feedback
is attached to existing #2438 (`issuecomment-5927438523`) and mapped to existing
maintenance/coverage watchlist nodes, not a new issue or framework.

| Manifestation | Planned exact proof |
| --- | --- |
| D11: obsolete output-pin tooling wording | Pre-extraction exact describe transcript; public text/JSON/quiet/routes comparisons on current compiled sources; explicit absence of retired output-pin fields; unchanged authoring/topology/source guards. |

The proof population is now 41 rows: A01-A14, D01-D11, C01-C12, Q01-Q04.
All original owner-consumption and closure requirements below remain binding.

## Current Post-#2505 Delta Gate Submission (2026-10-01)

**Agent-g; audit only; independent reviewer-g gate pending.** This section
supersedes the intake dependency/baseline statements below, not the three
distinct complete classes or their 40 planned A/D/C/Q proof rows.

PR #2505 merged on 2026-10-01 at 07:50:50 UTC as
`1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f`, now the exact fetched
`origin/master` analysis baseline. Its accepted review head was `ab707f89b`.
The audit branch was clean before rebasing; the old head is preserved as
`backup/agent-g-2508-preaudit-dced47dd2`. Rebase completed without conflict;
`git range-diff cf3e85023..dced47dd2 1c4cce209..5762d8bb5` proves the sole
original audit commit unchanged. No #2008 WIP or other agent worktree was changed.

This fulfills the dependency-integration portion of the independent delta ruling
`issuecomment-5923949475`. **It does not grant coding permission.** Request one
reviewer-g delta ruling, not a new concept-wide audit cycle or first-slice scope.

### Current Eligibility And A/C Drift Check

Exact canonical measurement at `1c4cce209` passes using the unchanged pinned
analyzers. Both populations now have **19026** callable rows: cyclo >=30 **279**,
>=50 **57**, maximum **184**; cognit >=30 **589**, >=50 **198**, maximum **302**.
The three candidates still exist at the same lines with unchanged values:

| Family / candidate | Cyclo / cognit | Delta disposition |
| --- | ---: | --- |
| A / OperatorAgentConversationHandlers | 82 / 212 | Same nine methods, separate typed read capabilities, identity resolution, result admission and error/store-call ordering. |
| D / writeDescribeText | 65 / 256 | Same renderer branches; current input projection differs as enumerated below. |
| C / scenarioRunner.findDecisionCard | 56 / 84 | Same two-action caller, eleven selector fields, registered anchor admission, complete validated cursor traversal, exact-one detail/hash and downstream mutation. |

`git diff --quiet cf3e85023 1c4cce209 --` the three candidate files, operatorread,
selected-store operatorsurface, control_mailbox.go and the human-code projection
guard returns zero. Production caller search still identifies serve's API mount,
one describe text callback and one mailbox-action selector caller. No new A/C
consumer, alternate interpreter or same-concept bypass appeared. API entity and
event-publication tests changed in #2505 for the separate grammar/ingress
contract, not A's read methods or C's match policy. Those retained semantic
owners remain different concepts, not shared factoring logic.

All original family-specific ownership, unchanged-test, under25 and exact-ratchet
conditions remain binding. The new callable count / three-count cognitive drop
belong to merged #2505, not claimed #2508 factoring credit. Parent-tail grouping
is unchanged; this delta does not independently qualify the other children.

### Refreshed D Owner / Consumer And Branch Inventory

Binding current spec sections were re-read, including:
`flow_model.flow_instance_authoring.effective_flow_mode_model` (line 12002),
`.analyzer_obligations.expand_minimize_tooling` (12490),
`flow_model.compiled_event_schema_projection` (12709),
`flow_model.composition_routing.w2_compiled_pin_edge_ownership` (12884),
`.authored_shapes.resolved_input_pin` (12926), and output registry
`cli_specification.foundations.output_contract.command_support.output_conformance_registry.rows.{describe,describe_routes}`
(24709/24719). API and scenario binding paths remain unchanged; their line numbers
shift to agent.list 35952, conversation.list 36541 and scenario selected surface
22320 / source authority 22357 / mailbox actions 22623 and 22635.

| Current owner | Complete relevant consumption / delta |
| --- | --- |
| FlowSchemaDocument.EffectiveMode | authoringview.buildFlows now consumes this owner: scalar instance derives template, omission derives static; authored mode is invalid on presence. describe consumes FlowView.Mode as projection only. The exact writeDescribeText flow-Mode guard exception remains untouched. |
| Primary-entity, template-instance and singleton-coordinator resolvers plus BuildSingletonCoordinatorDemandProjection | authoringview projects scalar identity and evaluates a coordinator only for static shape with actual contained-state demand. Stateless/unused-field cases do not invent coordinator errors. The text renderer consumes the optional projection; it must not restore a singleton shape or recompute demand. |
| CompiledFlowInputPin / CompiledConnectionInput / ConnectRoutePlan and effective compiled event-schema owner | inputPinViews retains pin, schema, initialization and retained fan-in/reply evidence; ResolutionFrom is removed. Ordinary create/select/select-or-create and exceptional key_from belong to the connection. authoringview/routingtopology/full describe/routes consume bound evidence, never a pin-owned fallback. |
| authoringview.Build and BuildRoutingTopologyWithReport | All full-text/JSON/quiet/graph and routes-only consumers still use the admitted structural source and these existing projections. No new semantic owner or source/readiness classifier was added. |
| Existing diagnostic and shared CLI-output owners | Admission/diagnostic text, source locations, structural/not-evaluated metadata and output modes remain unchanged by this factoring boundary. #2505's new source validity and JSON projection are the baseline, not behavior for G to preserve from the retired grammar. |

| Planned D row | Current shape / branch obligation after integration |
| --- | --- |
| D01 | Nil/empty/root transcript and root primary-entity/event fields unchanged. |
| D02 | Exact flow labels now reflect effective static/template only; preserve template identity, optional ingress, demanded coordinator positive/error and stateless/unused-map negative branches. Existing owner tests execute all coordinator distinctions; compiled stateless/template readback is captured below. |
| D03 | Graph nodes/edges still consume lowered owners; exact event/rename projection and bound receiver membership are #2505's current inputs, not re-derived in the renderer. |
| D04 | Timer/join/fan-in branches remain; pin-level resolution is only retained fan-in/reply policy, never ordinary instance selection or ResolutionFrom. |
| D05 | Fan-out/gate branch ordering, multiplicity metadata and outcomes unchanged. |
| D06 | Existing typed diagnostic formatting/indentation remains; characterize current retirement teaching errors, not accepted authored mode or pin-owned selectors. |
| D07 | Full describe/routes must expose the same current immutable connection/topology projection, including derived or exceptional key source and actual retained fan-in evidence. |
| D08 | Text/JSON/quiet/no-color baseline is refreshed. InputPinView JSON no longer has resolution_from; flow mode is derived. Do not reintroduce removed fields or compare to pre-#2505 JSON as authoritative. |
| D09 | Structural/source admission now rejects authored mode and retired ordinary pin selection. Preserve validation-before-presentation, findings and exit behavior; no readiness, credential or runtime reinterpretation. |
| D10 | Current compiled source cells cover static/template, payload versus intrinsic key, standing stateless ingress, retained fan-in and the golden source, with exact repeated transcripts and full/routes JSON equality. Exhaustive new characterization tests and post-refactor compiled proof are still required, not declared complete by this delta. |

No candidate disappeared, new required consumer appeared or semantic scope
changed. Existing A/D/C owners and the original chosen-class closure commitments
remain correct on this merged baseline. No spec change by G is proposed here.

### Post-Merge Pre-Extraction Evidence Actually Executed

All following runs use unmodified merged production and existing tests; no
existing test edit, extraction, behavior repair, workload relaxation or full-suite
claim follows.

| Execution | Actual result / evidence |
| --- | --- |
| Original focused A/C/describe controls, same selection as intake, count1 | PASS apiv1 4.083s / cliapp 15.947s; `/tmp/agent-g-2508-post2505-targeted.log`. |
| Verbose real SQLite/PostgreSQL operator and complete-match/201-card public-owner controls, count1 | PASS apiv1 2.781s / cliapp 5.219s; both backend leaves execute, no skips; `/tmp/agent-g-2508-post2505-public-owners.log`. |
| Existing authoringview projection/shape/coordinator, API-spec and human-code source guards, count1 | PASS authoringview 2.402s / apispec 0.531s / userfacing 11.362s; `/tmp/agent-g-2508-post2505-projections.log`. |
| Existing routingtopology TestBuild family, count1 | PASS 0.245s; `/tmp/agent-g-2508-post2505-topology.log`. |
| Exact canonical complexity snapshot / baseline check | PASS; `/tmp/agent-g-2508-post2505-complexity/{head.json,delta.json}`. |
| Compiled public describe/routes pre-extraction characterization | PASS **45 cells / 90 invocations**, five existing sources x nine output surfaces x two identical repeats. Each exits0 with empty stderr. `/tmp/agent-g-2508-post2505-describe/manifest.json` preserves actual commands, per-cell exit and stdout/stderr SHA256; raw transcripts are adjacent. |

Characterization sources are current existing template-select-or-create,
template-create-minted-key, telegram-agent, fan-in/barrier and the golden workload.
Surfaces are describe text/JSON/quiet/graph-text/graph-JSON/no-color and routes
text/JSON/quiet. The compiled binary was built from this integrated tree with
`go build -o /tmp/agent-g-2508-post2505-swarm ./cmd/swarm`; a scratch audit script
`/tmp/agent-g-2508-post2505-describe.rb` invokes only the public command, compares
both actual transcripts/exit codes and asserts full/graph/routes topology equality,
structural/not-evaluated metadata, exact effective-mode text, template identity,
retired pin field/policy absence and stateless coordinator absence. It never
calls runtime/private read helpers or starts provider execution.

An initial public invocation was correctly refused because the inherited test
DSN is quarantined production environment. That observation is not credited as
a pass or a separately archived failure receipt; the initial scratch output
paths were reused by a successful invocation before the retained matrix capture.
Actual public calls explicitly unset SWARM_TEST_POSTGRES_DSN; targeted Go
selected-store proofs retain the canonical test DSN. No admission bypass or
global environment mutation was used. No Telegram/Claude call or live execution.

These captures are baseline proof, not committed exhaustive characterization
tests, post-extraction D10 closure, provider/restart proof or whole qualification.
The promised new regression tests still must precede production extraction after
the independent gate. No goldens or full swarm-test were rerun in this audit pass.

SHA256 receipts: targeted log
`63413ad76882f1428ea6effb158ebafc39b4fe74e1e83a8b999195ae611ab3e3`;
public-owner log `f1d191847b509792a932e12c91fdf0a46a5dd4a0c584187ccd4b6551cd814cfa`;
projection/guard log `5f643b4711fcf683be55947ba52bcb3e7aabf9f6ebe455c7d6b1f51e1a130f61`;
topology log `d5c75d8faaba927b6773d49183d214d56095f970961e70a2d76a90bc3b2e34e3`;
head metrics `a936bc79df6952518c879a72a00fd897512829c721ced060c352306a63606221`;
delta `2476a310d9e4477ba974102e8f7266da29713199337f44d850bef97b2e3da347`;
compiled binary `b848e860f04e3aef93e68b783801e513e93e667d02a6249bdf291977c5746268`;
scratch script `6d093b17b9919bfee67699a55ebb048ada21058169ae9e4c156fc88b4fc7eef5`;
compiled manifest `e8eb3cda6bd4aba01b78826bfd9eb3a6fd802a637baf767f527902ae05f7d0ee`;
compiled log `d1096f9fdc6b38ac5a82bf4a2f0931a06ebd28fb645a3573ef5a7205ea6cd0fc`.

Tracker decision: update #2508 and existing watchlist nodes with dependency
consumed and this exact current projection/proof boundary. Keep #2447/#2407 and
#2250 open. No new issue or architecture disposition; the original watchlist-only
maintenance feedback remains. Request reviewer-g's final **independent delta gate**
on this integrated artifact. Coding stays frozen pending an explicitly recorded
outcome. The old external dependency refusal below is historical, not current.

## Historical Intake Audit (Superseded Baseline/Dependency Only)

Agent-g. Analysis baseline: `cf3e8502379c7ac3affeac782a4084f45a4bf9ff`
(merged #2507); docs baseline: `4c7452b`.
**Audit only. Independent gate requested, not granted. No production or test edits.**
#2505 remains OPEN at `13e946608c9b8986012bca023ba88d721b4a6c36`.
Consume its merged projection/fixture/spec changes and refresh characterization
before implementing this batch. The separate dirty #2008 worktree is untouched.

## Binding Context And Class Model

Category: **high-risk behavior-preserving maintenance**. Full #2508 body/thread
(no comments at intake), #2447 body/thread including the four-batch lead handoff
`issuecomment-5923421059`, and #2407 R1.4/program rules are binding. Re-read
IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md in the docs repository.

No exact platform-spec section mandates local factoring or a cyclomatic ceiling.
The issue/lead maintenance contract governs that obligation; these exact semantic
sections govern the behavior that must not change:

| Exact `platform-spec.yaml` reference at this baseline | Preserved authority |
| --- | --- |
| `api_specification.method_catalog.agent.{list,get,diagnose,delivery_lifecycle,usage,delivery_diagnostics}`, lines 35927-36310 | Exact run/concrete-agent identity; separate typed read capabilities; queue/status/usage/diagnostic semantics; public limits/errors/cursors; no transcripts or local fact reconstruction. |
| `api_specification.method_catalog.conversation.{list,list_turns,get_turn}`, lines 36516-36630 | Bounded summary/turn pages; exact session/turn identity; safe detail/frame only; no provider-private contents or log joins. |
| `api_specification.components.schemas` AgentSummary/Detail/Diagnosis/DeliveryLifecycleList/Usage/DeliveryDiagnostics and ConversationSummary/TurnListPage/TurnDetail | Wire fields, arrays versus null, optional fields, typed failures and closed result schemas. |
| `flow_model.flow_instance_authoring.analyzer_obligations.expand_minimize_tooling`, line 12479, including `.routing_topology` | `authoringview.Build` and lowered semantic owners supply the projection; graph, topology and diagnostic rendering cannot become interpreters. |
| `cli_specification.foundations.output_contract.command_support.output_conformance_registry.rows.{describe,describe_routes}`, lines 24684-24701; structural admission rule at line 8749 | Same admitted structural source as verify; `validation_scope=structural`, `live_readiness=not_evaluated`; retain findings for invalid sources, no credential-dependent live readiness claim. |
| `test_specification.deterministic_scenario_runner.selected_public_surface` and `.source_authority`, lines 22284-22347 | Fresh command-owned MockOnly session, readiness/source matching, public RPC-only execution; no signed-ingress/retained-restart credit from the public command. |
| `test_specification.deterministic_scenario_runner.action_steps.{mailbox_decide,mailbox_defer}`, lines 22598-22620 | Exact-one pending card across every page; cursor unchanged and filters retained; later failure/repeated/cyclic continuation refuses before get/mutation. |
| `api_specification.method_catalog.mailbox.{list,get,decide,defer}` and typed decision-card schemas | Registered anchor union; public list/detail validation and stale-content fence; mutation remains owned by the existing card/anchor owners. |
| `cli_specification.command_catalog` agent/conversation readers and `test`, lines 27125-27687 and 25615-25710 | Public client only, exact selectors/prefix rules, output modes and exit behavior; no direct DB/dashboard joins. |

Read the sections themselves, not just this table. #2505 changes adjacent flow
shape/input projection semantics; its merged contracts replace the corresponding
baseline assumptions. No semantic spec delta is proposed by #2508. Any discovered
code/spec contradiction requires disposition before production edits.

The observed symptom is complex nested dispatch/render/filter code, not a proven
runtime failure. Original functions are audit entry points, **not audit boundaries**.
The chosen working class is the explicitly lead-batched product of three distinct
maintenance classes; it is NOT one shared semantic owner:

| Subfamily | Exact concept / chosen class | Immediate parent / broadest parent |
| --- | --- | --- |
| A: operator read family | `operator_agent_conversation_read_dispatch_mixed_validation_read_presentation`; all nine mounted methods and capability combinations, not only agent.get. | #2447 keep-zone control-flow maintenance; #2407 executable regression/merge proof. |
| D: describe renderer | `describe_text_sections_mixed_in_one_renderer`; every existing text section/branch and untouched machine/quiet consumption, not a new authoring model. | Same maintenance parent, distinct from runtime topology/lifecycle #2250. |
| C: scenario card selector | `scenario_card_selection_mixed_admission_pagination_and_detail`; both actions and all three anchor kinds through the full public match set. | Same maintenance parent, distinct from durable mailbox mutation/lifecycle ownership. |

Framing: **broad enough for the three complete named subfamilies**. Lead explicitly
waived one-family-per-PR, 1500 gross changed-production-line ceiling and net-neutral/
negative production lines for #2508-#2511. No semantic, proof, test-preservation,
under-25 per-factored-function, exact-head ratchet or vendoring waiver follows.
The ordinary #2407 3000-added-production-line policy is not silently waived.

Intended closure: **failure class eliminated for each of these three bounded
maintenance classes**, independently proven; no runtime/model/parent closure.
One PR must close all three or obtain explicit pre-coding reclassification. A
short wrapper or lower total alone is not closure. `Closes #2508` is appropriate
only then; `Part of #2447` and `Part of #2407`, never closes either parent.

## Eligibility, Current Census And Minimal Design

Canonical measurement `go run ./cmd/swarm-complexity -head cf3e85023 -evidence
/tmp/agent-g-2508-complexity` passes. Unmodified pinned gocyclo v0.6.0/gocognit
v1.2.1 remain score owners: 19010 callable rows each; cyclo >=30 **279**, >=50
**57**, maximum184; cognit >=30 **592**, >=50 **198**, maximum302.
Historical salvage_map.csv/gocyclo_prhead.txt remain absent from tracked docs.
The replacement eligibility table below needs independent ratification; high
scores/package adjacency do not substitute for that gate.

| Candidate / dependency | Cyclo / cognit | Direct baseline execution / proposed disposition |
| --- | ---: | --- |
| OperatorAgentConversationHandlers, operator_read.go:299 | 82 / 212 | Existing handler/typed-error/malformed-owner tests and real both-store HTTP read probes. Keep the exported mount; extract nine unexported per-method factories, retaining per-method ordering and independent capability selection. |
| Its existing diagnosis/usage validators | 21 / 21 cyclo respectively | Existing hostile owner-data tests. Retain these actual admission helpers; do not move/duplicate invariants merely to claim factoring. |
| Its identity/options/error helpers | All cyclo <25 | Preserve current run-scoped resolution, error detail precedence and limit/default handling; same typed owners, no new read facade. |
| writeDescribeText, describe.go:216 | 65 / 256 | Existing describe text/JSON/graph/ingress/diagnostic tests. Extract flow details and graph node/edge/timer/join/fanout/gate/diagnostic sections as local render functions. |
| writeRoutingTopologyText / writeDescribeEvents / describeQuietValues | 21 / 4 / 4 cyclo | Already bounded shared projections; retain. Full describe and describe routes must continue using the same topology renderer. |
| scenarioRunner.findDecisionCard, test_command.go:2276 | 56 / 84 | Existing complete-match-set HTTP matrix and 201-card real API both-store test. Separate match evaluation/admission, card predicate, full traversal and exact-one detail; preserve their sequence. |
| runMailboxStep and shared mailbox validators | Existing direct action/public validation tests | Keep the one caller/mutation sequence and existing validators. Extract no second mailbox codec, anchor registry, selection facade or paging framework. |

The human-code source guard records exactly one flow `Mode` exception in
`describe.go`/`writeDescribeText`. Preserve the small flow heading/Mode rendering
there while extracting other sections; do not weaken or edit this existing guard.
Do not normalize/sort match maps as an incidental extraction: competing map-field
errors currently have Go map iteration semantics. Characterize deterministic
precedence between phases and single-invalid-field cases without inventing an
exact ordering between independent invalid map entries.

Forecast only: roughly 1100-1600 gross changed production lines across the three
existing files, about +60 to +180 net for explicit local steps. Actual additions,
deletions, net lines and each helper's purpose must be reported from the PR head.
No requirement to pad deletions or split a coherent path to satisfy a waived cap.
Every new/extracted callable <25 cyclo; update the artifact and independently
compare exact base/head with both >=30 counts nonincreasing. No score suppression,
renaming-only credit, policy reset or replacement analyzer. Existing tests stay
byte-unchanged by this work; only new characterization/public tests are authorized.

### Coordinated Parent Census (Not Closure Of Other Children)

Fresh historical/successor census covers the complete #2447 original target table,
not just the three requested functions. All four child bodies are read. This is
the #2508 portion of the coordinated census; other children still need their own
owner/consumer/proof gates, not this issue's approval.

| Historical/current target | Exact current cyclo / disposition |
| --- | --- |
| data.show router/resource/operation; dead pager cluster | Merged #2507 proof closes its maintenance class: 6/18/13 and six obsolete helpers absent; no new factoring credit. |
| OperatorAgentConversationHandlers; writeDescribeText; findDecisionCard | 82/65/56; all included in #2508, no candidate silently dropped. |
| validateAdmittedToolInputSchemaActive; validateToolSchemaValue | 69/55; #2509 schema half, distinct definition/value owners. |
| normalizeProviderTriggerSubject; normalizeSubject | 54/50; #2509 pack half; #2482 still open, consume merged result before its gate. |
| ClaudeCLIRuntime.continueSession; Executor.execReadResourceData | 68/60; #2510 distinct provider-turn and actor/run-pinned tool-read families. |
| PruneOperationResult.Validate; validateSourceOperationResult; RunCreationOperationRecord.Validate | 64/38/32; #2511 complete aggregate-validation family. |
| serveapp.Run / buildRuntimeComposition | 37/184; explicit R5/process-lifetime exclusion under open #2411/#2250, not eliminated by a wrapper and not retargeted into a mechanical batch. |
| Service.drive / driveLocked | Current successor driveLocked76; active #2241/#2482 owner, not maintenance closure or permission to factor concurrent code. |
| decodeWave1FieldNode / decodeWave1FieldValue | Old declaration absent; successor67 remains in active #2438/#2505 grammar work. Absence of the old name is not proof of successor closure. |

The 57 current >=50 rows also include separate typed-operation, engine, runtime,
pipeline, fork, store-bootstrap and development-tooling classes below. None is
promoted by high score into the keep-zone table; the existing architecture owners
are still live or need their own current gate. This is explicit non-credit, not
pretending the four batches close every >=50 function in the repository:

- Contracts/engine/value/expression: CompiledTransition.Validate57;
  EntityAssignmentAnalysis.transfer94; Executor.stepJoin54;
  entityruntime.normalizeValueForType51; workflowOptionalReadAnalyzer.validate50.
  Existing lowered transition/join/value/assignment owners and R4/R7/model lanes
  (#2410/#2413/#2349) govern these different concepts, not CLI presentation.
- Stateful authority: destructivereset.Operation.Validate66;
  effects.Authority.Valid63; Controller.Authorize60; fanoutobligation.ListPage.Validate55;
  inbound.handleResolvedWebhook77; llm.settleCompletionTurnWithProviderHead53.
  Exact reset/effect/ingress/obligation/provider-settlement owners remain binding;
  their architecture assessment stays under #2250/#2349 or the relevant child
  census. #2510 must explicitly classify its adjacent completion authority rather
  than claim it closed by moving continueSession.
- Manager: reconcileDynamicFlowRuntimeReadinessOnce92; replaceLoopLocked57;
  launchExecutionLoop58; replaceExecutionTargetConfigWithTopology70;
  PrepareDurableTopologySourceSetRebind90. R5/#2411/#2496 and #2250, not keep-zone
  extraction; current construction ownership is E's live lane.
- Pipeline: FreshActivityRequestLineage61; prepareMutation51;
  executeNodeContractHandler52; WorkflowEngineMutationCommand.Validate51;
  claimAndServeFanOutTurn65; handleWorkflowStageTimerFire50;
  reconcileInitialEntryDeclarations54. R4/R5 plus existing pipeline/timer/fanout
  owners; no runtime rewrite, schedule or work eligibility change here.
- Fork: ValidateFanOutPendingReplayAdmission66; ActivateSelectedContractRunFork67;
  selectedContractForkLocalRuntimeContainer.Publish70; applyRunForkDeliveryEventReplay51;
  requireExactMaterializedRunForkFanOut51; materializeRunForkForSelectedContractExecution84;
  admitRunForkTerminalBarrierHistory76; recoverSelectedForkTx102. Selected-fork
  execution/recovery is a port/semantic owner family, not operator-safe read factoring.
- Root/store: prepareStartLocked71; newRuntime69; authorizeFlowReadinessMutation55;
  persistLifecycleDiagnosticTx57; foldFanOutIntentTerminalDispositions64;
  newPostgresStoreComposition53; newSQLiteStoreComposition54; platformTableOrder78.
  R3/R4/R5 and #2250 exclusions remain; no new facade/transaction/lifecycle owner.
- Authored proof/development support: CopySelectedForkReadiness60; BindExecution61;
  RunPlan.Validate53; AttachJobEvidence52; replayconformance.Project73. These are
  measured authored support, not historical salvage keep approval. Existing proof
  planning/timing/fixture owners remain separate; no extra batch secretly assigned.

If another child's audit finds an eligible unowned keep-family, repair #2447 and
the affected child before coding it. Broad parent closure is **unproven**, not
inferred from this initial classification. Remaining named tail after #2508:
three child PR containers (#2509-#2511), five distinct semantic families; medium
confidence in grouping, low confidence in total landing count until their gates
and moving #2482/#2505 baselines settle. Replace the stale 11-13 historical queue
estimate with this conditional queue, not an unconditional four-PR parent exit.

## Full Execution Paths And Gate Classification

### A: Public Operator Reads

Serve selected store/bootstrap and context publication -> canonical capability
mount -> `/v1/rpc` listener/auth -> parse/registry/transport/unknown-param admission
-> per-method validation -> exact identity resolution where required -> bounded
typed selected read -> existing result admission -> shared error/wire response
-> CLI/client validation/output.

Before a read is reachable, serve/store/schema/auth and capability mounting must
succeed. Startup/work admission/auth are **different semantic concepts**: their
owners are serve composition/selected store and apiv1.Handler; public API tests
and compiled serve regression prove them, not mock handler output. Capability
selection, per-method order, typed read delegation, existing integrity checks,
error mapping and response are **same chosen A class**. CLI prefix/formatting
owners are **different concept with proof**, retained unchanged as consumers.
No lifecycle mutation or provider execution is needed to factor a read.

Order constraints: require store before direct-handler params; required agent_id
then run_id/optional flow_instance then resolver then method filters; usage since
then until/ordering; diagnosis queue_limit then queue_cursor; diagnostics
failure_limit then dead_letter_limit then failure_cursor then dead_letter_cursor;
lifecycle statuses then limit then cursor. Conversation list parses filters/cursor/
limit before optional agent resolver; no agent filter means no agent capability
is demanded. get_turn reads session_id then turn_id before exact detail read.
Outer HTTP descriptor validation happens earlier and is independently preserved.

### D: Describe

Invocation root/flags/config -> source load -> admitStructuralSource -> structural
verifier report -> authoringview.Build(IncludeStageGraph) -> shared output owner ->
text sections OR JSON projection OR quiet values -> stdout/stderr/exit.

Root/config/source admission, lowered semantic view, verifier and graph/topology
construction are **different concepts**, proved by existing source-selection,
verify/describe and authoring-view controls; #2505 is **explicitly tracked adjacent
dependency**, not absorbed grammar work. Existing text section branching is
**same D class**. Machine/quiet output is unchanged shared-output consumption.
Ordering: header/authority/readiness, root events/entity, flows, topology, optional
graph sections (nodes, edges, timers, joins, fan_out, gates), then diagnostics.
No sorting of input slices, new section, whitespace/trimming/punctuation change,
credential lookup or runtime/readiness query is authorized.

### C: Scenario Mailbox Actions

Command/source/scenario/payload/mock admission -> fresh private session readiness
and exact runtime.identity -> run context/step dispatch -> action fields and
idempotency expression -> findDecisionCard match evaluation -> registered anchor
and allowed-selector admission -> every validated mailbox.list page -> exact-one
count -> validated mailbox.get hash -> mailbox.decide/defer -> mutation result ->
quiescence/expectations -> joined session cleanup.

Fresh-session composition, run creation, expressions, anchor state/mutation,
stale-content enforcement, quiescence and cleanup are **different concepts with
proof** in existing public scenario/golden tests. Selector admission, filtering,
complete traversal and exact-one detail are **same C class**. Action prerequisites
must fail before lookup (missing run/verdict, fields type, invalid RFC3339 until,
expression error). No direct store/EventBus/private injection is a substitute.
No signed-webhook/live-provider/retained-restart claim is credited to public test.

## Canonical Owners And Exhaustive Known Consumer Census

All listed owners are real semantic owners, not first encountered local helpers.
Factoring local presentation/dispatch steps changes none of their contracts.

### A Owner / Consumer Table

| Owner | Consumers / disposition |
| --- | --- |
| agentidentity.Identity + agentpersistence/identity.go resolver, exposed by typed AgentReader/UsageReader/DeliveryLifecycleReader | Five agent detail/diagnostic/usage/lifecycle paths and conversation.list with agent filter **already consume** resolveOperatorAgentIdentityParam; keep exact run/flow target resolution, missing/ambiguous outcomes. No new resolver. |
| operatorread.AgentReader; operatorsurface.AgentSQLite/AgentPostgres | agent.list/get/diagnose/delivery_diagnostics **already consume** typed methods. Four inline factories become explicit local factories in this work; no store join moves. |
| operatorread.AgentUsageReader; operator_agent_usage_read_surface.go | agent.usage **already consumes** canonical spend_ledger window/breakdown owner; preserve separate capability when Agents is nil. No runtime token reconstruction. |
| operatorread.AgentDeliveryLifecycleReader; operator_agent_delivery_lifecycle_read_surface.go | agent.delivery_lifecycle **already consumes** canonical bounded delivery snapshots; preserve separate capability and event-status admission. No reconstruction from diagnostics. |
| operatorread.ConversationReader; operatorsurface/conversation_turn_projection.go and SQLite/Postgres conversation readers | conversation.list/list_turns/get_turn **already consume** bounded safe projections and cursor owner; list's optional agent resolution is retained. Dashboard server.go's list/detail also **already consume** this owner; different REST presentation, unchanged sibling proof. |
| store/selected accessors, runtimepersistence generated forwarders and conversation owner aliases | Production serveapp main.go mounts the typed capabilities once; API test assembly consumes the same factory. **Already consumes**; no port/facade migration or generated persistence inventory change. |
| apiv1.Handler, registry and existing per-method params/result validators | HTTP auth/transport/unknown-param admission and common result/error envelope **already consume** these owners; local factories move orchestration only. Nil capabilities retain METHOD_UNAVAILABLE and empty factory retains nil map. |
| cliapp shared v1 client, agents.go/conversations.go and identifier/output owners | agents list, agent view/diagnose/deliveries, conversations list/view/turn **already consume** these public methods. Prefix discovery is separate exact-ID admission with existing proof; no local store bypass. |

Nine methods exactly: agent.list, agent.get, agent.diagnose,
agent.delivery_diagnostics, agent.delivery_lifecycle, agent.usage,
conversation.list, conversation.list_turns, conversation.get_turn.
Full production definition/call scans include generated forwarders, store aliases,
selected composition, dashboard and CLI method strings, not just exported factory
calls. Agent control/frame/replay, event/run/entity/observability, conversation fork
and dashboard-local agent projections are **different concepts**: their owners
and method/schema/result sets are distinct; preserve sibling tests without
refactoring them. No live same-concept owner bypass found in the nine-method family.

### D Owner / Consumer Table

| Owner | Consumers / disposition |
| --- | --- |
| admitStructuralSource + semanticview + verifier report | full describe and describe routes **already consume** structural admission; verify is an **already consuming** sibling. No new source classifier or live deployment test. |
| authoringview.Build + lowered lifecycle/entity/instance/contained-operation owners | runDescribeCommandWithOutput **already consumes** View. Text details move into local render steps; JSON/quiet retain original output. #2505 changes effective mode/coordinator/input projection and fixture grammar, not our rendering authority. |
| routingtopology.Build via BuildRoutingTopologyWithReport | full View topology and describe routes **already consume** it; both **already consume** writeRoutingTopologyText. No graph reconstruction, event-name routing or ancestor lookup. |
| bootverify.FormatTypedDiagnosticFinding | describe text diagnostics **already consume** authored-location preference, fallback location, remediation/evidence and indentation; preserve exact presentation. |
| renderCLIOutput / describeQuietValues | stdout/stderr, JSON, quiet, no-color **already consume** these owners; renderer extraction must not affect machine omission/order or exit behavior. |
| userfacing human-code AST census | existing flow Mode exception **already consumes** the precise renderer site; retain it and all existing guard tests unchanged. |

Single production caller of writeDescribeText: runDescribeCommandWithOutput.
The `authoringview.Build` implementation/tests and routing/verify/CLI source/output registries were
checked; they stay projections/consumers, not replaced owners. No dead text path
or alternate live renderer for this same full describe transcript was found.

### C Owner / Consumer Table

| Owner | Consumers / disposition |
| --- | --- |
| scenarioExpressionEvaluator and scenario parser/action dispatch | findDecisionCard evaluates authored fields after runMailboxStep action prerequisites; **already consumes** expression owner. Evaluation/type/blank-field behavior stays unchanged. |
| decisioncard registered anchor union | stage_gate/human_task/proposed_effect selector admission **already consumes** registry. Extract existing restrictions, no new registry or inferred anchors. |
| public mailbox list/detail RPC + cliapp shared mailbox validation | scenario and ordinary mailbox list/view **already consume** validateMailboxListResult/validateMailboxDetailResult; existing registered card/proposed-effect evidence validation stays shared. No alternate codec. |
| scenarioRunner.findDecisionCard complete match-set policy | Only production caller runMailboxStep for decide/defer **already consumes** it. Admission, predicate and full traversal move into local helpers here; all allowed filters remain exact comparisons. |
| mailbox API/store cursor and card hash/anchor mutation owners | scenario **already consumes** opaque cursors unchanged and detail hash; mutation and pagination truth stay public owner-backed. Both-store 201-card API probe, not a fake page-only test, qualifies this seam. |
| TestSessionRunner / serveapp.RunTestSession + step/quiescence/cleanup owners | public swarm test executes selector through existing session/runtime/client **already consumed**; no new runner. Existing process-backed golden/harness paths are **different concept with proof**, regression-only. |

Old non-authoritative paths: **none newly made invalid**; no compatibility owner
or dead pager was found here. Original authoritative exported entries remain live
mounts; their mixed nested bodies are replaced, not kept as parallel selection/
interpretation paths. Do not duplicate old filters, cursor algorithms, source
interpretation or validators. Normal CLI mailbox mutation versus scenario match
selection is a different concept (explicit ID versus full-set authored predicate).

## Manifestations And Exact Planned Proof

All new names below are **planned**, not tests already run. First commit new
characterization against the merged pre-refactor production baseline, then use
the same assertions after extraction. Keep existing supported tests unchanged.
Each failure proof checks zero later reads/get/mutations, not only an error string.

### A: Operator Family (A01-A14)

| Row / manifestation | Exact planned proof |
| --- | --- |
| A01 capability mount Cartesian product | TestOperatorReadFactoringCapabilityMatrix: all 16 Agents/Conversations/Lifecycle/Usage presence combinations; exact mounted nine-method sets, nil all-absent; missing agent capability only needed for filtered conversation list. |
| A02 list/detail canonical success | TestOperatorReadFactoringWireCharacterization subtests agent.list/get: fixed correlation/time/identity, exact JSON fields/null/arrays/refs and no transcripts; exact option/read trace. |
| A03 exact identity admission | TestOperatorReadFactoringOrderCharacterization: absent/malformed run/agent/flow, missing and ambiguous targets, resolver failure; resolver once before detail/filter reads with exact original error details. |
| A04 diagnosis bounded queue and selected active evidence | Same wire/order tests diagnosis: queue totals not page size, retry count no +1, watchdog/active/tool fields and cross-turn failure; unchanged TestOperatorAgentDiagnoseFailsClosedOnMalformedOwnerData plus Active/Watchdog/LastToolOutcome controls. |
| A05 separate usage owner/window | Same tests usage: since then until, inclusive/exclusive window, wrong ordering, exact/estimated split, usage-only capability; unchanged malformed owner and window controls. |
| A06 rich delivery diagnostics | Same tests diagnostics: two independent limits/cursors, fallback cursorErr.Field, summary/detail/dead-letter evidence; multi-invalid param precedence and no later read. |
| A07 lifecycle page | Same tests lifecycle: each valid status, invalid/unknown status, limit/cursor errors, exact delivery status/failure/time fields and empty array; lifecycle-only capability. |
| A08 conversation list filtering | Same tests conversation.list: no filter versus exact agent/run/flow; flow requires agent; params before resolver, cursor/order/limit defaults unchanged. |
| A09 conversation turn page | Same tests list_turns: exact session, defaults/1/500 bounds, wrong cursor, absent session, compact safe projection and exact owner cursor bytes. |
| A10 exact safe turn detail | Same tests get_turn: session-before-turn; SESSION_NOT_FOUND versus TURN_NOT_FOUND; frame/privacy fields, no private/provider/log data. |
| A11 HTTP envelope/auth/schema gate | TestOperatorReadFactoringHTTPCharacterization: real registry/handler, auth missing/wrong/unconfigured, unsupported methods/capability, wrong/unknown params, fixed response envelope/errors and pre-read rejection. |
| A12 internal/corrupt owner failures | Hostile owner tests plus order trace: internal read/resolver errors preserved, existing validators' exact first failure unchanged, no opportunistic new validators on otherwise unvalidated list/get responses. |
| A13 real bounded store/cursor parity | TestOperatorReadFactoringSelectedStoreMatrix on SQLite/Postgres: all nine HTTP methods, multi-page queue/lifecycle/diagnostics/conversation/turn pages, captured cursor bytes forwarded unchanged and wrong-kind/foreign selector refused by existing owners. Add concurrent insertion control without offset pagination. |
| A14 compiled consumers | TestReadProofFactoringCompiledSurfaces: existing build/serve harness on each store, readiness then public HTTP and binary agent list/view/diagnose/deliveries + conversations list/view/turn in text/JSON/quiet where supported; original prefix/ref-only/safe output controls remain. |

### D: Describe Family (D01-D10)

| Row / manifestation | Exact planned proof |
| --- | --- |
| D01 empty/nil/root transcript | TestDescribeTextFactoringCharacterization: nil writer, empty View, root event fields/no fields, primary-entity presence; exact bytes. |
| D02 every flow-detail branch | Same test: flow label trimming/mode, activation, all optional ingress provider fields, primary entity, instance, coordinator and contained-operation count; exact order/punctuation. |
| D03 graph nodes/edges | Same test: root/path label, initial/terminal markers, description trimming, from <none>/multi-source, source/node/event/after/timer/loop/max/escape/decision/verdict combinations. |
| D04 graph timers/joins | Same test: all timer optional fields and join member/window source, fan-in pin/carrier fields; zero/nil sections omitted exactly. |
| D05 graph fanout/gates | Same test: from default, item alias/identity/max_items/source metadata, gate fields/outcome optional emit; exact lines and supplied slice order. |
| D06 diagnostic evidence | Same test: authored location then fallback, multiline remediation/evidence indentation, warnings/errors, no dedup or silent omission. |
| D07 topology sibling | Existing TestDescribeRoutesUsesVersionedTopologyAndMatchesFullDescribe and deterministic/typed-connect/root-input tests plus exact full transcript; preserve shared writeRoutingTopologyText. |
| D08 machine/quiet/no-color modes | TestDescribeFactoringOutputCharacterization: unchanged output View JSON including optional graph/diagnostics/approvals, quiet values and order, no-color text; unsupported flags remain refused. No YAML feature addition. |
| D09 source/admission failure precedence | Existing missing-contract/structural/verify tests plus compiled CLI: root/config and conflicting flags before source work, invalid admitted source still shows findings, exit/stdout/stderr unchanged, no live credentials/readiness dependency. |
| D10 merged grammar and compiled projection | TestReadProofFactoringCompiledSurfaces describe/describe routes/--graph/text/JSON/quiet with #2505's merged static/template/coordinator/edge shapes, captured pre-refactor output; existing human-code and source-selection guards unchanged. |

### C: Card Selector Family (C01-C12)

| Row / manifestation | Exact planned proof |
| --- | --- |
| C01 action preconditions | Existing missing-verdict/cross-anchor/action tests plus TestScenarioCardFactoringAdmissionCharacterization: missing run, bad fields/until/expression/idempotency stop before list; same error type/exit. |
| C02 all registered anchor selector sets | Same test: all eleven recognized match fields, per-anchor allowed/forbidden fields, missing/invalid anchor, blank evaluated value omission, expression failure/unsupported field; no RPC before admission. |
| C03 exact predicate comparisons | TestScenarioCardFactoringMatchCharacterization: notices/other anchors ignored; one-at-a-time mismatch and match for each allowed selector, no partial/fuzzy equality or default filter. |
| C04 unique later/first match | Existing TestScenarioMailboxConsumesCompleteMatchSet all 3 anchors x 2 actions; exact list params status=pending/run_id/limit200, only entity_id/anchor_kind sent server-side; full first page never short-circuits. |
| C05 zero/ambiguous full match set | Same unchanged matrix plus characterized count error bytes and no get/mutation; duplicate selected card occurrences still count as two, not deduped silently. |
| C06 repeated/cyclic continuation | Same unchanged matrix plus exact repeated-cursor error and forwarding bytes/retained params over 3+ pages; empty page with continuation does not terminate. |
| C07 malformed cursor/page or later error | New characteristic subtests: non-string cursor decode failure, existing-owner refusal of corrupted cursor bytes, missing/invalid items, malformed tagged anchor/effect, later RPC failure; no get/mutation after any invalid page. CLI does not parse opaque owner tokens. |
| C08 detail and stale-content fence | Same tests: exactly one get only after final valid page, detail validator malformed/hash/anchor refusal and RPC failure; decide forwards immutable hash, stale response error preserved; defer params unchanged. |
| C09 mutation response/errors | Existing decide/defer tests plus characterization of ok/card_id/change_id checks and no local mutation retry. |
| C10 real page-owner proof | Retain TestScenarioMailboxActualServerContinuationBothStores; TestScenarioCardFactoringActualContinuationBothStores adds proposed-effect/human-task cases using canonical writers and real mailbox handler, >200 rows, later ambiguity and foreign decision-card cursor codec refusal on both stores. Mailbox positions are creation-order cursors, not anchor-bound tokens; never invent a query-binding guarantee or change their schema. These are store-seeded API proof, not authored creation E2E. |
| C11 actual public mock command (approved correction) | TestReadProofFactoringCompiledScenario: compile swarm, run authored MockOnly stage-gate/human-task/proposed-effect scenarios through shared fresh session/RPC; assert text/exit and settled expected decision state. JSON/quiet are unsupported: prove their unchanged exit-2 refusal before source loading/session acquisition, not successful execution or a new output feature. No external API target, retained restart, signed ingress or live-provider credit. |
| C12 cancellation/failure before completion | Characterization blocks a later page/get, cancels context and proves error/no mutation; existing session cleanup/quiescence controls must pass. No retries, timeout changes or cancellation suppression. |

### Qualification / Sibling Proof (Q01-Q04)

| Row | Required proof after implementation |
| --- | --- |
| Q01 structural integrity | Existing API/OpenRPC/public capability matrix, source/output/identifier registry, userfacing guard and authoritative API-spec suites unchanged; no schema/wire/regenerated persistence authority drift. |
| Q02 store and sibling consumers | Existing selected-store conversation projection and agent-delivery pagination parity, dashboard conversation readers, authoringview/routingtopology and CLI agent/conversation/mailbox tests; focused race run for changed API/CLI controls. These do not grant unrelated owner closure. |
| Q03 unchanged process/golden | TestGoldenAgentWorkloadSQLiteSmoke; TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends; both BurstConcurrency iterations. Explicit backend leaves must execute, no skipped proof credit or workload/deadline relaxation. |
| Q04 final exact head | Default `go run ./cmd/swarm-test` (not --full unless requested), independent exact base/head complexity check, actual production diff inventory and unchanged-existing-test diff check, PR proof audit for A/D/C separately and required exact-head CI. |

Required supported-surface proof is A13/A14, D10, C10/C11 and Q03, not only fake
HTTP pages or direct helper calls. Test names/fixtures are commitments subject to
the gate, not claims that these new characterization or compiled tests exist.

## Baseline Verification Actually Run

- Small targeted unmodified API/CLI controls: PASS apiv1 10.327s, cliapp40.138s,
  log `/tmp/agent-g-2508-audit-targeted.log`.
- Verbose unmodified selected-store/public-selector controls: PASS apiv13.289s,
  cliapp6.447s; real PostgreSQL and SQLite leaves executed, no skips. Includes
  TestSQLiteAgentConversationOwnerBacksSupportedAPISurface,
  TestPostgresAgentConversationOwnerBacksSupportedAPISurface,
  TestSQLiteAgentUsageOwnerBacksSupportedAPISurface,
  TestSQLiteAgentDeliveryLifecycleOwnerBacksSupportedAPISurface,
  TestScenarioMailboxActualServerContinuationBothStores and all66 complete-match
  set cases. Log `/tmp/agent-g-2508-audit-public-owners.log`.
- Focused existing spec/userfacing guards: PASS apispec0.447s, userfacing10.891s,
  log `/tmp/agent-g-2508-audit-guards.log`.
- Exact canonical complexity baseline PASS; fresh metrics in
  `/tmp/agent-g-2508-complexity/{head.json,delta.json}`. Go1.25.5/linux/amd64;
  inherited host PostgreSQL test DSN used, no private backend skipped.

SHA256 receipts, in the same order: targeted log
`f43dd28d329e1becff430a928dabb816dc495347e06ed670cc208a8d5c4ed1d8`;
public-owner log `234e5fd274e433403f6ccff73d6dd2dd42f73812cc83e3fa7df7cb126f1193d1`;
guard log `7f9416b8c0542f8d10f37a78a6cd02ded2cbb68a1dc685ac9b71747b4f7c9579`;
head metrics `def9b6e94f435c6f31513784dfddb008dd635753ffbba289b3440bab1b3db148`;
delta `6d024d2fb3d5c461d5a98e241bf62bddaef5e30614fc0c05400624091de2cb58`.

No new characterization, mutation qualification, compiled journey, golden or full
swarm-test run is claimed in this audit. Baseline controls are not extraction or
closure proof. Existing historical failures remain recorded under #2353/#2394,
not solved by passing focused controls.

## Parent Action, Tracker, Watchlist And Gate Request

Parent-class sibling probe checked current keep-zone historical/successor targets,
all four assigned child scopes, open PR file overlaps, dashboard/CLI/operator
consumers, authoring/topology/source guards and mailbox public/ordinary readers.
The mapped `boundary_owned_decomposition` node already tracks large mixed seams,
score movement without ownership/proof, and active R5 work; `invariant_suite_coverage`
tracks proof fidelity and supported surfaces. They support the full three-family
scope, not absorbing providers/schema/aggregate validation or R4-R7 into this PR.

Post-pre-audit parent decision: **keep the explicitly lead-batched scope; leave
#2447/#2407 and semantic architecture #2250 open**. Broader live siblings are
already assigned #2509-#2511 and active architectural lanes. No same-concept A/D/C
interpreter is knowingly stranded. No new issue, broad framework, POTENTIAL_ISSUES
entry, or generic shared facade is warranted. Conditional parent closure remains
subject to the other child audits and exact-head census, not this initial queue.

Tracker-state decision: **update #2508 before coding** with this complete owner/
consumer/proof boundary and explicit #2505 dependency; refine the existing two
watchlist nodes. No new child or older-issue reopening. #2506/#2507 are merged
historical proof, not active dependency; #2008 WIP stays untouched. #2482 constrains
#2509 only. Other agents' worktrees remain unchanged. Other R1 rows retain their
original acceptance, including deferred #2499 and lead-owned policy.

Deeper smell: mixed presentation/selection/read orchestration makes invariants
hard to inspect; it is NOT evidence that the typed semantic owners are missing.
Long-run better direction here is explicit local steps consuming those existing
owners with pre/post characterization, not another interpreter/owner framework.
Tracking decision: **existing #2508/#2447 watchlist refinement**; no new architecture
issue. Rough effort 2-4 engineering days including exhaustive surface proof,
medium confidence; high maintenance ROI from replacing three large nested graphs
and permanently protecting their order/output/pagination, no throughput claim.

Feasibility: one PR is credible: nine bounded factories, independent rendering
sections and a small selector decomposition, without exported API/store/model
changes. Fixing one local helper alone would leave other branches of its chosen
class mixed; all are in scope. No broad duplication requiring a new runtime owner
was found. Do not preserve a second old algorithm as a compatibility seam.

Request an **independent pre-implementation gate**, explicitly ratifying the
replacement keep eligibility, distinct batched classes and A01-A14/D01-D10/
C01-C12/Q01-Q04 proof boundary. Existing-test edits remain prohibited. #2505's
merge and unchanged-design integration/remeasurement are prerequisites; if the
candidate disappears or semantic scope changes, repair/re-gate rather than factor
a guessed successor. A dependency merge does not itself grant the independent gate.

Stop conditions: new/spec-contradictory semantics, an unclassified same-concept
interpreter, a needed ownership/type-model migration, request for existing-test
weakening, generalized paging/render/validation infrastructure, vendors/dependency
source changes, or inability to close all three chosen maintenance classes.
**Gate outcome currently pending; no implementation permission is invented.**
