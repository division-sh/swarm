# Pre-Implementation Coverage Audit: #2555

Agent: agent-g. Phase: pre-audit only; independent coding gate requested.
Audited origin/master: `76fbddd6dacec435e7807de14562be03b2a47e1d`.
No production implementation, paid provider invocation, or Telegram call is
claimed. All new tests below are planned, not passing repair evidence.

## Binding governing context

The full issue body/thread, field reproduction, and both dispositions were
read. The latest user-ratified disposition resolves the three pre-analysis
questions and supersedes the original mock-under-serve acceptance:

- Four-part, one-PR ruling:
  https://github.com/division-sh/swarm/issues/2555#issuecomment-5976078034
- Command, workspace, doctor, and pre-dispatch corrections:
  https://github.com/division-sh/swarm/issues/2555#issuecomment-5976230920
- Linux field reproduction, not a new probe performed by G:
  https://github.com/division-sh/swarm/issues/2549#issuecomment-5972937872
- Banked paid-doctor contract, remaining separately open:
  https://github.com/division-sh/swarm/issues/1779#issuecomment-4948686334

IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md were re-read. Exact governing
`platform-spec.yaml` references at the audited base:

| Reference | Binding meaning and planned correction |
| --- | --- |
| `engine.process_execution_posture` | Public serve/dev remains live; public test remains MockOnly. No mock serve switch. |
| `engine.agent_session_management.llm_provider_selection_config_authority`, including `mock_agent_runtime` (7370) | Command-owned selection, captured performance bytes/digest, pinned CPython-WASI/Wasmtime and bounds remain authoritative. Retire mock `in_process` transport and local tool-dispatch claims. |
| `test_specification.compiled_process_full_lifecycle_profile` | T owns fresh public mock execution; H owns retained compiled lifecycle execution; L owns provisioned live-provider credit. Host mocks remain Docker-free; selected Docker workspaces require Docker. |
| `cli_specification.foundations.local_cli_test_gateway_startup` (23846) | Bind before constructing runtime; public test owns private resources and never borrows deployment connections, stores, credentials or listeners. Add an explicit workspace-only test selection, not ambient deployment-resource import. |
| `cli_specification.foundations.local_tool_gateway_binding` (23887) | Runtime-owned listener/token provenance, host/workspace endpoints, normal and selected-fork lifetime. No env-selected URL/token or fabricated grant. |
| `cli_specification.foundations.local_claude_cli_preflight_admission` (24004) | Static dependency checks stay distinct from a free target-local gateway observation and paid credential validation. |
| `cli_specification.command_catalog.doctor` (26215) | Default source-free/static diagnostics remain non-mutating with respect to retained state. An explicit free gateway probe owns and joins its temporary resources; it does not validate credentials. |
| `cli_specification.command_catalog.serve.listener_topology_v2_1` (26682) | API loopback and independent listener precedence remain. Correct the MCP default for Linux Docker and document authenticated non-loopback/hardened-host topology. Explicit listener choices are never silently rewritten. |
| `managed_agent_capability_surface` (39117), startup, provider-turn and enforcement children | Exact planned and delivered bindings must agree before execution. Missing planned MCP evidence is a refusal, not merely a narrower success. Preserve exact inventory/hash, attempt, actor, call-occurrence and narrowing rules. |
| `managed_external_effect_authority`, completion/launch/settlement and directive-origin contracts | No fake delivery claim. A pre-model refusal records no provider invocation/uncertainty; an actual dispatched failure retains truthful uncertainty and committed evidence. |

Spec changes must land in the implementation PR. Issue prose is not a merge
artifact. Paid `doctor --probe`, live-provider health, new native capabilities,
public mock serve, and post-boot bundle replacement are not authorized.

## Category, class chain and closure boundary

- Category: failure-class / semantic-drift / supported-platform parity.
- Observed symptom: a Linux Docker agent has no usable MCP tools, yet doctor,
  startup and a no-call turn can report success/delivered without an emit.
- Exact concepts changed: planned MCP availability as execution admission;
  execution-target-local transport observation; bound-listener/workspace
  projection; model-only substitution; truthful diagnostic proof credit.
- Chosen working failure class: **managed Swarm-MCP execution can proceed
  without proving the planned gateway/tool surface from its actual workspace,
  while mock execution bypasses that transport entirely**. Includes every
  mock construction/continuation, Claude startup/launch, and free diagnostic
  consumer of that same boundary, not only Linux name resolution.
- Immediate parent: execution-target/provider transport and surface parity.
- Broadest plausible parent: runtime execution-boundary policy drift, mapped
  by `transport_policy_and_surface_parity`.
- Issue framing: original platform symptom is too narrow; body repaired to
  the complete ruled class and corrected command acceptance before coding.
- The failing validator/helper is an entry point, not the audit boundary.
- Intended closure: failure class eliminated for this chosen class; broader
  transport parent remains open. This PR aims to eliminate the chosen class
  entirely, including the host mock bypass, not establish a second path.
- Feasibility: one PR is credible using existing selection, workspace,
  capability, MCP, lifecycle and settlement owners. Fixing only the endpoint
  leaves startup's host probe, mock local dispatch and later launches live;
  those paths are all included. No compatibility readers/fallbacks are kept.

## Concrete composition design to gate

1. Keep `Surface` and its planned bindings as semantic authority. Strengthen
   existing CLI admission to reject unavailable **planned** MCP bindings,
   without making denied/unplanned/empty native-only plans mandatory. Preserve
   independent mismatch, invalid inventory and exact native-set refusals.
2. Extend the existing target execution and MCP binding consumers with one
   bounded pre-dispatch transport operation. It executes in the selected host
   or Docker target and uses that target's URL, boot token, and exact turn or
   startup-probe context. It performs authenticated initialize/list, checks
   canonical definition identity and planned availability, and joins on exit.
   Safe startup calls retain the existing explicit validation-only contract;
   no business emit is permitted by a reachability probe.
3. Run this observation once per activation attempt, before executable agent
   lifecycle admission, and before **every actual launch**. Do not reuse an
   old readiness grant as current network evidence. A terminal pre-dispatch
   `workspace_gateway_unreachable` includes the redacted target URL, target
   kind and observed status/cause; no token, credential or context secret.
   Missing planned provider evidence from init also refuses startup. A
   disconnect after dispatch is not retroactively called pre-model refusal.
4. Linux Docker container creation adds
   `host.docker.internal:host-gateway`. For an otherwise-default Docker MCP
   listener on Linux, use a reachable authenticated bind (proposed minimal
   choice: wildcard MCP bind, with advertised host-gateway URL). Do not infer
   the alias from `mas_default`'s gateway or hard-code a subnet: Docker's
   host-gateway address can differ. Preserve explicit flags/config, host-only
   loopback, Docker Desktop selection and API loopback. Document exposure and
   token authentication; do not edit the firewall. An explicitly unreachable
   listener remains a typed failure. Hardened hosts may bind serve's explicit
   workspace-network address; the projected endpoint must match that bind.
5. Mock provider selection remains `mock`, but its one descriptor uses the
   existing subprocess/CLI transport kind, not `in_process`. `swarm` has a
   bounded internal mock-agent mode: captured Python source/digest and limits
   arrive through a private bounded input; the pinned interpreter evaluates
   them inside the selected target. Native host Python is never a fallback.
   The child makes real gateway HTTP calls with existing occurrence metadata.
   Parent settlement consumes results and existing continuation/output
   authorities; it never re-executes those calls in `Conversation`'s local
   executor. API-provider function-call execution is a different binding
   concept, not a mock exception. Internal mode is not a public backend switch.
6. Target-compatible worker provisioning belongs to workspace build/target
   admission, not a runtime compiler or another agent framework. On native
   Linux Docker, provision the current Linux swarm artifact read-only. An
   image/explicit artifact used from a different host OS must supply the
   compatible swarm mock mode and pass a bounded version/ABI/digest check;
   never mount a Darwin executable and claim Linux proof. Extend the existing
   workspace-build materialization with an explicit compatible-artifact input
   if needed. Missing/incompatible bytes fail before execution with remediation.
   The gate must ratify this provisioning port; no implicit downloads, new
   interpreter, vendoring, or broad release-system redesign is authorized.
7. Public test currently ignores deployment workspace configuration. Add only
   explicit `--workspace-backend host|docker` (and explicit image selection if
   required by that Docker selection), carried by `TestSessionRequest` to the
   existing private composition. Default host behavior and deployment-resource
   isolation stay intact. This small public port is submitted for gate
   ratification; importing all of `swarm.yaml` into private execution is not
   an alternative. Public test still uses its private SQLite store; PostgreSQL
   credit comes from the retained compiled H composition, not a fake public
   store selector.
8. Default doctor says gateway was **not checked from inside a container**.
   An explicit free gateway-probe mode uses existing target/network/binding
   owners with an owned temporary listener/container, performs no model call,
   touches no retained DB, and joins/disposes resources. It reports target
   reachability separately from CLI installation and credential presence.
   Successful infrastructure ping alone cannot certify an actor's planned
   tool inventory. Paid credential validity remains open in #1779.
9. Every composition constructing a mock runtime must provide its own exact
   binding. Public T is single-context. Normal H/recovery/reset and selected
   forks consume their existing runtime/gateway owners. If an internal
   multi-context mock composition is exercised, use separately owned private
   endpoints through the same existing gateway construction/lifetime, never
   the primary context's registry. Keep public multi-context live Claude
   explicitly unsupported; do not add a general multiplexing registry.

These are explicit proposed ports, not implemented facts. Reviewer-g must
confirm the workspace-only public selector, native-artifact provisioning and
reachable default within the one-PR gate. An incompatible product/spec ruling
or need for a general packaging/multi-context framework is a stop condition.

## Full execution path and gates

The failed turn is reachable only after all earlier stages succeed. Each row
is classified explicitly; no endpoint-only audit or "same seam" credit.

| Order | Stage/gate | Classification and proof |
| --- | --- | --- |
| 1 | CLI purpose, source/pack admission, exact scoped actor and captured mock module | Different semantic concept: selection/profile and source-admission tests preserve live/MockOnly, alias, missing module and byte/digest rules; M01/M03/Q04. |
| 2 | Structural verify versus deployment/native/credential readiness | Different semantic concept: verify/describe remain structural; existing #2321 separation tests and D01/D06/Q04 preserve no live execution during readers. |
| 3 | Workspace backend decision and immutable projection/target | Same chosen class for target selection/provisioning; N01-N07/M02/M11/Q01. Source-mount/path authority remains separately proved by workspace tests. |
| 4 | Actual listener bind, sealed binding and target URL/token | Same chosen class: N01-N07/A01/D02. API authentication is a different security concept, with N05/N06 and existing binding tests. |
| 5 | Static preflight and target-local startup/activation observation | Same chosen class: A01-A06/D01-D05/L01-L04. Credential validity is explicitly split, #1779 paid tail; ingress dormancy is separately F-owned #2319. |
| 6 | Attempt-owned registration, route/timer attachment, readiness and recovery release | Same chosen class for mandatory observation before executable admission; L01-L05. Attempt phases/compensation are E-owned #2496 and remain exact, not recreated. |
| 7 | Persisted delivery/directive or selected-fork origin, session and completion acquisition | Different semantic concept with proof: existing origin/launch/settlement matrices plus A07/M08/L06 ensure no forged delivery or foreign lifecycle authority. |
| 8 | Exact per-launch target-local probe before child/provider dispatch | Same chosen class: A02/A04/A06-A09/M02/M09/L03-L07. First/resume/tool-result/directive/fork and fork-chat are each executed, not assumed. |
| 9 | Native provider init inventory and MCP delivered definitions | Same chosen class for missing planned MCP; A01-A05. Native mismatch/invalid inventory are distinct existing security rules explicitly retained by A03/A05. |
| 10 | Tool call occurrence, MCP authentication/catalog lease and executor | Same chosen class for mock transport consumption; M04-M08. Existing capability/effect/catalog owners remain proof-bearing; replay/foreign/unplanned calls refuse. |
| 11 | Tool result, output/continuation commit, session/usage and delivery settlement | Different semantic concept: M06-M10/L06/Q01-Q03 preserve exact committed outcomes and no parent redispatch. Pre-model refusal versus post-launch uncertainty is part of chosen admission boundary, A07/M09. |
| 12 | Restart/reset/fork reattachment and next turn | Same chosen class for renewed target proof and binding lifetime: L03-L07/Q02/Q03; immutable run/source/fork authority is preserved independently. |

## Canonical owners and exhaustive systematic-consumption census

The owners below are actual semantic owners, not merely the first local files
encountered. Status meanings: **already consumes**, **moved in this work**,
**different concept with proof**, **split/tracked separately**. Census includes
production callers and existing tests that construct a runtime directly.

| Owner | Consumer/seam | Status and exact evidence/proof obligation |
| --- | --- | --- |
| `core/managedcapabilities.Surface` and `llm/capability_surface.go` | CLI native/MCP plan and evidence (`managedCapabilityPlanForActor`, `observeCLIResponse`, `ValidateCLIProviderCapabilitySurface`) | Moved in this work: missing planned MCP admission; A01-A05, preserve narrowing/hash checks. |
| Same | Mock `ObserveMockRuntimeCapabilitySurface`, local definition/provider contract | Moved in this work: delete local-runtime transport proof; M01/M04/M05 consume actual MCP evidence plus immutable interpreter input. |
| Same | Anthropic API, OpenAI-compatible, Responses definitions/calls | Different concept with proof: `BindingAPIDefinition`, provider contracts and API schema/committed-phase controls; Q05. No local execution removal for real API function-call adapters. |
| Same | Denied/empty/generated-current-entity-only startup surfaces | Already consumes plan; A03 confirms no fabricated missing-tool requirement when no concrete admitted MCP binding exists. |
| `toolgateway.Binding` + actual listener projection | `serveapp.createServeToolGatewayBinding`, private test, cold boot, retained reconstruction and dev reset | Moved in this work: target-correct projection and reachable conditional default, N01-N07/L03/Q01. Seal and per-boot token remain authoritative. |
| Same | `runforkexecution.startSelectedContractAgentRuntimeGateway` and prepared selected catalog | Already consumes dedicated fork owner; moved target observation/mock execution, L06. No borrowing normal gateway or predecessor plan. |
| Same | Fork-chat runtime construction and sandbox MCP transport | Moved in this work: target-local launch and mock HTTP execution; L07/M08. Preserve sandbox versus executable-fork distinctions. |
| Same | Secondary runtime contexts | Current live-Claude multi-context refusal remains; any mock runtime construction must get an exact owned private endpoint, not primary fallback; L05. Framework requirement stops coding. |
| Same | Retired `SWARM_TOOL_GATEWAY_*`, `SWARM_CLAUDE_USE_MCP` | Already fail closed for live Claude; M05/A04 prove no mock reintroduction or transport-disable/local fallback. |
| Existing workspace lifecycle/`ExecutionTarget` | `DockerManager.EnsureContainerRunningWithIdentity`, source/system/flow/agent scoped containers, reused identity inspection | Moved in this work: Linux add-host and worker admission at shared create/adopt boundary; N01/N02/N07/M11. Foreign/partial containers never silently adopted. |
| Same | Host manager, isolated host roots and workspace backend classifier | Already owns host target; moved mock child into it without Docker requirement, M02/N03/M12. Preserve native/exec safety reasons. |
| Same | `CheckWorkspaceCLICommandAvailable` and `workspace build` | Moved in this work for explicit target-native mock artifact proof; installation is not network evidence, M11/D03. Existing CLI version checks retained. |
| `selection.ResolveAgentExecutionSelection`, `AgentRuntimeSet`, `RuntimeFactory` | Normal actors, dynamically spawned/reconfigured actors, templates and persisted executable adoption | Moved in this work: one mock subprocess descriptor and mandatory workspace/binding dependencies, M01/L01-L04. No second selector or credential-as-mock heuristic. |
| Same | Selected-fork catalog/preparation/materialization and fork-chat | Moved in this work: consume frozen descriptor and exact target; L06/L07. No reselection of persisted work. |
| Same | Direct `NewMockRuntime` test constructors and injected execution fault seams | Moved in this work: tests inject the same bounded process/transport dependencies or test only pre-launch lifecycle operations. No production default local executor retained; M13/Q05 deletion census. |
| `MockRuntime`, captured `mockperformance`, `pythonmodule` | First completion, tool-result round, frame validation, fuel/memory/output limits | Moved in this work: evaluate immutable captured bytes inside target-native swarm child; M02/M03/M06/M10/M11. Pinned interpreter remains the same owner. |
| Same + existing completion/conversation owner | `executeMockCompletionWithExecutor`, `Conversation.executeToolResponse` and output/continuation consumption | Moved in this work: remove mock direct dispatch while preserving consumed continuation, logical call identity and terminal output; M04/M06/M07/M09. No parent double execution. |
| `llm.BuildMCPHTTPBinding`, MCP context registry/gateway | Target endpoint selection, HTTP initialize/list/call, per-boot/turn tokens and inherited catalog lease | Already owns auth/effects/occurrences; moved all mock producers and target-local probes to it; A02/M04-M09. No second token registry, occurrence ledger or HTTP allowlist. |
| Same | External MCP HTTP/stdio provider client | Different concept with proof: `mcp.Client` strict untrusted provider boundary and external credential tests, Q05; not the internal Swarm tool gateway, no new external-effect permission for mocks. |
| Existing lifecycle replacement and E's attempt-owned attachment | Fresh spawn, persisted executable adoption, readiness/reconcile, runless preparation and restart | Moved in this work: composed admission before runnable replacement and phase release, L01-L04. Lifecycle-only teardown remains non-executable, L04. Probe success cannot mark ready itself. |
| Same | Complete-source generation restamp and failed/abandoned attempt replacement | Already E-owned semantic transitions; L02 checks no second attempt on valid restamp, failure is joined through existing compensation before fresh attempt. |
| Existing CLI child launch/completion owner | Startup process, first/resumed/tool-result managed launch, directive, selected-fork and fork-chat | Moved in this work: per-launch target observation before actual dispatch; A06-A09/L06/L07. Existing streaming/buffered starts both included. |
| Existing effect/session/delivery settlement | No-dispatch refusal, actual dispatched failure, committed tool response and recovery | Already canonical; A07/M06/M07/M09/Q02 prove no uncertainty for pre-model refusal and no retry/replay of sent effects. |
| Local-preflight/report and serve presenter | Default/source-free doctor, JSON/text/target/schema modes, free gateway probe, boot diagnostics | Moved in this work: honest separate static/target/provider credit; D01-D06/N06. No DB or paid model access for static/free doctor. |
| Compiled T/H/L harness and Ubuntu image smoke | Public private host/Docker test, retained H both stores, dev-live refusal, Linux image conformance | Moved in this work: Q01-Q06. Preserve credential-free host golden/lifecycle proof and no mock-under-serve credit. |
| F's credential/onboarding/currentness owners | Serve ingress dormancy, provider activation and recovery publication | Different concept with proof: #2319 crossed startup controls Q04/L03; shared-file integration, not permission to bypass credentials. |

### Old paths now invalid or removal candidates

- Mock `TransportMock = in_process`, `ProviderTransportInProcess` mock contract
  and persisted descriptor expectations; remove dead enum branches only after
  full usage census, not through a dual reader. Old selected stores unsupported.
- `BindingLocalRuntime` / `mock_interpreter_input_delivered` as *tool transport*
  evidence and mock performance returning calls to direct local tool dispatch.
  Keep immutable interpreter digest evidence, but it cannot prove MCP delivery.
- Mock factory construction without workspace, runtime-owned gateway and child
  execution dependencies; test-only injected parent interpreters cannot remain
  a production escape hatch.
- Startup's host-side HTTP list/call as proof of Docker reachability; a host
  control can remain diagnostic but never confer target-local admission credit.
- Permissive planned-MCP-unavailable validation and reuse of a prior activation
  observation to authorize a new launch.
- Linux implicit hostname projection without host mapping/reachable listener;
  endpoint rewriting, ambient gateway tokens and private-context fallback.
- Doctor's static gateway-green implication and fixture comments asserting
  mocks always avoid MCP. Docker-free still applies to host targets.
- Agent-free Linux smoke as agent gateway conformance; retain its own credit,
  add the discriminating target-local agent transport journey.

## Manifestation matrix and exact planned proof

Prefixes locate proposed new tests: A = `internal/runtime/llm` plus startup;
N = workspace/serve listener; M = mock/transport/selection; D = cliapp/doctor;
L = manager/serve/runforkexecution lifecycle; Q = compiled supported surfaces.
Parameterized rows explicitly name every branch required, not aggregate
"same corrected path" assertions. Store-bearing proofs use SQLite **and**
PostgreSQL; skips cannot earn either store's credit.

| ID | Known manifestation | Named planned proof and decisive oracle |
| --- | --- | --- |
| A01 | Planned MCP provider/tool evidence unavailable is tolerated | `TestPlannedMCPAvailabilityAdmissionMatrix`: failed/missing server, absent one/all planned names -> typed refusal; expected/denied/empty controls; immutable planned bindings unchanged. |
| A02 | Host succeeds while execution target cannot connect | `TestGatewayObservationUsesExecutionTarget`: real child/HTTP endpoint, container-only refusal with host success control; zero model launches, exact redacted URL and target kind. |
| A03 | Denied/empty/static-generated surfaces could become false refusals | `TestGatewayAvailabilityRequiresOnlyConcreteAdmittedBindings`: native-only, zero tools, fully denied, generated-current-only versus one concrete MCP tool. |
| A04 | Auth/DNS/connect/status/malformed list and disable env drift | `TestGatewayPreDispatchRefusalMatrix`: bad boot/turn token, unresolved name, refused socket, timeout, 5xx, malformed inventory/hash and transport-disabled config; no local fallback/provider launch. |
| A05 | Stricter MCP check could mask native/unknown inventory errors | Extend exact native and malformed/missing/explicit-empty inventory tests; preserve original error precedence and mismatch refusal with reachable gateway. |
| A06 | Only first turn receives the check | `TestClaudeLaunchGatewayCheckMatrix`: first, resumed session, tool-result successor, streaming and buffered launch, each with unreachable-after-prior-success; launch sentinel stays zero. |
| A07 | Pre-launch rejection becomes outcome_uncertain/retry | `TestGatewayRefusalSettlementBothStores`: delivery and directive origins, failed pre-model operation, zero invocation/turn/spend/output, no delivered outcome; actual-started failure uncertainty control. |
| A08 | Selected execution accidentally asks for ordinary delivery authority | `TestSelectedGatewayPreDispatchAuthority`: prepared runless startup and executable selected fork carry exact existing variants; no source-run/normal claim substitute. |
| A09 | Fork-chat launch skips target check | `TestForkChatGatewayLaunchRefusal`: sandbox context, real target child sentinel, typed pre-model refusal before session/response acknowledgement; managed-turn token rejected. |
| N01 | Native Linux Docker has no host-gateway alias | `TestLinuxWorkspaceGatewayHostMapping`: shared create/adopt arguments plus real Ubuntu container lookup/connect; verify exact mapping, not only mock argv. |
| N02 | Correct hostname still reaches loopback-only socket | `TestLinuxDockerDefaultGatewayProjection`: default Docker MCP bind -> successful authenticated container initialize/list; explicit loopback negative, no inferred subnet. |
| N03 | Docker fix changes host-only/macOS behavior | `TestGatewayListenerTargetDefaultMatrix`: host loopback, Linux Docker reachable default, Desktop projection, explicit IPv4/IPv6/wildcard; independent API address unchanged. |
| N04 | Explicit address or Docker alias override is hidden | `TestGatewayExplicitListenerAuthority`: flag > config > conditional default; no silent rewrite; observed projected URL follows actual listener and exact target. |
| N05 | New non-loopback MCP listener leaks unauthenticated execution | `TestReachableGatewayAuthentication`: missing/wrong boot and context tokens refuse /mcp and /tools, correct token/exact actor succeeds; API remains numeric loopback with its existing auth rules. |
| N06 | Hardened host transport is falsely reported healthy | `TestHardenedWorkspaceGatewayTopology`: explicitly reachable serve-network endpoint positive and network-disabled/unreachable control; doctor/boot typed status and documented topology agree. |
| N07 | Reused/partial/foreign container misses new immutable target inputs | Extend workspace source/projection lifecycle tests: owned reuse confirms required mapping/artifact, incompatible/foreign identity refuses, failed create/removal retains joined cleanup and no runnable child. |
| M01 | Mock descriptor still selects in-process transport | `TestMockWorkspaceTransportSelection`: all-mocked, mixed, normal/dynamic/persisted/selected/fork-chat construction resolves one mock provider/subprocess descriptor; live/source-double inert controls. |
| M02 | Public host mock now unnecessarily needs Docker/Claude/credentials | `TestHostMockAgentUsesRealGateway`: compiled host child with absent Docker/Claude/Python/credential commands; real HTTP tool call and settled emit, no parent local dispatch. |
| M03 | Container execution rereads mutable module or uses host Python | `TestMockChildPinnedInputBoundary`: captured module bytes/digest, changed source path, wrong digest, invalid ABI, imports, fuel/memory/output limits, cancellation; exact pinned interpreter only. |
| M04 | Docker mock performs tool call in parent | `TestDockerMockAgentGatewayEmission`: container process evidence + real HTTP initialize/list/call + store emit identity; parent executor trap fails if invoked outside gateway. |
| M05 | Mock capability plan/list definition uses a local waiver | `TestMockMCPDefinitionAndAvailabilityParity`: exact schema/description hashes, planned names, unavailable/foreign/unplanned denial, disabled env cannot select local runtime. |
| M06 | Child tool call is executed again by Conversation | `TestMockGatewayToolRoundCommitBothStores`: terminal emit and nonterminal result/next frame; exactly one call/effect/output, exact consumed continuation and usage, no parent redispatch. |
| M07 | Retry/restart repeats a committed child effect | `TestMockGatewayOccurrenceRecoveryBothStores`: lose response after committed tools/call, reopen/restore and continue via existing receipt/occurrence; one durable effect, no minted second call authority. |
| M08 | Mock token crosses sibling run/fork/sandbox | `TestMockGatewayAuthorityIsolationBothStores`: same slug foreign run, selected fork/source and fork-chat sandbox cross-use refused before executor; exact legitimate origins succeed. |
| M09 | Target cancellation/leak or missing gateway triggers fallback | `TestMockChildFailureAndJoin`: prelaunch unavailable typed/no model, cancellation during interpreter/HTTP joins child and request, post-effect failure preserves committed evidence; no detached cleanup or synthetic success. |
| M10 | Mock child framing permits unbounded/ambiguous input/results | `TestMockAgentProtocolBounds`: missing/unknown/trailing/duplicate fields, oversized stdout/stderr/input, structured error, malformed results refuse under existing bounds; no secrets logged. |
| M11 | Wrong-OS/stale worker binary is counted as transport proof | `TestWorkspaceMockWorkerArtifactAdmission`: current Linux executable positive; Darwin/missing/wrong ABI/digest/version negative; warm artifact remains exact; no runtime download/build/fallback. |
| M12 | Agent-free/native/exec consumers lose prerequisite safety | Workspace classifier/preflight matrices: agent-free no child, host mock, mixed live Claude, native bash/file IO, exec tools retain their separate permissions/dependencies; no blanket mock waiver. |
| M13 | Test-only direct MockRuntime constructors hide production bypass | `TestMockTransportOwnerCensus` plus existing constructor consumers: no production local-dispatch/parent-interpreter path; legitimate pre-provider rotation/release tests remain bounded owner tests. |
| D01 | Static doctor claims container path was checked | `TestDoctorGatewayProofCredit`: text/JSON/source-free say not checked from inside a container; credentials present != validated; no Docker launch/model/retained DB for static report. |
| D02 | Free doctor probe executes on host instead of target | `TestDoctorFreeGatewayProbeTarget`: real Docker probe proves positive/host-only negative, correct explicit network/mapping and actual endpoint, bounded typed status. |
| D03 | Installed CLI/image is confused with reachable gateway | `TestDoctorGatewayPrerequisiteMatrix`: image/CLI/version/static listener, failed transport, credential presence remain distinct category/findings; original checks retained. |
| D04 | Doctor probe owns temporary resources badly | `TestDoctorGatewayProbeCleanup`: partial listener/container failure, cancellation, concurrent probe, cleanup errors and request join; zero retained state or paid calls. |
| D05 | Doctor target/schema modes acquire execution resources | Extend target/schema-inventory tests: no probe unless explicit free mode, incompatible mode combinations fail before acquisition; paid probe remains unimplemented/open. |
| D06 | Readiness wording causes verify/describe/live-mock confusion | Compiled verify/describe/doctor/test/serve command matrix: structural truth separate, serve source doubles inert, no paid/live claim from public mock success. |
| L01 | Fresh/dynamic admission becomes runnable before check | `TestGatewayProbeFencesActivationAttemptBothStores`: fresh spawn, publish/materialize, persisted executable adopt, reconcile; pause probe, assert no runnable dispatch/phase-ready, fail compensates exact attempt. |
| L02 | Same attempt gets second authority or fresh attempt skips check | E-crossed attachment test: once per attempt, same-plan generation restamp retains attempt, failed joined replacement/new attempt rechecks; stale probe result cannot advance replacement. |
| L03 | Cold/recovered/dev-reset work runs on stale/unpublished gateway | Retained H both-store boot/recovery modes and dev-reset test: binding publication before observation/execution, retained pending work blocked until exact target check, no old token after reconstruction. |
| L04 | Teardown-only adoption or partial failure launches work | Lifecycle-only adoption and activation compensation controls: non-executable adoption skips model/work, failed target owns/joins resources, no guessed running state or second readiness owner. |
| L05 | Secondary mock context borrows primary gateway | Exact two-context internal-composition probe: each mock uses its own endpoint/registry and cannot access other source; public multi-context live Claude still refuses as specified. |
| L06 | Prepared/executable selected-fork gateway path is missed | Both-store supported selected fork execution and recovery: exact catalog/binding/probe/attempt, reachable positive, gateway-loss pre-model refusal, terminal cleanup/restart cannot repeat committed emit. |
| L07 | Fork-chat uses another factory/continuation path | Both-store public conversation fork/chat: host/Docker mock path, normal exact response and later continuation, unavailable target before launch, sandbox restrictions and acknowledgement semantics retained. |
| Q01 | CI image smoke has no agent transport proof | New Ubuntu image job journey: compiled public `swarm test --workspace-backend docker` authored mock emits via actual gateway, public scenario/store assertion; after initial success deliberate unreachable per-turn refusal proves no delivered/no emit and zero model invocation. |
| Q02 | Restart proof is accidentally a fresh private test | Compiled retained H on SQLite/PostgreSQL, graceful and forced interruption: stable causal/session identities, unsettled checkpoint, exact post-restart HTTP call/output cardinality; not credited as public serve or live provider. |
| Q03 | Boot refusal is tested through unsupported mock serve | Compiled **live** `serve --dev` with deliberately unreachable gateway: typed boot refusal before Claude/provider sentinel, no mock under serve; free doctor agrees. This is network/boot proof, not successful paid provider proof. |
| Q04 | Private Docker test imports deployment DB/listener/credentials | Extend `TestPrivateTestIgnoresDeploymentResources`: explicit workspace choice only, invalid selector/image refusal before acquisition, deployment traps/sentinels untouched, default host preserved. Cross F's dormant ingress and E's activation ordering. |
| Q05 | Parent/API/external-MCP/native siblings regress | Existing provider-contract, origin, completion, external MCP parser/auth, toolgateway seal, session/rotation, workspace projection and catalog lease controls; Linux/Darwin target correctness and deletion census. |
| Q06 | Partial focused proof is counted as final qualification | Same-head focused/race + both-store supported matrix, authoritative spec/API checks, generated proof inventory/complexity checks and full `swarm-test` on server2; hosted Ubuntu transport job and exact-head full CI. Reviewer sets CI/Local tier; proposed full/full. |

## Parent probes, watchlist promotion and tracker decision

The mapped node already records transport token parity, provider-visible versus
callable truth, startup versus provider-turn authority, selected-fork gateways,
mock false-green proof, exact definition hashes and per-occurrence evidence.
It directly demands promotion beyond a hostname/validator fix: mock execution,
host-only startup observations and every later launch are absorbed now.

Parent sibling probing inspected actual code, not labels: API adapters deliver
`BindingAPIDefinition`; external MCP uses a separate strict untrusted provider
client; native tools use their provider inventory; paid doctor uses real
credential/provider execution. These are different concepts. Their baseline
controls are named above; no new defect or closure is inferred for them.

- Parent action: absorb every known same-class workspace/MCP bypass in #2555;
  keep the broader transport/security/credential parent explicitly open.
- Tracker-state decision: update #2555 body before coding to corrected
  acceptance/class/ports and this complete matrix; preserve the full historical
  field report. #1779 stays open for paid backend probing, with its free
  gateway half linked here. F's #2319 and E's #2496 stay separate and unchanged.
- Watchlist decision: refine existing `transport_policy_and_surface_parity`
  with #2555/#1779, target-local evidence, mock transport, all launch consumers,
  diagnostics and proof separation. No new issue/node/POTENTIAL_ISSUES entry.
  Repair published on swarm-docs master at
  `b5b89ab78185cee125b5a8fbe3c403b9430ea34b`; YAML and issue mappings validated
  after integrating concurrent docs updates. This is pre-audit tracking,
  not runtime closure or independent gate approval.
- Estimated child tail to close this chosen class after this PR: **zero**, if
  the matrix is proved. Confidence medium before Docker/process implementation.
  Broader parent tail includes paid #1779, external-provider/tool-policy and
  provider-specific lifecycle seams already mapped; not a finite promised
  estimate. Paid doctor is roughly one bounded diagnostic child, medium
  confidence; other parent groups need their own current census.
- Architecture smell: model substitution was coupled to execution transport,
  and canonical unavailable evidence was interpreted as success by admission.
  Better direction: preserve existing canonical owners, substitute only the
  model primitive, and demand exact target observations at their admission
  boundaries. Tracking: **#2555 absorbed repair + existing watchlist**, not a
  second framework issue. Broader phase ownership remains E/F's tracked work.
  Rough effort: one multi-day implementation/proof pass, high regression ROI
  because ordinary mocks finally test the transport that live Claude needs.

## Coordination and implementation ordering

F #2319 shared files: `internal/serveapp/main.go`, `internal/runtime/runtime.go`,
`internal/serveapp/main_runtime_test.go`,
`internal/serveapp/serve_lifecycle_presentation.go`, `platform-spec.yaml`, and
proof-plan/generated inventory. Its separate credential/currentness and ingress
publication boundaries are preserved. Coordination was posted on #2319; its
worktree was inspected read-only and no merge dependency is manufactured.

E #2496 read-only census at `c7b1df7e8` includes
`manager/flow_runtime_readiness.go`, `flow_attachment_resources.go`,
`agent_manager.go`, `types.go`, `runtime.go`, `runtime_claude_startup.go`,
`platform-spec.yaml` and inventories. It uses the same attachment receipt across
planned -> agents_registered -> route_installed -> timers_armed -> ready.
G's transport observation must complete before executable lifecycle admission
and readiness release, through an existing composed admission port; it cannot
mint a new readiness/attempt authority. Revalidate before every actual launch
even after a ready or restamped attempt. Proposal and shared-file census are
posted to #2496 for coordination before implementation; response is not
represented as agreement until recorded.
Coordination threads: F
https://github.com/division-sh/swarm/issues/2319#issuecomment-5976188635;
E https://github.com/division-sh/swarm/issues/2496#issuecomment-5976348525.

Implementation order after independent approval: spec/typed admission and
target projection; target-native internal modes and sealed binding; global mock
consumer migration/deletion; doctor/private test ports; crossed lifecycle and
supported Ubuntu/dual-store proof; final full qualification and proof audit.
Whichever overlapping PR lands second integrates actual owners, regenerates
generated artifacts, and runs crossed controls. No other agent's WIP is edited.

## Baseline receipts and gate status

Passed at the audited unchanged production base, not repair proof:

1. Focused native inventory, workspace selection and retired doctor-input
   controls; complete `llm/selection` and `toolgateway` packages (recorded in
   the linked pre-analysis). The earlier filtered gateway package had no
   matching tests and is explicitly not counted.
2. `go test ./internal/runtime/mcp ./internal/runtime/workspace
   ./internal/runtime/llm -run
   'TestTurnContextRegistryAdmitsOnlyExactRunlessStartupPlanProjection|TestTurnContextRegistryRejectsSameSlugSiblingCapabilityPrincipal|TestMCPToolCallLogicalIdentityRequiresProviderCallCoordinateForManagedTurns|TestExecutionTargetDistinguishesDockerHostAndEmptyContainer|TestHostManagerResolveWorkspaceCreatesScopedHostTargets|TestObserveMockRuntimeCapabilitySurfaceBindsExactInterpreterInput'
   -count=1 -timeout=3m`: all three packages passed.
3. `go test ./internal/runtime -run
   'TestValidateManagedProviderPreflightConsumesRunlessBlueprintBeforeLiveAdmission|TestValidateClaudeMCPToolsForManagedAgents_AcceptsFullyDeniedToolPlan|TestValidateClaudeMCPToolsForManagedAgents_RequiresCLIStartupProbeForMCPOnlySurface|TestValidateClaudeManagedAgentWorkspacesUsesNonExecutingCapabilityAdmission'
   -count=1 -timeout=3m`: passed. Runless/denied/probe-required/nonexecuting
   workspace controls are preserved baseline, not target-local repair proof.

These are baseline ownership controls only. No new Docker, free-doctor,
compiled mock-worker, dual-store target-local, live-provider or full-suite
closure credit exists yet. No successful paid turn is required or authorized
by the stated gate; genuine live calls would need separate authorization.

Blocking conditions: missing independent gate; unratified proposed public or
artifact port; target-local observation cannot retain exact existing authority;
new same-concept interpreter; a general multi-context/packaging/readiness
framework required; contradictory spec or unknown supported fallback;
pre-dispatch check cannot run before model without business effects;
mock tool/output identity cannot use existing effect/continuation owners.
Escalate any such condition, do not narrow claims or preserve a bypass.

Independent gate outcome: **requested, not yet recorded**. The user-ratified
architecture answers are not substituted for reviewer-g's coverage approval.
Runtime implementation remains frozen until the issue thread records it.
