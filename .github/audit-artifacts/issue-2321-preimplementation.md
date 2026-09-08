# Pre-Implementation Coverage Audit: #2321

Agent: agent-g. Date: 2026-09-08. Audited code: `origin/master@09eaf68345cfc24770ce3b5109efd0d49cd15196`.

Phase: pre-implementation only. Independent semantic/broad-refactor gate requested; implementation is not approved by this artifact. The lead's product ruling is accepted, but the execution topology and verification questions below still require a recorded decision.

## Binding authority and spec reconciliation

Binding product ruling: https://github.com/division-sh/swarm/issues/2321#issuecomment-5577792999. It supersedes the original mock-default/graduation framing. `serve`, including `--dev`, is live; `test` is mock. Config cannot select posture. Agent `mock:` remains in source permanently and is consumed only in test execution. Retained serve defaults recovery to true; explicit false and dev scratch semantics remain. Existing store/workspace coordinates remain.

Read the full #2321 thread, #2319 body/thread and cross-pin, #2285 ownership ruling, and the implementer/semantic-drift guidelines. PR #2391 merged on 2026-09-07; its topology changes are present in the audited master.

Exact existing governing sections of `platform-spec.yaml`:

- `engine.process_execution_posture` (5898): currently requires authored posture and prohibits defaults. Its config/no-legacy text contradicts the new ruling and must be replaced, while causal-mode admission remains a real runtime invariant.
- `engine.agent_session_management.llm_provider_selection_config_authority.mock_agent_runtime` (6227), especially selection, lifecycle, fully-mocked startup waiver, authoring, and effect policy: currently mock presence selects mock even in a live process. Selection and waiver clauses must change together.
- `engine.boot_verification` (7816), `workspace_model.workspace_backend_selection` (19025), and source effect reachability: verification, credential, capability, and workspace consumers must follow the selected command execution.
- `configuration_source_authority.unified_swarm_config` source precedence/schema (18567 onward): retire the key in every layer, generated example/schema, diagnostic and config consumer. Keep other layer precedence under #1801.
- `engine.runtime_store_backend_selection` (5186) and `cli_specification.foundations.local_runtime_state_authority` (23109): preserve `.swarm/stores/dev.db`, separate `dev-scratch.db`, exact possession/epoch refusal, borrowed-source protection, and PostgreSQL selection.
- `test_specification.deterministic_scenario_runner` (20412), including `derived_scenario_compilation.execution_profile`, and `cli_specification.command_catalog.test` (23689): currently the CLI targets an existing server and explicitly does not activate mock execution. These are blocking contradictions with a command-owned mock runtime, not incidental documentation edits.
- Adjacent authority: API scenario selector admission, durable `run_scenario_execution_profiles`, source/effective-source identity, mock-effect denial, timer/readiness immutable execution mode, selected-contract and conversation fork ownership, provider-trigger credential admission.

No exact governing section currently defines a self-contained command-owned test runtime or bare verify's behavior under the new command split. The ruling, this concrete proposal, and adjacent contracts are the proposed binding context; independent ratification is required. Authoritative platform-spec.yaml is NOT changed during this audit. Approved decisions must be promoted in the matching implementation PR.

## Classification, concept, and closure

- Category: semantic migration, execution-authority canonicalization, CLI/runtime/store parity, first-user conformance.
- Observed symptom: both newly generated archetypes fail bare `serve --dev` at store admission because the shipped hidden config explicitly chooses a SQLite path.
- Chosen working failure class: `command_owned_execution_and_zero_project_edit_startup`.
- Immediate parent: `execution_selection_admission_dispatch_authority_drift`.
- Broader parent: `deployment_and_test_authority_leaks_into_portable_source`.
- Exact concepts: command-owned live/mock selection; inert authored test doubles in live execution; selection-dependent startup and descriptor materialization; isolated mock scenario execution; deployment-only config and truthful provenance; retained recovery default; shipped-artifact and first-run ownership.
- Original issue framing: too narrow and now contradictory. The repaired body must encompass selection, test runtime ownership and all consuming families. A config deletion alone is not an honest closure.
- The observed store refusal and local defaults helper were entry points, not the audit boundary. Runtime, persistence, roots/continuations, forks, validation, CLI/API, and conformance producers were swept independently.
- Intended closure: **failure class eliminated**, conditional on gate approval and complete execution proof. This PR aims to eliminate its chosen working failure class entirely. No runtime closure is claimed now.

## Proposed architecture and decisions requiring the gate

### G1: command and process ownership

Propose one typed execution purpose passed by command composition into existing runtime construction: live serve versus mock test. Keep the existing internal `executionposture.Posture`/`executionmode.Mode` distinction and effect fences, but remove posture as an authored configuration input. Internal typed construction is not another user selector.

`swarm test` currently creates an API client (`test_command.go:331`), resolves `runtime.identity`, and publishes into its selected server. `operator_scenario_execution.go:76` rejects derived execution unless that server is mock-only. `operator_event_publish.go:655` takes root mode from the server. A local CLI variable cannot change any of these authorities.

Propose a command-owned isolated test runtime using existing serve/runtime/store construction and the existing scenario runner/public API handlers. Fresh temporary SQLite state, private loopback endpoint and token, no public exposure/channel registration/provider ingress, no project context/retained-store claim or current-context rewrite. Test cancellation/failure must join runtime work and close the selected store before releasing temporary resources. Concurrent live serve and test must not share agent identities through a store, ports, context registration, or scratch epoch. Backend parity harnesses use isolated PostgreSQL databases via the existing test provisioner; no new public posture/backend-test flag is proposed.

Explicit decision requested: approve that isolation/lifecycle and retire connected/ambient target selection for `swarm test`, with a teaching refusal for explicit incompatible targeting. Do not silently turn a command addressed to an operator's live server into mutations against that server or ignore an explicit target. The existing scenario language, compiler, run profiles, public mutation/readback handlers and quiescence owner remain; no second runner or scenario framework.

Golden/lifecycle/catalog tests that currently launch `serve` with mock config are mandatory migration consumers. They must consume the test construction owner and retain real compiled command and restart claims. Do not relabel mock fixture execution as live, create a hidden `serve --mock`, or substitute a unit simulator. The gate must approve the shared test runtime lifecycle seam used by compiled/restart proof before implementation.

### G2: authored versus executable agent data

Propose that source retains the compiled test double as authored source data, while canonical selection produces the executable descriptor for the command. Live descriptors omit the inactive performance; test descriptors contain the exact compiled bytes/digest and mock mode. This preserves the current selected-store rule rejecting live descriptors that carry executable mock artifacts. Persistence consumes the resolved descriptor; it does not strip/reselect on read.

All static/template activation, reconfigure/replacement, recovery, restart, selected-contract execution and fork-chat must consume the same selector. Source mock presence may no longer waive live credentials, model, Claude CLI or native capability checks. Config or persisted `llm_backend: mock` must not become an alternative public command selector. Current historical causal mode must never be rewritten during recovery or fork; unsupported old selected stores remain unsupported, with no conversion or compatibility reader.

Proposed missing-performance scope: all effective agent declarations admitted into the test runtime, enumerated through scope-complete `semanticview.AgentDeclarations`, including templates/imports and duplicate local names. Missing performances fail before runtime/store/provider mutation with exact agent and `add mock:` guidance. Do not infer reachability from names or test coverage. Live execution retains source grammar validation of `mock:` but never executes it.

### G3: verify, backend, and environment prerequisites

Propose bare `verify` performs source/structural verification without choosing execution or requiring live credentials; live startup readiness remains in serve/doctor, and test performs mock-specific completeness/effect admission. Existing verify and live boot findings must be classified explicitly, not indiscriminately suppressed. This behavior is a proposed spec change requiring a ruling.

The current configured backend default is Claude CLI. Removing the authored mock waiver under live serve restores genuine Claude/Docker/model prerequisites. The ruling's wording that the only first-run obstacle is a missing secret is therefore not fully specified on a machine without those prerequisites. Proposed interpretation: preserve backend/capability requirements and honest installation guidance; do not change backend default or waive native/exec checks incidentally. Lead must confirm this interpretation or specify a different live default. No real external provider is invoked by this audit.

### G4: ingress dependency and deployment defaults

Credential-absence dormancy belongs to existing #2319, whose thread already carries the new ruling. It is a blocking acceptance dependency for the credential-absent webhook journey until implemented or explicitly absorbed by the gate. No fake raw subscriber, dummy signing key, broad credential waiver, or credential-as-posture selector.

Keep current config layering for unrelated facts (#1801). Reject retired posture at its actual file/key source even if another layer overrides it. Preserve valid deployment backend/secret references. Retire shipped `swarm.live.yaml` too. Recovery default true belongs to config resolution with presence-aware override semantics; an explicit false must survive merging. `--dev` remains fresh scratch.

## Full user-visible paths and gate classification

S = same chosen class; D = different semantic concept with named evidence; X = explicitly split/tracked.

1. `swarm new` -> exact embedded artifact inventory -> generated tree/next-command output [S]. Destination no-overwrite/file-copy semantics [D: `new_archetype.go`, retain existing scaffold refusal tests]. No generation of config or `.swarm/`.
2. Invocation root -> layered config/retired-key diagnostics/defaults [S for retired posture/defaults; X #1801 for unrelated discovery policy] -> exact source/resource loading [D: #2391 merged filesystem-tree loader and canonical-routing corpus; retain invalid source, symlink and missing-module proofs].
3. Bare verify -> source checks and classified deployment readiness [S, G3]. No config edit or hidden state creation should be required merely for structural verification.
4. Serve composition -> live selection [S] -> exact agent/model/capability/workspace projection [S selection consumers] -> backend prerequisites/secret resolution [S truthful reachability; D native/exec execution semantics, preserve negative proofs] -> ingress credential activation [X #2319] -> selected-store/epoch possession [D #2360; existing refusal test plus public first-run proof] -> recovery default and existing startup admission [S default; D topology and transaction algorithms] -> Manager/runtime readiness -> ingress/agent/activity side effects and public health [S actual selected mode].
5. Retained restart -> same admitted source/store -> selected descriptor reconstruction -> default-on recovery / explicit-false admission -> timers/deliveries/decision/activity continuation -> ready [S selector propagation; D immutable causal/lifecycle transaction owners]. Dev uses a fresh scratch epoch, not this replay path.
6. Test command -> exact source/scenario discovery and validation -> scope-complete mock admission -> isolated runtime/store/startup [S, G1] -> runtime identity/effective source/profile matching [S consumption; D exact digest algorithm] -> setup/new-run or derived publish -> root mock causality -> template/agent/node/connect dispatch -> timer/card/activity/connector-response settlement -> public diagnose/trace/entity assertions -> joined shutdown [S mode/ownership propagation; D scenario grammar and quiescence algorithms]. Missing mock or identity mismatch must fail before publishing or launching work.
7. Test while live serve exists -> distinct test ownership -> zero live-store/context/credential/transport changes -> independent shutdown [S]. Current CLI does not satisfy this path.

Earlier admission/config/source/model/store gates must succeed before any observable turn; tests cannot bypass them to claim the complete first-run journey.

## Canonical owners and exhaustive known consumer census

Status labels describe the proposed work, not a claim that code has already moved. A = already consumes the canonical owner; M = moved to the canonical owner in this work; D = different semantic concept, with proof; X = still bypasses and explicitly split/escalated.

| Owner | Consumer seams and status | Invalid/removal candidates and required execution proof |
|---|---|---|
| Command composition -> existing typed runtime posture | M: `cliapp.Execute`/`ServeRunner`, serve/LocalRun composition, `runtime.New` config reader, serve bundle validation/fork-chat construction, CLI verify/local preflight/pack validation. X G1/G3 for test composition and verification semantics. | Remove config-owned `Runtime.ExecutionPosture` YAML authority and `ProcessExecutionPosture` config inference; all 8 non-test call sites plus declaration found by `rg`. Prove serve/dev/local-run live, test mock, verify mutation-free, and no flag/env/backend alternative. |
| Existing unified config loader/default resolver | M: discovery, merged decoding, Validate, project/local/global/explicit schema, generated config example, CLI docs/doctor diagnostics, serve options and recovery default. D: unrelated source precedence (#1801). | Delete posture key producer/schema/tutorials, reject retired key with file provenance; prove every layer including overridden values and true/default/false recovery. |
| `llmselection.ResolveAgentExecutionSelection` | M: `llm/agent_selection.go`, `bootverify/checks.go`, `bootverify/source_effect_reachability.go`, `cliapp/workspace_backend.go`, `runtime_claude_startup.go`, `runforkexecution/runtime_container.go`. These are the six production call-site files found repository-wide. | Remove source mock presence as execution selector. Matrix: both command purposes, mock present/absent, supported providers, authored backend, scoped collisions; verify actual adapter reached. |
| `llm.ResolveAgentExecution` / `AgentRuntimeSet` | M: `manager/agent_manager.go:575` and `llm/runtime_resolver.go:146`; managed/static/template actors, lifecycle replacement/reconfigure, persisted adoption/restart, selected-contract `agent_runtime_materialization.go`, served and standalone fork container, fork-chat factory. | No adapter choice reconstructed from artifact presence or global-only runtime. Prove each construction family invokes live or Python adapter according to admitted purpose, preserving exact scope and generation. |
| Source effect reachability and typed workspace decision | M: declared/active backend credentials, Claude startup, managed workspace/probe checks, local preflight, workflow validation/final boot, model/native capability checks. Source-wide `bundleFullyMocked`, `declaredAgentMockCensus`, active mock consistency and teaching helpers must be removed/delegated where they independently select/waive. | Live source carrying doubles still requires genuine live prerequisites; test missing double fails. All-mocked/mixed/agent-free/native-bash/exec/non-Claude/imported activity/scoped-agent/shared-credential matrix. |
| Typed actor construction and selected-store descriptor projection | M: `actors/agent_config.go`, `agent_config_merge.go`, `agentpersistence/projection.go` encode/decode and manager writes; both backends share projection. A: store strict mode/backend/artifact consistency. | Keep source double separate from selected live descriptor. Prove persist/reopen/reconfigure/restart/selected fork on SQLite+PostgreSQL; corrupted descriptor fails without mutation; no reader-side re-selection. |
| Existing scenario runner, source projection and scenario profile owner | X G1: `test_command.go` external client/target construction. M after approval: authored and derived scenarios, setup/new-run/continue publication, `operator_scenario_execution.go` admission, runtime catalog creation, durable profile readers and selected forks. A: `scenarioexecution`, schema inhabitant, public mutation/readback and quiescence owners. | Retire execution against ambient live target; retain exact profile/source validation. Execute authored+derived scenarios in owned mock runtime; mismatch/absent mock/concurrent live target/cleanup failures have negative proofs. |
| Runtime causal-mode and external-effect owners | A: EventBus root/child admission, LLMAgent monotonic delivery, activity/completion controllers, connector response dispatch, session/tool/MCP/native/write gates. M: purpose input propagation/diagnostic text only where needed. | Do not delete `execution_mode`, health posture truth, persisted profile or mock-effect fences just because the config key retires. Prove zero provider/network/native side effects in tests and live transport selection in controlled serve proof. |
| Durable work/reintroduction owners | A: generic schedules/workflow timers, readiness plans, standing reconciliation, run/runtime resume, mailbox decide/defer/expiry, replay, selected/conversation forks; M only selection-dependent construction and command authority inputs. | Mode remains immutable across delay/recovery/fork; positive same-mode and negative cross-mode before mutation on both stores. Do not absorb timer/lifecycle algorithm redesign. |
| Existing store/default/epoch and workspace-root owners | A: backendselection, local runtime state, devscratch EpochAuthority, workspace root validation. M: remove authored redundant scaffold paths and consume default recovery. X G1: private test store lifecycle. | Keep durable and dev-scratch coordinates and workspace root. Prove fresh zero-edit dev, durable restart, explicit Postgres, explicit false, scratch refusal of authored paths, borrowed-project protection. |
| Provider-trigger credential snapshot/activation owners | X #2319: provider pack preflight, boot verification, inbound gateway, channel/registration readiness and notices. | Credential-absent ingress dormant while other work serves; present-invalid/read-error must not be silently classified absent. Separate credential-specific prerequisite, never selects execution. Required #2319 supported proof before #2321 closure. |
| Canonical artifact/scaffold and conformance inventory | M: `new_archetype.go` embed/copy/next commands, root embedded Telegram asset, archetype tests, example README, generated config docs, fixture/catalog/routing smoke gates, golden and full-lifecycle compiled profiles. | Delete five shipped configs (listed below), every authored posture corpus producer and hidden-config embed. Mutation-test newly discovered example/archetype, not hardcoded counts. Preserve exact source, turn, restart and matrix claims after test construction migration. |
| Public read/diagnostic projection | A: health.check/subscription/runtime identity read existing runtime facts. M: command announcements, missing-performance and retired-key provenance messages. | Config removal must not remove truthful live/mock observability. Readback compared with actual execution and no secret values. |

The existing owners are semantic owners, not the first helper encountered. The missing owner is the **lifetime and isolation of command-owned test execution**; G1 names and bounds it instead of claiming an API client is an execution owner.

## Census and observed proof

Baseline `git grep -l execution_posture`: 49 tracked files, including spec/tests/generated documents. This is a search denominator, not a deletion count: typed public runtime observations and negative retired-key tests remain legitimate. Refresh at implementation head. Also sweep CamelCase posture references, `MockConfigured`, `Mock.Configured`, `bundleFullyMocked`, execution selector callers, actor descriptor writers and test runtime producers.

Five shipped config files found with `git ls-files` (hidden files included):

1. `examples/integrations/telegram-agent/swarm.yaml`
2. `examples/integrations/telegram-agent/swarm.live.yaml`
3. `examples/integrations/telegram-agent/.swarm/swarm.yaml`
4. `internal/cliapp/archetypes/zero-agent-automation/swarm.yaml`
5. `internal/cliapp/archetypes/zero-agent-automation/.swarm/swarm.yaml`

`swarm.example.yaml` is generated deployment guidance, outside shipped example/archetype trees; remove its posture entry, not the entire deployment reference. `.github/fixtures/sqlite-local-smoke.swarm.yaml` is a proof producer to migrate, not a supported user-config exception.

Actual pre-audit execution:

- Built the real current-master binary with `go build -o /tmp/agent-g-2321-swarm ./cmd/swarm`.
- Ran `swarm new zero-agent-automation` and `swarm new webhook-responder` into separate fresh temporary roots. Both succeeded and taught explicit config commands.
- Ran bare `swarm serve --dev` in each untouched generated root. Both exited 3 with `cannot use authored or shared SQLite path` pointing to their generated `.swarm/swarm.db`; neither reached runtime readiness.
- The first attempt was correctly blocked by inherited test-only `SWARM_TEST_POSTGRES_DSN`; repeated production-shaped commands with only that test variable unset. No provider credentials were printed or live services invoked.
- Focused current-contract config/selection tests passed: `go test ./internal/runtime/llm/selection ./internal/config -run '^(TestResolveAgentExecutionSelection.*|TestProcessExecutionPostureIsMandatoryAndStrict)$' -count=1`. These confirm current contrary semantics, not target behavior.
- Focused current scaffold/default/epoch tests passed: `go test ./internal/cliapp -run '^(TestScaffoldAdmittedArchetypesAndTeachNextCommands|TestDevScratchEpochRejectsConfiguredSQLitePath|TestDefaultRuntimeConfig_DoesNotInferLLMBackendFromCredentials)$' -count=1`. They currently enforce the old scaffold/default behavior; a passing unit suite does not negate the two failing real first-run journeys.

## Manifestation coverage and exact planned proof

All proposed test names below are planned, not existing or passed. D = direct reproducer and fix; E = execution proof through the corrected path; X = split / escalate as a separate class. All D/E rows belong to the chosen class; X is explicitly separate.

| ID | Manifestation / owner | Plan | Exact proof required |
|---|---|---|---|
| M1 | Two fresh archetypes reject dev because of shipped config / scaffold+epoch | D | `TestZeroConfigScaffoldFirstRun` builds CLI, generates each tree, hashes project files, starts bare dev, observes readiness after prerequisites, proves no project edits and correct scratch coordinate. Baseline reproduced above. |
| M2 | Shipped live/mock/hidden configs and wrong Next commands / artifact inventory | D | `TestExampleArchetypeConfigBan` discovers full corpus; inject each config spelling and hidden directory into a new discovered artifact; gate names file and doctrine. |
| M3 | Config posture required / unified config+command | D | `TestCommandExecutionPurposeConfigRetirement`: omission accepted; live/mock/empty/unknown retired key rejected at all layers, including masked lower layer; exact file/key diagnostic. |
| M4 | Bare commands require config / CLI | D | `TestZeroConfigBareCommands`: real verify, retained serve, dev serve, test for both roots, no project config, no synthetic go.mod or absolute source operand. |
| M5 | Source mock chooses live-runtime adapter / selection | D | `TestCommandOwnedAgentSelection`: serve ignores executable mock, test invokes exact Python, missing mock teaches; supported provider table and authored backend conflict cases. |
| M6 | Scope/inherited/template agent bypass / declaration census | E | `TestCommandSelectionScopedAgents`: root/flow/imported declarations, identical local names, template instances; spy actual selected adapter and assert unmocked exact scope before mutation. |
| M7 | Live credentials waived by source doubles / reachability | D | `TestCommandSelectionCredentialAdmission`: absent/present credentials, mixed/all-double/agent-free, declarative and agent activity sites, shared-key requirement. Never waive live credential merely because mock exists. |
| M8 | Claude/native/workspace selection drifts / typed workspace+preflight | D | `TestCommandSelectionWorkspaceParity`: verify/doctor/serve/test for Claude/non-Claude, native bash/file/exec, no Docker executable, explicit unsafe host, managed startup probe; preserve independent capability refusals. G3 determines verify grading. |
| M9 | Live descriptor forbids retained source performance / actor projection | D | `TestCommandSelectedDescriptorParity`: source unchanged, live resolved descriptor no executable mock, test exact bytes; encode/reopen on both stores, corruption rejected. |
| M10 | Adoption/restart/reconfigure selects stale semantics / manager | E | `TestCommandSelectionLifecycleParity`: static+template spawn, replacement, persisted adoption, reconfigure, graceful/crash restart; check selected mode/backend/generation and actual turn on both stores. |
| M11 | Selected-contract and fork-chat reconstruct wrong adapter / fork owner | E | `TestCommandSelectionForkParity`: served and standalone selected execution and fork-chat, exact source/profile/artifact, same-mode preservation and incompatible-history refusal on both stores. |
| M12 | Test CLI mutates running live server / test composition | D | `TestTestCommandOwnsIsolatedRuntime`: run live server concurrently with sentinel public facts, invoke bare test, prove distinct store/endpoint/context and zero live mutations/transport. Explicit incompatible target rejects before any request. G1 required. |
| M13 | Derived profiles refused by always-live server / scenario admission | D | `TestOwnedTestRuntimeScenarioProfiles`: authored steps and derive/all-inputs with exact effective identity, mock root/agent/node/activity modes and public quiescence; mismatched/late profile fails unchanged. |
| M14 | Test startup/failure leaks work or state / process lifetime | E | `TestOwnedTestRuntimeCleanup`: normal exit, invalid scenario, startup failure, timeout, cancellation; no process/lease/listener/store possession left; retained and dev stores untouched. G1 required. |
| M15 | Test accidentally launches real side effects / effect owner | E | `TestCommandTestExternalEffectFence`: provider, HTTP/MCP/native/write/registration counters remain zero; exact connector response path succeeds; missing response refuses; delayed timer/card/activity continuation remains mock. |
| M16 | Recovery omission still false / default+existing startup | D | `TestRetainedServeDefaultRecoveryParity`: pending work retained then restart with omitted key; one convergence, loud notice, same source/IDs; explicit false retains applicable refusal; SQLite+Postgres. |
| M17 | Dev affects durable DB or path relocation / epoch | E | Existing `TestDevScratchEpochRejectsConfiguredSQLitePath` plus real two-start dev proof, durable-store sentinel unchanged, same established paths, no shared/borrowed/Postgres scratch. |
| M18 | Hidden error points to wrong file / config provenance | D | `TestRetiredPostureAndDevPathProvenance`: explicit file plus lower local offending key; message names actual originating file/key without secrets. Preserve layer policy under #1801. |
| M19 | Mock golden/lifecycle tests rely on retired serve mode / proof producers | D | Migrate existing `TestCompiledProcessFullLifecycleJourneysSQLitePostgres` and golden workload through approved test construction owner; preserve every current identity, ordered ingress, delivery, restart and cardinality assertion. G1 must settle restart test exposure. |
| M20 | Readback/boot output claims wrong mode / read projection | E | `TestCommandModePublicReadback`: compare startup notice, health and subscriptions with observed live/test adapter and root modes; no config-based narrative. |
| M21 | Bare verify live/test ambiguity / verification | X | G3 semantic decision required; proposed `TestBareVerifyIsStructural` plus serve live readiness and test mock completeness proves exact separation without weakening malformed-source checks. |
| M22 | Ingress absent credential blocks first run / activation | X | #2319: real webhook first-run dormant notice/no executable route, secret set then supported activation/restart, valid/invalid signature and missing/error/shared credential matrix. Both stores as owned there. Required before combined acceptance closes. |
| M23 | Other config discovery layering / discovery | X | #1801 remains owner; preserve explicit/local/project/global precedence tests for non-posture keys. No blanket config-loader redesign. |
| M24 | Native capability portability/backend choice / provider selection | X | G3 clarify prerequisite wording; #2284 owns web-search fulfillment portability. Keep genuine native/Claude/Docker admission tests; no incidental provider default change. |
| M25 | Delayed/replayed mock causal authority promoted / durable work | E | `TestCommandModeReintroductionParity`: schedule/fire/restore, workflow timer, standing/readiness, mailbox outcome, event/agent replay and run/runtime resume; compatible modes preserved, incompatible mode refused before state/claim/transport on SQLite+Postgres. |

M21 is an in-issue blocking spec question, not a deferred implementation slice. M22-M24 name distinct owners; no execution credit is granted until required supported proof exists.

## Parent probe, tracker, watchlist and feasibility

Parent sibling probe:

- Command/config selection, startup mock waiver and actor persistence: broken relative to new ruling; absorb all into #2321 now.
- Scenario runtime topology and verification: still unproven/underspecified; G1/G3 block coding, tracked here. A narrow defaults patch would leave the same class live.
- Credential-driven ingress activation: distinct lifecycle concept, #2319 open and cross-pinned. Record blocking acceptance dependency; ask gate whether separate delivery remains appropriate.
- Config discovery for other facts: distinct layering policy, #1801 open; current merged loader behavior is explicit evidence, not assumed obsolete.
- Deployment web-search fulfillment: distinct capability portability, #2284 open; no absorption.
- Durable data projection: #2295 owns retired data_source/volumes_from semantics; retain their rejection, emit neither. No new path/default owner.
- Store coordinate/epoch protection: different concept; baseline epoch refusal is correct. Preserve #2360 behavior.
- Source topology: PR #2391 merged; consume filesystem flow tree, do not reopen package grammar.

Parent action: absorb the complete command-selection consumer family into #2321; keep the broader deployment-authority parent open across #1801/#2319/#2284. This is not approval of a defaults-only first slice. Estimated remaining parent tail after chosen-class closure: three known groups (layer discovery, ingress activation, capability fulfillment), with moderate confidence; no exhaustive platform-wide closure claim. #2319 is directly acceptance-blocking, the others are not assumed runtime blockers.

Tracker-state decision: **current issue must be updated before coding**. Replace original mock-default, graduation, new coordinates, and narrow config-only title/body with command-owned serve/test contract and dependency/spec questions. Preserve historical comments. #2285 items 1/3 remain consumed; credential doctor item 2 stays separate. Existing closed #2172/#2214 history is not reopened: this is a new product-contract migration.

Watchlist decision: refine existing `runtime-operations.yaml#per_agent_llm_execution_selection_and_dispatch` and `maintenance-and-cleanup.yaml#invariant_suite_coverage`; add #2321 active mapping. Record that authored mock/config posture are superseded selectors under the new ruling; include scenario topology, persistence, conformance consumer and independent-gate pressure. The existing nodes explicitly name forks/recovery/waivers and prove the parent cannot honestly be reduced to config deletion. No new watchlist node or POTENTIAL_ISSUES entry.

Watchlist repair completed and pushed on `swarm-docs` branch `agent-g/2321-preaudit`, commit `a7edf75ee12306cc4735bd671d5877683491429c`. Both YAML files parsed successfully and `git diff --check` passed. The shared docs worktree's unrelated edits were left untouched.

Architecture feedback and tracking: existing code conflates authored test doubles, selected executable actor descriptors, configurable process ceilings and external scenario clients. Long-run direction is explicit command composition plus the existing canonical selector and runtime/effect/store owners. Promote now in #2321; no general runtime multiplexer, scheduler, ledger or new persistence framework. Rough effort: 5-8 engineering days including corpus migration and dual-store compiled lifecycle proof, moderate confidence and high ROI. This replaces an understated defaults-only estimate; G1 may change it materially.

Closure-feasibility check: potentially closeable in one coordinated PR once G1-G4 are settled and #2319 acceptance is available. It is not currently honest to assert unconditional one-PR feasibility. Fixing the local error leaves config, mock-presence, scenario-client and descriptor interpreters live. If implementation needs shared live/test runtime execution, public mode selectors, migration, a new scenario framework, or another source interpreter, stop and repair the gate.

## Gate request and implementation stop

Request an independent **Broad Refactor Escalation** decision for the bounded complete consumer migration, plus explicit G1/G3 and #2319 disposition. Acceptable broad-refactor response: `approved as broad refactor`, `denied; keep first-slice scope`, or `split: do first slice now, open canonicalization follow-up`. Approval must name the resulting class/closure level and cannot be inferred from the earlier product ruling.

Required before review after implementation: focused selector/config/lifecycle tests; compiled both-archetype zero-edit live serve and isolated mock test; genuine controlled live-provider dispatch evidence and missing-prerequisite teaching; SQLite/host-Postgres persistence/restart/fork/reintroduction matrix; unchanged semantic claims of golden/full-lifecycle profiles; full repository suite using `go run ./cmd/swarm-test`; deletion/corpus census and final Post-Implementation Proof Audit. Test adapters may observe routing but cannot stand in for the compiled supported-surface acceptance. Live provider proof must use configured prerequisites; absence is a reported proof blocker, never fake success.

Current independent gate outcome: **pending**. No implementation, runtime/spec promotion, new schema, migration, compatibility layer or PR closure has occurred. Stop after publishing this audit, tracker/watchlist repair and gate request.
