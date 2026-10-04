# Issue #2555: pre-analysis and contract reconciliation

Agent: agent-g. Phase: pre-analysis, not implementation or gate approval.
Historical questions below were answered by the user-ratified disposition
https://github.com/division-sh/swarm/issues/2555#issuecomment-5976230920.
The current coverage audit is `issue-2555-preimplementation.md`; this earlier
artifact is retained as history, not a live unresolved-question record.
Baseline: origin/master `76fbddd6dacec435e7807de14562be03b2a47e1d`.
Worktree: `worktrees/agent-g-2555`, branch `agent-g/2555-workspace-mcp`.
The unrelated #2008 worktree and F's #2319 worktree are untouched.

## Governing context

The four-part, one-PR disposition is binding:
https://github.com/division-sh/swarm/issues/2555#issuecomment-5976078034
The field reproduction is:
https://github.com/division-sh/swarm/issues/2549#issuecomment-5972937872
The banked #1779 doctor contract is:
https://github.com/division-sh/swarm/issues/1779#issuecomment-4948686334

IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md were re-read. Exact
platform-spec.yaml sections inspected:

- `engine.process_execution_posture`: serve/dev live, test mock_only.
- `engine.agent_session_management.llm_provider_selection_config_authority.mock_agent_runtime`:
  command selection, captured Python performance, pinned Wasmtime ABI,
  in-process provider contract, mock effect ceiling and consumer census.
- `cli_specification.foundations.local_cli_test_gateway_startup` and
  `local_tool_gateway_binding`: actual bound listener, runtime-owned token,
  workspace projection, normal and selected-fork consumers.
- `cli_specification.foundations.local_claude_cli_preflight_admission`:
  static doctor reporting and shared prerequisite owners.
- `cli_specification.command_catalog.doctor` and
  `serve.listener_topology_v2_1`: source-free static doctor, independently
  selected listeners, loopback defaults and API authentication restrictions.
- `test_specification.compiled_process_full_lifecycle_profile`: public T,
  retained internal H and separately provisioned live L credit; Docker-free
  mocks are currently an explicit contract.
- `managed_agent_capability_surface` startup/provider-turn/enforcement rules:
  exact actor, probe, execution, attempt and delivered binding evidence.

## Verified code findings

1. `llm.observeCLIResponse` records missing planned MCP provider bindings as
   `EvidenceUnavailable`. `ValidateCLIProviderCapabilitySurface` rejects
   mismatch and unequal native inventories, but never checks those unavailable
   MCP bindings. The canonical surface narrows them to noncallable; this is a
   consumer admission gap, not a missing capability owner.
2. `ValidateManagedProviderPreflight` then performs its additional HTTP list/call
   through `MCPGatewayHostEndpoint`, which cannot prove container reachability.
   Exact mock descriptors are skipped altogether. Normal startup, staged reset
   and selected-fork preparation need separate named execution proof.
3. `validateClaudeContinuationCapabilities` runs after the completion process.
   Changing that validator alone cannot satisfy "before the model runs". The
   workspace path must be checked before completion dispatch, independently of
   after-launch uncertainty handling. First, resumed, tool-result, directive,
   selected-fork and fork-chat turns must be covered without fabricated claims.
4. `DockerManager.EnsureContainerRunningWithIdentity` supplies a workspace
   network but no host-gateway mapping. `serveMCPContainerGatewayURL` advertises
   host.docker.internal for local listeners, while serve binds loopback.
   Mapping and reachable listener selection must agree; docker0 and the custom
   workspace bridge cannot be assumed to have the same gateway address.
5. Doctor checks credential presence, host listener availability, Docker/image
   availability, CLI version and retired gateway environment inputs. It has no
   live gateway or paid backend probe and currently overstates measured truth.
6. `MockRuntime` explicitly declares `ProviderTransportInProcess`, executes
   captured Python through `pythonmodule.Execute`, and returns calls to the
   ordinary `llm/conversation.go` local tool loop. Its factory receives neither workspace resolver
   nor gateway binding. Moving only a test client would retain the production
   mock bypass; moving all performances changes public test prerequisites.
7. The existing Ubuntu `sqlite-local-dev` job builds the workspace image, then
   runs an agent-free root-ingress source. That cannot qualify MCP transport.

## Owners to consume, not duplicate

- Callable plan/evidence: `core/managedcapabilities.Surface` and
  `llm/capability_surface.go`; availability rejection belongs to the existing
  CLI admission consumers.
- Binding and authentication: `runtime/toolgateway.Binding`, serve's bound
  listener projection, `llm.BuildMCPHTTPBinding`, MCP turn context registry and
  gateway. Preserve exact capability, execution and occurrence authority.
- Container lifecycle/target: `workspace.DockerManager`, `ExecutionTarget` and
  configured source projection. No second container manager or network registry.
- Mock selection/interpretation: `selection.ResolveAgentExecutionSelection`,
  `AgentRuntimeSet`, `MockRuntime`, captured mockperformance and pythonmodule.
  Preserve the pinned interpreter, source digest and resource limits rather
  than replacing them with unrestricted host/container Python.
- Invocation settlement: existing completion/effect, session and delivery
  owners. Pre-model transport refusal is not an uncertain provider outcome;
  actual post-launch failures retain their existing truthful settlement.
- Diagnostics: existing local-preflight finding/report owner and serve
  lifecycle presentation. Static checks cannot confer live-probe credit.
- Proof: existing compiled-process harness and Ubuntu workspace-image job.
  No new scheduler, proof framework, registry, compatibility path or vendoring.

## Three contract questions before the pre-audit gate

Q1. The acceptance names public `swarm serve --dev` with a mock agent. The
current selector and authoritative spec explicitly make serve/dev live,
retire public mock backend selectors, and make source doubles inert under
serve. Recommend preserving that rule: public `swarm test` proves fresh
MockOnly container/MCP transport; the existing compiled H entry proves retained
serve-composition restart. Do not add a hidden mock serve flag. Is that the
intended acceptance correction, or is a new public serve-mock product operation
intentionally authorized?

Q2. Does part 3 intentionally replace the current Docker-free mock contract for
every mock performance, including ordinary public test and retained H/golden
journeys? Literal complete replacement is coherent and removes the bypass, but
makes Docker/image availability a new prerequisite and changes the currently
forbidden container/MCP mock transport boundary. Recommend that explicit
replacement if "no in-process dispatch" is global; do not silently keep the
old path as fallback or claim a container-only smoke closes it. The gate must
bind the consumer/proof changes, not merely one Linux fixture.

Q3. Is #1779 riding as the free in-container gateway half only, with its paid
credential-validity requirement remaining open, or must this PR also deliver
the previously ruled opt-in `doctor --probe` real Claude round-trip and
journaled diagnostic run? Recommend an explicit distinction: the free
gateway check cannot claim real provider or credential validity. No paid call
or Telegram message is authorized or performed by this analysis.

## F coordination

Read-only comparison of F's `agent-f/issue-2319-dormant-ingress` at
`a6d38275a08fa6eb9ac776ddb89a64dbfabe2a29` identified direct or expected overlap:

- `internal/serveapp/main.go`: shared composition, registration/publication and
  boot order. F's current diff preserves the same composition owner; G must
  not move ingress ahead of its credential/currentness fences.
- `internal/runtime/runtime.go`: construction/options and startup admission.
- `internal/serveapp/main_runtime_test.go`: shared composition proof.
- `internal/serveapp/serve_lifecycle_presentation.go`: operator notices; gateway
  failure and credential dormancy must remain different findings.
- `platform-spec.yaml`: command purpose, startup/readiness and proof-credit
  boundaries. F's dormant ingress is not permission to run live Claude without
  its separately required credentials.
- `.github/test-proof-plan.yaml` and generated proof/inventory artifacts if
  new roots require their existing regeneration owners.

F's inspected diff does not change the CLI capability validator, mock runtime,
workspace container creation, doctor, or CI workflow. Coordinate shared hunks;
do not manufacture a merge dependency or modify F's worktree. The #2319
channel/credential class remains F-owned.

## Audit and gate status

Baseline controls (not repair or live-path proof): the focused CLI missing and
unexpected native-inventory test, workspace capability matrix, and both doctor
retired backend/gateway-input tests passed. The gateway package in that filtered
command had no matching roots; it is not credited as executed gateway proof.
Separate complete selection and toolgateway package tests then passed with
`go test ./internal/runtime/llm/selection ./internal/runtime/toolgateway
-count=1 -timeout=2m`. These prove the existing command/binding contracts only.

This document is not a complete Pre-Implementation Coverage Audit. The above
product-contract intersections need an explicit recorded disposition before
the exhaustive owner-consumption and manifestation proof matrix can be honest.
No coding gate, runtime fix, Docker/live probe, full suite or closure is claimed.
Refine the existing `transport_policy_and_surface_parity` watchlist node when
the scope is resolved; no new issue or speculative framework is needed.
