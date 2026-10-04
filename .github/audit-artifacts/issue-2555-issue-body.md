## Ruled class and pre-audit amendment (agent-g)

The original field report below is preserved. These corrected acceptance and
class statements supersede its mock-under-serve wording, per the user-ratified
lead comments:

- https://github.com/division-sh/swarm/issues/2555#issuecomment-5976078034
- https://github.com/division-sh/swarm/issues/2555#issuecomment-5976230920

Chosen class: managed Swarm-MCP execution can proceed without proving the
planned gateway/tool surface from its actual workspace, while mock execution
bypasses that transport entirely. One PR absorbs startup/activation, every
launch, Linux networking, global model-only mock substitution, free doctor
gateway probing and discriminating Ubuntu/retained both-store proof.

Corrected acceptance:

1. Positive and per-turn refusal through public `swarm test` with an explicitly
   selected Docker workspace; the mock executes inside that container and
   makes real authenticated gateway calls. Host-workspace mock tests remain
   Docker-free and also cross the real host gateway. No local-tool fallback.
2. Boot-level negative through public live `serve --dev`, gateway deliberately
   unreachable: doctor/boot report `workspace_gateway_unreachable`, no model
   invocation. Source doubles remain inert under serve; no public mock serve.
3. Target-local observation once per activation attempt and before every
   first/resumed/tool-result/directive/selected-fork/fork-chat launch. Preserve
   exact E-owned attempt/lifecycle authority; no second readiness owner.
4. Linux host-gateway mapping and reachable authenticated MCP default preserve
   explicit listener precedence, API loopback and hardened-host topology.
5. Mock uses the target-native swarm mock-agent mode with captured bytes,
   digest, pinned interpreter and bounds. Parent never re-executes its calls.
6. Free doctor gateway half rides; paid `doctor --probe` consent/journal half
   remains open on #1779. Static/installation/credential presence is not live
   proof, and no paid-provider success is claimed.

Audit artifact: `.github/audit-artifacts/issue-2555-preimplementation.md` on
`agent-g/2555-workspace-mcp`, audited master
`76fbddd6dacec435e7807de14562be03b2a47e1d`. It records the class chain, full
flow, canonical owners, exhaustive consumption/removal census, 48 manifestation
rows, authoritative spec delta, parent/watchlist decision and stop conditions.
The proposed workspace-only public test selector, compatible worker provisioning
port and reachable default are explicit gate decisions, not silently implemented
features. New frameworks, compatibility paths and vendoring are not authorized.

Tracker decision: body updated before coding; no new issue required.
Existing `transport_policy_and_surface_parity` node is refined. #1779 paid
tail, F #2319 and E #2496 remain separate; no inferred merge dependency.
Coding gate: **approved** by reviewer-g in
https://github.com/division-sh/swarm/issues/2555#issuecomment-5976477595.
The 48 rows are planned proof, not runtime closure. Implementation exposed two
bounded stop conditions: the existing persisted `start_failed` compensation
operation is outside both stores' operation union, and Docker fork-chat
capability admission can replace the source actor's execution container. Those
bounded dispositions are recorded at
https://github.com/division-sh/swarm/issues/2555#issuecomment-5978140439.
Fork-chat isolation is approved inside #2555, through the existing workspace
owner and exact selected-store fork authority, shared by mock and Claude.
E's #2525 alone owns the compensation fix; no duplicate/cherry-pick here.
L01/L04 and final closure wait for its actual merge and G's both-store
target-local refusal/retry proof after integration. Independent work continues.
Both remain audited obligations, not a schema/framework or identity fallback.
Independent M09 proof then exposed a third bounded stop condition: loss of the
attached local Docker client lets `RunWorker` return while its exact native
container worker and held HTTP request are still live. Real-Docker probes fail
3/3 while request-deadline controls pass 3/3. The same existing owner serves
mock model, gateway probe and tool-call consumers; no second interpreter was
found. Reviewer-g approved the bounded existing-worker-owner repair in
https://github.com/division-sh/swarm/issues/2555#issuecomment-5978927926.
The uncommitted repair has real-Docker request/process join, sibling/source
isolation and ambiguous-call controls; those are partial WIP proof, not closure.
The already-audited Conversation consumer also must stop an uncertain transported
tool outcome rather than feed it to a new model round. Its real HTTP lost-response
counterexample failed three times; the bounded correction preserves ordinary
observed tool-error feedback. No source-container deletion, deadline-based
completion inference, provider replay, ledger or new lifecycle framework is
authorized. Independent proof continues; #2525 is not the only remaining
obligation.
The current M09 controls pass all seven real-Docker leaves with race detection
three times, including post-call deadline uncertainty. Typed possible-commit
evidence survives cancellation and malformed replies; the Conversation stops
without a successor call. The ordinary client-loss tests retain strict
request/process absence before return. An intentionally failed cleanup observer
instead proves uncertainty and both errors, with eventual disposal owned by
the proof, not credited as a successful runtime join. Both-store lost-reply
replay refusal survives selected-store reopen. Retained forced restart and
exact two-context native gateway preparation also pass named partial controls.
These are uncommitted WIP results, not final-head or 48-row closure proof.

## Historical field report

The following report is preserved as evidence. Its original acceptance wording
is superseded by the ruled class/acceptance above.

## Summary

On a Linux host with native Docker, `claude_cli` agents in workspace containers cannot reach the Swarm MCP tool gateway with the default configuration. They run **without any Swarm tools** (no `read_flow_data`, no emit tools, no entity reads), and nothing reports it: `swarm doctor` passes, boot says the agent runs in a container, and most turns settle as successful with nothing emitted. The same contract worked on macOS for months.

Found on `master@a2d84f0` (2026-10-03) while running the jobflow v5 weekly flow on a Debian 13 server. Evidence and discussion: https://github.com/division-sh/swarm/issues/2549#issuecomment-5972937872 (section "Environment finding that hides all of the above on Linux").

## Why it works on macOS and fails on Linux

When the MCP listener binds to loopback (the default `127.0.0.1:8082`), serve tells containers to use `http://host.docker.internal:<port>`. The code is `serveMCPContainerGatewayURL` in `internal/serveapp/main.go`, around line 3190 at `a2d84f0`:

```go
containerHost := host
if isLocalListenerHost(host) {
    containerHost = "host.docker.internal"
}
```

- **macOS (Docker Desktop, colima/lima):** the VM's network layer resolves `host.docker.internal` and forwards connections to the Mac's loopback. A loopback-bound listener is reachable, so this path has always worked.
- **Linux (native Docker):** containers share the host kernel and get no such forwarding. Two independent failures:
  1. **The name does not resolve.** Workspace containers are created with `ExtraHosts=[]`, and the runtime source has no `host-gateway` / `ExtraHosts` / `--add-host` anywhere. Verified in the workspace image on Docker 26.1.5:
     ```
     $ docker run --rm --network mas_default swarm-workspace:<tag> getent hosts host.docker.internal
     (no output; /etc/hosts has no entry)
     $ docker run --rm --network mas_default --add-host host.docker.internal:host-gateway swarm-workspace:<tag> getent hosts host.docker.internal
     172.17.0.1      host.docker.internal
     ```
  2. **Even with the mapping, the listener is unreachable.** `host-gateway` resolves to the `docker0` gateway, but serve is bound to `127.0.0.1`, and a loopback socket is not reachable from a bridge network. The workspace network is `mas_default` (hard-coded default in `internal/runtime/workspace/manager.go`), whose gateway is a different address again.

Our server also has a host firewall that drops all container → host traffic. That made the failure total, but it is not the root cause: points 1 and 2 hold on any default Linux Docker host.

## What the user sees

Timeout probe: 6 investigations, real `claude-sonnet-5`, `max_concurrency: 4`.

- The agent's own summary: *"neither of these functions is present in my actual available tool set for this session (I only have `ExitPlanMode`, `RemoteTrigger`, `WebFetch`, and `WebSearch`)"*. After the agent's `native_tools` were restored, it still had no MCP tools.
- **4 of 6 turns** finished with `parse_ok=1`, delivery `delivered`, and **no emit**. Their instances stayed in `investigating` until the stage timeout.
- **2 of 6 turns** were dead-lettered as `claude_cli_attempt_outcome_unconfirmed` (operation `validate_tool_calls`).
- Inside a workspace container: `curl http://172.18.0.1:9252/` → connection failed.

Every surface said things were fine:

- `swarm doctor`: `claude_cli preflight: ok`, plus INFO lines saying gateway endpoints are derived from `ToolGatewayBinding`.
- Boot banner: `workspace  docker · agent "investigator" runs in a container`, `ready`.
- Serve feed: ordinary `✓ sent` lines; failures surfaced only later as `internal error`.

Diagnosing this took about an hour of reading store tables, container env and Go source. A new Linux user would most likely conclude that agents don't work.

## Workaround we used

Run `swarm serve` itself in a container on the workspace network, so agent containers reach it container to container:

- MCP listener bound to the serve container's own IP (`hostname -i`). `--ip` is rejected on `mas_default` because it has no user-configured subnet.
- API on the serve container's loopback (a non-loopback API correctly requires `--api-token-file`).
- CLI through `docker exec`.
- Docker socket and Docker CLI mounted; project and state directories mounted at identical paths.

With that, all 18 sessions of the real week-2 run worked: tools available, 0 dead letters. It is a workaround, not a supported topology.

## Asks, in priority order

1. **Fail loudly when a turn's MCP tool surface is empty or the gateway is unreachable.** This is the most valuable fix: a turn that cannot see its declared emit tools should fail with a typed, named error such as `workspace_gateway_unreachable`, not settle as delivered. This alone would have turned an hour of archaeology into one line.
2. **Probe gateway reachability from inside a workspace container** in `swarm doctor` and at serve boot, and refuse or warn clearly when it fails. This is the live-probe idea in #1779 applied to the gateway, not only to `claude`.
3. **Make the default work on Linux:**
   - add `--add-host host.docker.internal:host-gateway`, the `ExtraHosts` equivalent, to workspace containers on Linux;
   - bind the gateway where the workspace network can reach it, for example the workspace network's gateway address, or a Unix socket mounted into the container (#1576 studied IPC transport);
   - keep the API on loopback either way.
4. **Document the supported topology for Linux and for hardened hosts** whose firewall drops container → host traffic, including running serve on the workspace network.

## Acceptance

- On a stock Linux Docker host, `swarm serve --dev` plus one `claude_cli` agent turn sees its MCP tools with no extra flags. There should be a conformance journey on a Linux runner that asserts a real or scripted agent calls one MCP tool through the workspace path.
- With the gateway deliberately unreachable, doctor and boot report it, and an agent turn fails with a typed error naming the gateway, not `parse_ok` with no emit and not `outcome_uncertain`.

## Related

- #1568 (closed): introduced `ToolGatewayBinding`; the endpoint derivation lives there.
- #1576 (closed): IPC transport study.
- #1779 (open): doctor live backend probe.
- #2549: the field measurements this blocked on Linux.

## tracker
```yaml
blockers: [2525]
score: 80
```
