# Provider backing progress and new live stop condition

Agent G, 2026-09-09. This is not a final Post-Implementation Proof Audit.
The approved provider-state patch is implemented; full class closure is withheld.

Status supersession: the freeze requested below was superseded by the independent
activation-lifetime gate in `issue-2321-activation-lifetime-amendment.md`.
`issue-2321-activation-progress.md` records the ensuing implementation and its
evidence. The historical live failure below is not reclassified as a pass.

## Implemented under the existing gate

`workspace.ClaudeStateRequest` carries reusable session, exact stateless delivery,
invocation, probe or fork storage identity. DockerManager binds a private state
volume/tmpfs to a disposable execution container sharing only the existing
logical work/source/data mounts. Normal and fork turns use the checked binding;
startup uses isolated probe binding. `buildCommand` rejects missing bindings and
sets CLAUDE_CONFIG_DIR explicitly. The confirmed head is validated before launch;
the exact candidate transcript is checked before atomic head settlement.

The selected-store lease is authoritative even when its provider head is empty.
Rotation clears the old process head. Root invocation/probe cleanup releases the
container without erasing completed session audit readback. Release errors are
returned; failed binding cleanup is joined, not suppressed. Persistent provider
volumes are retained as inert artifacts after their existing session/delivery
authority terminates. No generic GC, transcript migration, new registry, provider
head fallback, or sent-delivery replay was added.

## Executed proof

| Manifestation | Execution / result |
| --- | --- |
| Full source, actor/run, session and kind isolation | TestClaudeStateNamespace PASS; changed full-hash suffix, run, session and state kind produce different keys |
| Stateless reclaimed delivery versus another delivery | TestClaudeStateNamespace PASS; new claim version preserves key, another delivery changes it, foreign run refused; this is coordinate proof, not complete crash/successor execution credit |
| Real provider transcript survives disposable projection | TestClaudeStateDockerRetentionAndRefusal PASS, network none, synthetic JSONL, real image 2.1.87 recognizes exact head before/after source release by reaching auth refusal, not missing-conversation refusal |
| Stopped and force-removed container | Same Docker test PASS; existing head checked after reattachment, no file reconstruction |
| Missing/corrupt backing | Same Docker test PASS; corrupt JSONL refused, missing confirmed volume refused without recreating it |
| Invocation/fork temporary state | Same Docker test PASS; within-invocation head reuse works, release destroys tmpfs, subsequent old-head selection fails, no named provider volume created |
| Shared flow workspace versus private memory | TestClaudeStateDockerSharedWorkspaceIsolation PASS (2.871s); both actors read the same work fixture but cannot see each other's private file, despite deliberately equal session UUID |
| Candidate backing failure before head promotion | TestClaudeMissingCandidateBackingNeverPromotesHead PASS, SQLite/Postgres (1.121s); one provider dispatch, terminal uncertainty, zero retries, no promoted head |
| Session cleanup result/error ownership | TestClaudeInvocationReleasePreservesSessionReadbackAndError PASS; audit session retained, failed cleanup propagated |
| Existing adapter matrix | SQLite/Postgres attempt identity, prelaunch retry, postlaunch/commit/settlement failure tests PASS (6.088s); fake backing is protocol proof only |
| Focused/package/spec | Focused LLM PASS (0.536s); LLM/workspace packages via swarm-test PASS (2.145s/0.110s); APIspec PASS (2.439s); diff check PASS |
| Genuine live first and graceful retained restart | TestCommandLiveServeAndRestartParity: ingresses 1001 and 1002 reply, accepted-turn and delivery convergence PASS on SQLite and PostgreSQL |
| Genuine forced retained restart | Same journey FAIL on both stores at ingress 1003, no reply decision card; see new stop below |
| Fresh dev epochs and final retained-history check | NOT REACHED in either live backend because forced-restart step failed |
| Full suite | First invocation invalidated by a temporary probe-file removal/build race and interrupted. Stable-tree rerun through swarm-test is in progress; no full-suite pass claimed |

Latest expanded offline Docker run: 24.739s. Complete live command: 404.634s,
SQLite 197.94s and PostgreSQL 201.00s. Logs remain local and redacted:
`/tmp/agent-g-2321-private-state-docker.log`,
`/tmp/agent-g-2321-private-state-live.log`,
`/tmp/agent-g-2321-private-state-full-stable.log`.

Remaining provider proof includes a real reclaimed stateless tool-successor
execution, complete probe/selected-fork lifecycle controls, unusable/foreign
backing negatives and every required dev/reset isolation row. A shared backing
owner does not close these rows by itself.

## New stop: nested channel presentation during publication

Both real stores now pass the original graceful-restart failure. At the later
forced-restart checkpoint the recovered third delivery runs a real Claude turn,
but no reply approval card is produced. The turn lasts 68,563ms (SQLite) and
68,991ms (PostgreSQL), then reports the reply tool unavailable. Both deliveries
are marked delivered with zero active leases/deliveries on public diagnose.
The sanitized serve log reports `mcp.tools.list.context_error: context canceled`.
Do not replay either settled delivery to repair the missing reply.

The source-supported suspected wait cycle is:

1. `agents.(*LLMAgent).applyTurnToolDefinitions` acquires an activation
   presentation lease for the whole provider invocation.
2. Serve's post-`startServeRuntimeContexts` publication enters
   `publishChannelActivations` -> `ReplaceChannelActivationsContext` ->
   `channelactivation.Owner.ReplaceContext`, fencing new leases and awaiting the
   old provider's lease.
3. The provider's separate HTTP MCP tools/list request calls
   `Gateway.mcpToolsForRequest` -> `acquireToolDefinitionsInContext` ->
   `tools.Executor.AcquireToolDefinitionsForActorInContext`, requesting a new
   presentation instead of consuming the already-pinned one.
4. New presentation waits for replacement; replacement waits for provider;
   provider waits for tools/list until the client times out.

The enclosed offline probe `issue-2321-channel-activation-probe.go.txt` uses the
unchanged real activation owner and an exact fence barrier. Desired-progress
assertion fails 3/3 (0.158s aggregate), with no network/provider calls. Releasing
the outer presentation lets replacement finish. This proves the mechanical
wait cycle; its attribution to the live failure is a source-supported inference,
not a captured live goroutine stack. The temporary executable probe was removed;
no channelactivation/MCP/serve publication production code was changed.

This is outside the approved provider-private file lifetime and uncertainty
handoff repair. Request independent bounded attribution/repair disposition before
changing those owners. The narrow design question is how exact pinned
presentation authority crosses the MCP request boundary, and whether bootstrap
publication must precede recovered provider work. Do not bypass fences, skip
tool checks, add waits/retries, silently assign it to F's #2319, or introduce a
general lifecycle framework.

Watchlist refinement: swarm-docs 2147d1d, existing transport parity and workspace
lifecycle nodes. #2432 still awaits its independent fork-provenance amendment
gate; #2319 remains F-owned/nonblocking under the unchanged split. No new issue
or POTENTIAL_ISSUES entry created. #2321 is not review-ready and no closure claim
is made.
