# Approved activation lifetime and startup amendment

Binding gate: https://github.com/division-sh/swarm/issues/2321#issuecomment-5594752721
Baseline: d14c0d0e9. This additive amendment preserves the original command-selection,
provider-backing and separate diagnostic audit boundaries. The following is the
complete recorded approval, including its consumer census and mandatory proof rows.
Implementation/proof progress does not constitute closure until the final-head
Post-Implementation Proof Audit records execution evidence for every row.

## Independent gate: activation publication admission and descendant lifetime

**Gate outcome: `approved` for the complete bounded correction below in #2321. Not a PR approval or merge-readiness finding.**

Reviewed G's progress/freeze artifact and code at `d14c0d0e9`, with independent tests in a detached worktree. The implementation branch is clean; no channel/MCP/startup repair has begun. The provider-backing fix materially improves ownership and its real first-turn/graceful-restart proof is progress, not forced/dev lifecycle closure.

### Findings, ordered by severity

1. **P1: nested presentation reacquisition deadlocks an already admitted turn against replacement.** `internal/runtime/tools/executor.go:322-343` unconditionally acquires a new presentation, even if its incoming Go context already contains the turn's presentation. The turn holds the old lease while awaiting MCP; replacement fences new acquisition while awaiting that turn. This is independently reproduced through both the real executor and the authenticated HTTP MCP catalog handler, three times each. Fixing only HTTP context copying will leave the direct executor manifestation live.
2. **P1: MCP registration/reconstruction loses the parent publication authority.** `internal/runtime/mcp/context.go:86,159` and `gateway.go:880` preserve other turn authorities but not the pinned activation presentation. `tools/list` then acquires again; `tools/call` and the legacy tool route also reconstruct the context without the pin. A list-only fix would leave execution rejecting or interpreting another publication. Preserve and consume one owner-issued, process-local capability across the existing registry and all those consumers, not merely a generation string or cached tool list.
3. **P1: startup admits business work before its executable channel publication exists.** Serve passes declared plans but no initial executable publication; `internal/runtime/runtime.go:1151` creates an empty executable publication. `Runtime.Start` releases manager recovery, delivery continuations and autonomous producers (`runtime.go:1697-1744`) before `startServeRuntimeContexts` registers the context (`serveapp/main.go:2428-2465`); local channel reconstruction and `publishChannelActivations` happen afterward (`main.go:1558-1590`). This is a separate ordering gap: preserving the early empty pin does not create the missing authority. The source-order test that currently requires full runtime start before channel publication is not a valid proof of safe business execution admission. This is code-level ordering evidence, not a claim that a second live stack was captured.

The live ~69-second tools/list cancellation matches the proven mechanical cycle, but attribution to that exact production interleaving remains a source-supported inference. Both failed journey deliveries are already marked delivered without the required reply decision card. Do not replay them or count them as successful user journeys.

### Failure-class and architecture ruling

- Category: failure-class / high-risk semantic ownership and startup-versus-runtime parity. Independent gate required and recorded here before the new repair.
- Observed symptom: recovered forced-restart turn cannot discover its MCP reply tool and finishes without the expected reply.
- Chosen working class: **exact executable channel-publication admission and lifetime across startup, model presentation, transport descendants, validation and execution while publication changes**.
- Immediate parent: runtime capability authority can be lost or reacquired at an execution boundary instead of being inherited from its admitted owner.
- Broader parent: runtime execution-boundary policy drift, already mapped by `transport_policy_and_surface_parity`. We are not claiming all tool/credential/provider lifecycle classes are closed.
- Framing: broad enough with this binding amendment; a tools/list-only or timeout-shaped repair is too narrow. This is not an approved first slice of the chosen activation class.
- Architecture smell: canonical owner exists, but the turn and transport do not compose its lifetime; full runtime start also bundles preparation with execution release before channel reconstruction. **Disposition: promote now in #2321**, using the existing activation owner, turn registry and startup owners. No new global lease registry, generic startup framework, retry policy, credential fallback or compatibility/migration path.
- Full bounded closure is feasible in one implementation/review pass if the matrix below is implemented before another paid journey. It is not a one-line patch: startup phase separation and descendant lifetime are real ownership changes. A green owner unit test alone is not a plausible closure target.

### Governing contract

`platform-spec.yaml`: `tool_model.hitl_channel_pack_interface.connected_channel_onboarding.activation_rule` (especially lines 9554-9581 at this head) requires one publication owner, fencing/draining predecessor presentation and execution, inheritance for admitted children, rejection of stale/foreign recovery, and model/MCP use of the same exact publication. Preserve the managed-agent capability surface's exact actor, occurrence, definition-hash and effects checks. Update authoritative `platform-spec.yaml` in the same PR to make transport descendant lifetime and the executable startup barrier explicit. Declared-only preflight remains non-executable.

### Owner / consumer / lifecycle map

| Seam | Classification and required action |
| --- | --- |
| `channelactivation.Owner` / immutable publication / owned leases | Already canonical; add only finite owner-validated descendant operations/lifetime needed below, rather than a second owner |
| `LLMAgent.applyTurnToolDefinitions`, normal/callback turns | Already obtains root presentation; preserve the root through provider completion and admitted children |
| `Executor.AcquireToolDefinitionsForActorInContext` | Move nested acquisition to validated consumption of the existing pin; genuinely new requests still use current admission |
| Turn registry: managed, startup and conversation-fork registrations | Move typed presentation transfer into existing exact token/turn authority; classify every constructor explicitly |
| MCP tools/list, managed definition/evidence filtering, fork catalog | Consume that exact pin; retain canonical hashes and capability filtering; remove eager unleased catalog work from leased paths where redundant |
| MCP tools/call and retained direct tool route | Consume the same authority through request reconstruction and execution, not a separately current catalog |
| Runtime channel operation -> private activity | Existing parent execution borrowing is the positive sibling; preserve it and prove it after the transport change |
| Durable activity replay / recovered turn with no live parent | Different admission case, same owner: fresh exact-current acquisition only; persisted generation is not a live lease |
| Context manager publication / channel refresher / readback | Already delegate to owner; preserve exact occurrence checks and serialized replacement |
| Serve preparation -> context occurrence -> connected-state reconciliation -> channel publication -> business release | Move the publication prerequisite ahead of business admission while preserving existing preparation and effects-recovery owners |
| Startup preflight, selected-contract-fork, fork-chat and API/mock turns | Explicitly classify their root creation versus inherited descendant use; no blanket same-path credit |
| Multi-context serve | Preserve supported backend behavior and current fail-closed multi-context Claude restriction; do not enable a new provider surface for this fix |
| Provider private-state lifetime | Different owner; previous gate and remaining proof obligations stand |
| Diagnostic event/ack atomicity and fork provenance | Different class, #2432 open and separately frozen; this gate does not approve it |
| Credential-absent ingress dormancy | Different class, #2319 open, F-owned and nonblocking under the recorded split |

### Binding implementation instructions

1. Before code, transcribe this complete amendment and the rows below into the versioned pre-audit/spec delta. The issue/watchlist repair is recorded with this gate; another prose-only approval round is not required for these exact conditions.
2. Use an immutable typed process-local binding from the existing activation owner, carried by the existing turn registry. At consumption verify the owning runtime/publication and exact admitted turn/source/actor context. A released, foreign, mismatched or revoked binding must fail closed, never fall back to current acquisition or wait for a successor. Do not serialize authority into tokens, generation strings, tool names or durable rows.
3. Distinguish root admission from a descendant of an already admitted root. The latter may use the predecessor under its replacement fence. New unrelated work remains fenced. All catalog projection, hashes, filtering, runtime dispatch and nested private activity must agree on that predecessor.
4. Define child lifetime, not just pointer transfer: no double release; no root release while an admitted child is still executing; no leaked registry-owned reference until TTL. Use an owner-validated retained child operation or a proven existing join. Parent cancellation/completion, token unregister/expiry/reset and concurrent request resolution must revoke new children and finish/cancel-and-join admitted children before replacement can publish. No noop borrowed release without this proof.
5. Separate serve preparation/publication from business execution release. Obtain the exact prepared context occurrence and required standing/topology facts, perform existing destructive-effect reconciliation and safe local connected-state reconstruction, install the complete canonical executable publication, then admit recovered/new business work and autonomous producers. Retain managed preflight, topology-before-replay, continuations-before-readiness and all rollback/failure gates. Prepared does not mean externally ready/runnable. Do not simply move publication before an occurrence exists, promote declared-only plans to executable authority, or call full `Runtime.Start` as preparation. Factor the existing owners minimally; do not add a second startup scheduler or copy learned rows.
6. Preserve publication writer serialization. The context manager currently serializes replacement under its mutex while leases drain. If changing lock placement, retain exact occurrence validation and serialized replacement/commit; prove readback/child calls do not need a lock held across that same drain. Raw overlapping owner replacement is not independently established as a currently reachable production defect, so do not invent an unrelated concurrency rewrite.
7. No suppressing tools/list errors, extending timeout, retrying Claude, dropping the turn lease, bypassing capability checks or skipping unchanged-publication fences as the sole repair. Ordering alone cannot close live replacement; transport propagation alone cannot close early empty publication.

### Required adversarial manifestation/proof matrix

Each row needs a named proof and its actual outcome in the final audit; split rows only where the separate owner above justifies it.

| Manifestation | Required execution proof |
| --- | --- |
| Same-context nested catalog while replacement waits | Real executor + owner, barrier at fence; child completes on predecessor, successor waits; independent reviewer test currently fails 3/3 |
| Managed HTTP tools/list under that fence | Real registry registration and HTTP resolution, real executor/filter/hash checks, admitted managed occurrence; no resolver success stub for closure |
| Fork HTTP catalog under fence | Actual fork registration and policy, not merely copied managed proof; reviewer fixture handler probe currently fails 3/3 |
| MCP tools/call under fence | Real registered turn through auth/evidence to exact executor; predecessor target used, successor target untouched |
| Direct tool route and channel -> private activity | Named route proof plus existing execution-child borrowing; no current-target substitution |
| Unrelated new request | Remains fenced or returns existing typed refusal; cannot borrow another turn's authority |
| Foreign owner/source/actor/occurrence; stale generation; released/revoked token | Independent fail-closed rows with zero executor/effects invocation |
| Parent completion/cancellation with in-flight child; expiry/unregister/reset races | Deterministic barriers and targeted race run; joined lifetime, no deadlock, no late new child, no leak/double release |
| Replacement cancellation, repeated replacement, readback and shutdown | Existing wait/fence semantics preserved; canceled replacement does not strand current owner; child progress and teardown terminate |
| Cold retained learned binding before recovered business execution | Real serve composition on SQLite/PostgreSQL; pin cannot precede complete executable publication; earlier rebind/effects/topology gates preserved |
| Configured-only, no-channel, unavailable/contradictory learned state | Positive startup controls; invalid state fails before business effects/readiness, without manufacturing learned authority |
| Startup publication/reconciliation failure and partial preparation | No leaked runnable context, provider dispatch or ingress readiness; cleanup errors retained |
| Normal first/resumed, startup probe, selected-fork and fork-chat | Explicit surface-specific proof; inventories and native/MCP authorization unchanged |
| Supported multi-context ordering and unsupported Claude multi-context | Correct per-occurrence publication for supported modes; unsupported CLI mode still refuses |
| Durable replay/recovery without an inherited process lease | Exact new/current authority required; no old process lease adoption |
| Retained graceful/forced and fresh-dev full user journeys | Required reply/card causal path AND accepted turn/delivery convergence on both stores; a delivered row alone is insufficient |
| Remaining provider-backing rows | Real stateless reclaimed successor, probe/fork isolation, unusable/foreign backing, dev/reset and retained-history controls remain required |

Use offline barriers first. Then fresh authorized live acceptance inputs only, never replay the previously settled missing-reply inputs. Do not invent additional sends or credential requests to debug a wait already reproducible offline. Small focused tests use `go test`; broad/integrated runs use `go run ./cmd/swarm-test` with adequate timeout and a stable tree.

### Independent verification and limits

- `TestAgentGChannelPresentationNestedAcquireProbe`, count=3: desired progress FAIL 3/3 on unchanged owner.
- `TestReviewerChannelNestedPresentation`, count=3: real same-context executor and authenticated HTTP MCP fork-catalog desired progress FAIL 3/3 each; unfenced nested acquisition controls pass. Handler test uses a fork-policy resolver fixture, not full managed registry/effects admission, and is not full-path closure evidence.
- Existing owner/connected/admitted targeted controls, count=3: PASS. `TestGatewayMCPToolsForRequestUsesLeasedActivationCatalog`, count=3: PASS; this existing stubbed catalog test does not prove parent handoff.
- Five focused Claude uncertainty/timeout/structured-error/redaction/release tests, count=3: PASS. `TestClaudeStateNamespace`, count=3: PASS.
- Real network-disabled Docker `TestClaudeStateDockerRetentionAndRefusal` and `TestClaudeStateDockerSharedWorkspaceIsolation`: PASS (29.200s aggregate). This independently supports the backing repair, not all its still-open lifecycle rows.
- No paid provider call, Telegram send, delivery replay, whole-suite rerun or production change by this reviewer. The live results and queued full suite are G's evidence, not an independently rerun merge proof.
- Watchlist YAML parsed successfully with Ruby; `git diff --check` clean. G's 2147d1d additions were preserved, with two explicit descendant/startup refinements pushed as `swarm-docs@11ec18c` on `review/2321-gate`.

### Tracking and next work item

#2321 owns this bounded repair now. #2079 is historically closed; its old closure is not evidence that this residual disappeared, and its thread receives an explicit residual-to-#2321 cross-reference. #1965 remains open for broader artifact layout; it does not own this lease fix. #2432 remains open with its already-submitted `cfc60b3b4` fork-provenance amendment awaiting a separate reviewer decision, not another G submission. #2319 remains open/F-owned/nonblocking. No new issue, architecture queue entry or `docs/POTENTIAL_ISSUES.md` entry: actionable new work is assigned here, not left as passive risk.

**G next:** repair inherited presentation consumption/transport and the executable startup barrier together, add the complete offline matrix, then finish the outstanding backing and dual-store journey proofs. Keep diagnostic production changes frozen until #2432's separate gate. Post an exact-head proof audit and integrated swarm-test result before requesting PR review. Stop again only for an actual new owner/contract contradiction or a required weakening of these conditions, not for another wording round.
