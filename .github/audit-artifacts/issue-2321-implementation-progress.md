# Issue #2321 Implementation Progress

Agent: agent-g. Status: verification exposed an additional lifecycle diagnostic
projection defect; awaiting lead classification, not review-ready.
This is not a Post-Implementation Proof Audit or a first-slice
closure claim. The approved 29-manifestation class remains binding.

## Authority And Integration

- Independent gate cycle 2: https://github.com/division-sh/swarm/issues/2321#issuecomment-5584592517.
- Implementation base: origin/master@bdc82dfba, preserving its run-scoped identities,
  receiver/lifecycle contracts, terminal retirement and fresh activity lineage.
- Approved audit and 13-target spec delta remain historical design artifacts;
  authoritative rules are promoted into platform-spec.yaml alongside implementation.
- Existing docs watchlist refinement efac8cd is consumed. No new issue or architecture
  framework. #2319 remains F-owned and open; #1801/#2284 remain separate.
- Original Agent G worktree's unrelated #2008 WIP is untouched.

## Implemented

- Structural readers share admitted effective source and the existing validation
  report. No credential/MCP/machine observations; declared model/tool/workspace
  constraints, supplied doubles, severity and harness independence remain checked.
  Output separates structural validity from unobserved live readiness.
- Commands select Live (serve/dev) or MockOnly (test). Authored doubles do not select
  a backend or waive live credentials/native/Claude/workspace requirements.
- ResolveAgentExecution owns write-time executable descriptor projection. Live
  descriptors omit executable doubles; mock descriptors retain exact admitted
  performance and live-model metadata. ValidateAgentExecutionDescriptor consumes
  complete persisted truth unchanged at dispatch, adoption, recovery and selected
  fork. Removed unused backend-migration helper; no read-side reselection.
- Manager fresh/blueprint/reconfigure paths use the selector; persisted paths
  validate. Selected-fork blueprint creation now carries configured model aliases
  before sealing, rather than depending on later read-side model resolution.
- Native boot checks use the same command-selected descriptor and real runtime
  capability owner. Same-source mock/live negative proof preserves native refusals.
- Retired runtime.execution_posture and mock backend selectors reject with originating
  file/key, including masked config layers. Recovery defaults true; explicit false
  survives merging. Removed all five shipped deployment configurations and stale
  graduation teaching. Artifact census rejects newly introduced deployment files.
- TestSessionRunner is injected into the CLI; RunTestSession uses the same
  buildRuntimeComposition as Run. Admission of source, scenarios, variables, setup,
  effective identities and exact mock completeness precedes acquisition.
- Public test owns fresh temporary SQLite, workspace/data roots, ephemeral loopback
  listeners and generated memory auth. No ambient API/context/store/credential
  selection, provider ingress, public exposure or registration. Existing process,
  selected-store, source, workspace and supervisor owners perform joined teardown.
- Private-root cleanup runs after joined shutdown and removes immutable projections
  without following symlinks out of the owned root. Cleanup errors remain failures.
- Internal retained mock proof uses TestOwnedMockLifecycleProcessEntry in a go test -c
  binary, not a production flag. Parent retains isolated SQLite/PostgreSQL and signs
  real local ingress across child restarts. Internal scenario protocol fixtures use
  explicit injected parent-owned sessions, never a connected-test compatibility path.
- Golden possession/invocation-root tests retain real public live serve, now using the
  agent-free canonical RootIngress fixture because these tests prove process ownership,
  not agent execution. Retained agent workload tests stay explicitly internal (H).

## Executed Evidence

These are development results, not a final integrated-head closure matrix.

- Public compiled verify/describe/graph/routes passed on unchanged webhook scaffold
  without external executables or readable credential documents; explicit structural
  output and invalid-source/no-probe tests pass.
- Actual compiled public bare test and verify passed on both unchanged scaffolds
  without live credentials/Claude/Docker.
- Full CLI package passed (41.906s); full catalog package passed (368.598s).
- Full manager, bootverify, llm and selector packages passed at the earlier command
  selection checkpoint; later integrated run remains required.
- Private session success/callback failure/cancellation tests passed: auth required,
  ingress absent, listeners joined, DB closed, private root removed.
- Private startup failures before store acquisition, after schema acquisition and
  during workspace preparation passed. Workspace release invoked once; injected
  release failure surfaced. Common public listener-bind refusal tests also passed.
- Concurrent public live runtime noninterference and immutable projection cleanup
  tests passed.
- Internal MockAgent signed-ingress/retained-restart proof passed on both stores.
  Golden SQLite internal smoke passed. Full internal J1-J5 lifecycle matrix passed
  on SQLite/PostgreSQL (54.878s), preserving receipt chain, semantic identities,
  old snapshots, crash convergence and fresh-epoch cardinality.
- Internal authored/derived/root-setup/atomic-publication scenarios passed. Exact
  scenario profiles and mismatch refusal passed across SQLite/PostgreSQL.
- Selected-fork provider/model/failure-compensation focused tests passed (4.712s),
  including custom model preservation after configured aliases change.
- Controlled live Anthropic/Telegram transport proof passed with source doubles
  unchanged. This is not paid external-provider or provisioned Claude acceptance.
- Native boot purpose/refusal matrix passed. API specification tests passed (1.820s).
  Production async-owner census passed (0.865s); focused persistence boundary guards
  passed. Retired product-surface census and git diff checks passed.
- Actual compiled public live possession and invocation-root tests passed (14.064s),
  including retained/dev ownership, hostile ancestor and borrowed-project refusal.
  Agent-free fixture: this does not claim live-provider turns or agent restarts.

Large/whole suites use go run ./cmd/swarm-test; targeted tests use ordinary go test.
The first unfiltered whole suite failed in nine packages. In-scope fixture/guard
repairs are committed in 009e18c34 and pass targeted checks, including Claude
retry/restart, directive acknowledgment/rollback, private-runtime isolation and
exact descriptor consumption. The second unfiltered run completed with two failing
packages (conformance and serveapp). The remaining fixture repairs pass targeted
startup, decoder, abandonment and complete dual-store mock emission checks. New
dual-store persisted descriptor corruption/read-only proof also passes. The next
unfiltered run will enable the full proof profile, including retained lifecycle
journeys. No whole-suite success is claimed. The complete 29-row checklist and proof
gaps are recorded in issue-2321-proof-accounting.md; it is not a final proof audit.

## Remaining Work And Acceptance

1. Complete the unfiltered integrated swarm-test run and repair in-scope failures.
   Rerun changed proof families on the final commit rather than aggregating obsolete
   checkpoint results into a final-head pass.
2. Finish explicit M1-M29 owner/consumer/proof accounting, including failure injection
   for every acquired resource and all required selection/reintroduction negatives.
   A shared constructor or pre-existing passing test is not alone closure proof.
3. Complete actual live serve/dev/signed-ingress/graceful/crash restart proof on both
   stores with provisioned prerequisites. This host lacks Claude and the workspace
   image. An environment/configuration location has been requested; no credentials
   were logged and no paid live turn has been attempted.
4. Keep credential-absent webhook startup/activation and Telegram example collapse
   explicitly split/tracked with F in #2319, not passed. The approved split permits
   independent #2321 review/merge/closure; the broader onboarding claim stays open.
5. Record the final-head exhaustive proof audit, then open the single normal PR
   only when review-ready. No final proof audit, PR or failure-class closure exists
   at this checkpoint.

## Additional Verification Finding

The third complete run used the full proof profile and passed releasee2e (533.640s),
including the retained J1-J5 and golden restart/burst assertions. It did not pass
the whole suite: conformance reported a terminal lifecycle diagnostic projection
conflict; serveapp found one unmigrated mixed-agent fixture and two ambiguous audit
text references; runtimepersistence exhausted the default aggregate ten-minute
package timeout while its current subtest had run only two seconds.

The mixed-agent fixture now uses the existing H entry, not public live serve as a
mock host; the exact readback/cardinality assertions remain. Both store cases and
the audit-text guard pass (15.019s). These are bounded test/document corrections.

The lifecycle conflict is not dismissed as flaky or suppressed. Ten focused real
SQLite three-child repetitions passed (61.285s), but a deterministic two-reader
barrier probe fails three of three times: the same pending diagnostic is logged
twice and the second mark returns `lifecycle diagnostic projection conflict`.
The probe exercises the production manager projector with an explicit snapshot/CAS
test double; it is not additional real-store execution credit. The real SQLite
manifestation is the full-profile failure. Reproduction source is retained in
`issue-2321-diagnostic-projection-probe.go.txt`, not in the passing test inventory.

`projectLifecycleDiagnostics` is byte-identical to the integrated bdc82dfba base.
Its list/log/mark steps have no concurrent claim or serialization, and the two
store mark operations intentionally require one newly updated row. Terminal
completion, loop/start paths and startup hydration can all call this projector.
Lifecycle diagnostic convergence is distinct from command-owned selection and
needs lead tracker/scope disposition before production remediation. No projector,
store CAS, lifecycle ownership, or diagnostic failure policy was changed here.
No new issue or reopening of a closed tracker has been performed by this work.

## Lead Disposition And Separate Repair Gate

The preceding request for classification is superseded by the lead ruling on
#2321: https://github.com/division-sh/swarm/issues/2321#issuecomment-5588210838.
#2321 keeps its approval. The lead created separate acceptance prerequisite #2432
for lifecycle diagnostic projection convergence AND provenance, including sequential
acknowledgement failure and cross-manager caller-context contamination. #1927 stays
closed; #2250 retains broader phase-ownership debt.

Agent G posted the bounded #2432 Pre-Implementation Coverage Audit at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5588649360,
with checked-in artifact at 5e303a531 on agent-g/2432-diagnostic-audit, based on
fresh origin/master 496498457. It requests an independent repair gate for one
selected-store atomic projection/ack operation, exact persisted mode/provenance,
and auxiliary-error separation. Production diagnostic repair remains unstarted.
The existing watchlist refinement swarm-docs@12e94ca was consumed. No additional
watchlist change or issue is proposed.

Independent #2321 work remains authorized. F's #2319 is still open; a current
gate/activation schedule and PR reference have been requested on its thread.
Live-provider provisioning was rechecked: Docker exists, but this box has neither
Claude nor a Swarm workspace image (only Go and PostgreSQL images). No external
provider execution is claimed. The exact planned TestCommandLiveServeAndRestartParity
journey still needs implementation and execution; the existing paid Claude lifecycle
test is not an automatic replacement for the approved serve/dev/restart matrix.
An environment/configuration location, not credential values, has been requested.

After integrating the separately gated repair, rerun the failing conformance family
and dual-store lifecycle proof, then the whole suite with explicit package budget:
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -timeout=30m ./...`.
Individual scenario deadlines remain enforced; a repeated timeout requires diagnosis.
No final proof audit, review-ready PR, whole-suite pass or runtime closure is claimed.

## Diagnostic Gate Amendment And Provisioning

The first independent #2432 gate was insufficient: selected-fork lifecycle
producers and both activation validators consume the same durable diagnostic path.
G posted the additive provenance amendment at
https://github.com/division-sh/swarm/issues/2432#issuecomment-5589274582,
pushed as cfc60b3b4 on agent-g/2432-diagnostic-audit. It requests a focused
independent re-gate; diagnostic implementation remains frozen. Watchlist 7a23362
already records this correction. #2321's approval is unchanged.

Live environment provisioning has started through the supported command:
`env -u SWARM_TEST_POSTGRES_DSN go run ./cmd/swarm workspace build --backend claude_cli --image swarm-workspace:agent-g-2321`.
The first invocation correctly refused the inherited test-quarantined variable;
the retry reached Docker package installation. Build/Claude validation has not yet
completed at this checkpoint, so no provisioned image or authenticated provider
turn is credited. The command log is /tmp/agent-g-2321-workspace-build.log.
The requested supported credential configuration location remains outstanding.
F's #2319 remains open with no reply to the existing coordination request.

## Approved Independent Acceptance

The user-approved split supersedes earlier F-dependency wording:
https://github.com/division-sh/swarm/issues/2321#issuecomment-5590488648.
Watchlist a5ed497 is consumed. #2321 can be reviewed, merged and closed without
waiting for #2319 once #2432 repair, genuine provisioned L live/restart proof,
all in-class checks, the integrated full suite and independent review pass.
Credential-absent ingress startup/activation and Telegram example collapse remain
F-owned and explicitly split/tracked, never passed or credited to H. Existing
fail-closed ingress credential checks remain valid; no mock fallback is authorized.
#2432's fork-provenance re-gate remains pending; no diagnostic implementation starts.

Provisioning follow-through: the default-network workspace build failed because
the build container could not resolve deb.debian.org. Host and host-network
container DNS resolve it. Retrying the unchanged Dockerfile.workspace through
Docker with build-time --network host succeeded; no runtime networking or credential
policy change. The supported public workspace build then passed against cached
layers, including its runnable-Claude validation. Image
swarm-workspace:agent-g-2321 is available; direct `claude --version` returned
2.1.87. Logs: /tmp/agent-g-2321-workspace-host-build.log and
/tmp/agent-g-2321-workspace-build.log. No authenticated provider turn, runtime
network readiness or retained restart is credited. The expected OAuth credential
is absent from this process environment; the supported configuration location
request remains outstanding. The exact L harness still needs implementation.

Administrative split verification: API-spec package passed (4.773s), both YAML
documents parsed, and git diff --check passed. No production runtime code changed;
the full suite is deferred to the integrated diagnostic repair, not claimed green.
