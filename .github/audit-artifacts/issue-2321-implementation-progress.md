# Issue #2321 Implementation Progress

Agent: agent-g. Status: implementation and integrated verification in progress,
not review-ready. This is not a Post-Implementation Proof Audit or a first-slice
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
  passed. Retired Builder semantic census and git diff checks passed.
- Actual compiled public live possession and invocation-root tests passed (14.064s),
  including retained/dev ownership, hostile ancestor and borrowed-project refusal.
  Agent-free fixture: this does not claim live-provider turns or agent restarts.

Large/whole suites use go run ./cmd/swarm-test; targeted tests use ordinary go test.
The unfiltered whole suite acquired its slot after 13m28s. Its result is
pending; no whole-suite success is claimed.

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
4. Consume F's #2319 activation proof before combined absent-credential webhook
   acceptance. Its open status does not block independent implementation.
5. Record the final-head exhaustive proof audit, then open the single normal PR
   only when review-ready. No final proof audit, PR or failure-class closure exists
   at this checkpoint.

No new semantic contradiction or approved stop condition has been discovered.
