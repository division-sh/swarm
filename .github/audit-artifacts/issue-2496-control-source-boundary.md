# Historical Constructed Source Consumer Boundary

This is the pre-repair d8d863c3d record, not current implementation status.
Ruling5967912454 authorized the complete bounded consumer migration; current
code/proof state is issue-2496-construction-source-repair.md. Its historical
refusals remain evidence, but its pending ownership question is superseded.

Qualified code/test/spec source: d8d863c3dd54df3e3c89cdc0c9b05482d2cd44d1.
The later ledger-only commit does not relabel these executions as its own.

## Existing Owners And Consumption

The constructor owns exact Instance/ParentRoute/ParentEntityID. Join lifecycle
owns immutable entry/member/arm/continuation evidence; decision-card lifecycle
owns its exact gate anchor. AgentExecutionSemanticScope owns declared actor
name/flow/execution agreement, not permission to construct an instance. The
flow/source projections must consume these owners without reconstructing an
absolute path or turning parent identity into recipient/connection authority.

The exact production symbol census finds:

- joinSchedule: closed-join reconciliation, stage-exit cancellation,
  initial/stage-entry arming, pending deadline cancellation and pending
  completion upsert (five workflow_lifecycle_plan.go callers).
- buildWorkflowDecisionCard: stage-gate source projection. These are the two
  production consumers of AdmitFlowExecutionRoutingSource.
- AdmitAgentExecutionRoutingSource: direct AgentExecutionSemanticScope consumer.
- ResolveAgentPlanExecutionSemanticScope: the same semantic-scope owner's plan
  wrapper, consumed by bus/selected_input_validation.go and
  runforkexecution/preparation.go. Neither selected path is proven by a direct
  component call alone.

All these source projections remain open consumption obligations. No patch has
been made to the shared join/lifecycle/gate/agent semantic-scope files in this
batch. Existing agent declaration/run/entity/generation and explicit-target
refusals must survive; new agent naming, row-state-machine or replay work is not
implicitly promoted. PrepareInitialEntryLifecycle and
FlowInstanceActivationPlan signatures remain unchanged.

## Execution Evidence

The committed real selected-store test
TestWorkflowJoinConstructedDescendantAdmissionOnBothStores is RED on SQLite
and PostgreSQL at57cd0229d under race/count3, then RED on both in the unchanged
d8d863c3d managed default. Preparation fails before construction commits or the
test's downstream admission/non-mutation assertions. Complete receipt:
issue-2496-handoff-qualification.json; composed result:
issue-2496-default-d8d863c3d-red.json. This proves the timed-join manifestation,
not installed-agent/provider execution or the gate manifestation.

The isolated source sibling probe is retained verbatim in
issue-2496-agent-parent-source-probe.go.txt. It first calls the canonical
KeylessChild and ValidateConstruction, with explicit RootSchema/FlowSchemas
and exact tree parent links. It then invokes both existing source APIs for the
same exact authored scope orders/child and concrete orders/one/child.

Actual command (candidate untouched; this is an overlay probe, not an
unmodified-commit regression):

```sh
go test -overlay=/tmp/agent-e-2496-agent-parent-source-overlay.json \
  ./internal/runtime/core/pinrouting \
  -run '^TestConstructedParentSourceSiblingProbe$' -count=1 -v
```

Both component cells refuse, package0.005s:

- Flow source: static flow orders/child requires an instance owned by the
  declaration path orders/child.
- Declared-agent source: agent execution route orders/one/child conflicts with
  declaration flow path orders/child, in ResolveAgentExecutionSemanticScope
  before the shared source projector.

Corrected raw receipt: /tmp/agent-e-2496-agent-parent-source-probe-corrected.log,
SHA256011dc5556ca389a348d6e786b05d9be5d9b65b034a88e311da07734c64944052.
Probe bytes SHA256e0124d10f7c6d1ab6bd688c08de31807599784f170ed7acdd97db03f85a6399a.
The initial incomplete synthetic source refused at the constructor prerequisite;
it receives no downstream credit. Its separate raw receipt SHA256 is
8f1c90027754717d91766ea180cfa0cf503b73c4de45eaa4ee28b69d56947c0f.

## Disposition And Tracking

The new known consumer census is recorded in existing #2496/#2525, not a
silent follow-up or a class-elimination claim. The construction/parent-topology
watchlist refinement and precise A/E/B carrier ownership confirmation are
pending reviewer coordination before additional shared implementation:

- https://github.com/division-sh/swarm/issues/2496#issuecomment-5967326775
- https://github.com/division-sh/swarm/issues/2496#issuecomment-5967680220

The bounded direction is consumption of existing exact construction facts;
no identity derivation, ancestry-based routing permission, row/generation
framework or expanded replay capability is requested. #2497 remains the
separate queued agent-row work. Binding contract: platform-spec.yaml
engine.flow_constructor.recursive_commit and
engine.dynamic_instance_lifecycle.route_materialization.canonical_template_source.

All21 A handoff roots PASS race/count1; all66 earlier default failures now PASS.
All13 later planned units PASS separately after the default stops at broad-01.
The default remains RED, with five explicit Docker/remote-tracker skips, and
this head is not merge-ready. No push or hosted CI request occurred.
