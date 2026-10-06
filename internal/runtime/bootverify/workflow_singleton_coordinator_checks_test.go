package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRun_ValidatesSingletonCoordinatorWithContainedState(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, singletonCoordinatorEntitiesYAML(), "", "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("unexpected singleton_coordinator_validation error: %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessSingletonWithIndependentAgentMemory(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, `
coordinator_state: {}
`, `
memory-agent:
  role: analyst
  intent: {inline: "Analyze coordinator jobs."}
  model: regular
  memory: true
  subscriptions: [job.received]
`, "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("agent memory must not create coordinator demand for a stateless singleton, got %#v", report.Errors())
	}
}

func TestRun_AllowsTemplateContainedDeclarationWithoutCoordinatorDemand(t *testing.T) {

	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
instance: vertical_id
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  vertical_id: text
  verticals: map[text]VerticalState
`, "", "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("an inert typed declaration must not manufacture coordinator demand: %#v", report.Errors())
	}
}

func TestRun_RejectsSingletonCoordinatorUnresolvedContainedType(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  verticals: map[text]MissingType
`, "", "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if _, err := bundle.ResolveFlowSingletonCoordinator("coordinator"); err == nil || !strings.Contains(err.Error(), "MissingType") {
		t.Fatalf("coordinator demand must reject unresolved contained type, got %v (report=%#v)", err, report.Errors())
	}
}

func TestRun_DoesNotTreatBareStaticFlowAsSingletonCoordinator(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  status: text
`, "", "")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("bare mode: static must not be interpreted as singleton coordinator, got %#v", report.Errors())
	}
}

func TestRun_SingletonCoordinatorRejectsDynamicContainedTargetPath(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, singletonCoordinatorEntitiesYAML(), "", `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      data_accumulation:
        writes:
          - op: set
            target: entity.verticals[payload.vertical_id]
            key: payload.vertical_id
            value:
              status: "active"
              active_jobs: []
`)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "contained_state_operation_compliance", "dynamic bracket path syntax") {
		t.Fatalf("expected contained_state_operation_compliance dynamic target rejection, got %#v", report.Errors())
	}
}

func TestBuildSingletonCoordinatorDemandProjection_UsesExactTypedConsumers(t *testing.T) {
	bundle := loadCanonicalSingletonCoordinatorFixtureBundle(t, canonicalrouting.SingletonCoordinatorPilotDemandProjection)

	demands := BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle))
	var write, read *SingletonCoordinatorDemand
	for i := range demands {
		demand := &demands[i]
		if demand.Field == "unused_index" {
			t.Fatalf("unused typed field created coordinator demand: %#v", demand)
		}
		if demand.Field != "audit_log" || demand.Node.NodeID() != "coordinator-indexer" || demand.EventType != "lead.observed" || demand.SourceFile == "" {
			continue
		}
		if demand.Kind == "handler.data_accumulation" {
			write = demand
		}
		if demand.Kind == "entity_read.fan_out.items_from" {
			read = demand
		}
	}
	if write == nil || read == nil {
		t.Fatalf("typed demand projection = %#v, want exact direct write and structured fan_out read", demands)
	}
	if !write.HasWriteIndex || write.WriteIndex != 0 {
		t.Fatalf("direct write demand location = %#v, want writes[0]", write)
	}
}

func TestBuildSingletonCoordinatorDemandProjection_CoversScopedReadersWithoutIntrinsicJoins(t *testing.T) {
	tests := []struct {
		name       string
		entities   string
		nodes      string
		wantKind   string
		wantTarget string
	}{
		{
			name: "group_by source",
			entities: `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`,
			nodes: `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      group_by:
        items_from: entity.verticals
        key: status
        store_as: metadata.grouped
`,
			wantKind:   "entity_read.group_by.items_from",
			wantTarget: "entity.verticals",
		},
		{
			name: "join state membership",
			entities: `
coordinator_state:
  members:
    type: "[text]"
    initial: []
`,
			nodes: `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {from: state.members, by: payload.vertical_id}
        output: payload.job
        on_complete: {advances_to: done}
`,
			wantKind:   "entity_read.join.members.from",
			wantTarget: "entity.members",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSingletonCoordinatorFixtureBundle(t, `name: coordinator
stages:
  active: {}
  done: {final: true}
  failed: {final: true}
pins:
  inputs:
    - job.received
`, tc.entities, "", tc.nodes)
			demands := BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle))
			var matched *SingletonCoordinatorDemand
			for _, demand := range demands {
				if demand.Kind == "workflow_join" {
					t.Fatalf("join declaration created intrinsic coordinator demand: %#v", demand)
				}
				if demand.Kind == tc.wantKind && demand.Target == tc.wantTarget && demand.FlowID == "coordinator" && demand.Node.NodeID() == "coordinator-node" && demand.SourceFile != "" {
					if matched != nil {
						t.Fatalf("duplicate scoped reader demand: %#v", demands)
					}
					matched = &demand
				}
			}
			if matched == nil {
				t.Fatalf("demands = %#v, want kind %q target %q with exact scoped provenance", demands, tc.wantKind, tc.wantTarget)
			}
			if got := matched.Field; got != strings.TrimPrefix(tc.wantTarget, "entity.") {
				t.Fatalf("scoped reader field = %q, want exact target field", got)
			}
		})
	}
}

func TestBuildSingletonCoordinatorDemandProjection_PreservesDuplicateScopedNodeIDs(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.CopyDuplicateScopedSingletonDemand(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	demands := BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle))
	for _, demand := range demands {
		if demand.FlowID == "a" && demand.Node.NodeID() == "shared-node" && demand.Field == "items" && demand.SourceFile != "" {
			return
		}
	}
	t.Fatalf("duplicate scoped-node demands = %#v, want flow a shared-node items write", demands)
}

func TestBuildSingletonCoordinatorDemandProjection_DoesNotTreatQuerySelectAsExpression(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      query:
        source: payload.job
        select: [entity.verticals]
        store_as: metadata.selected
`)

	for _, demand := range BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle)) {
		if demand.Target == "entity.verticals" || demand.Field == "verticals" {
			t.Fatalf("literal query selector created singleton coordinator demand: %#v", demand)
		}
	}
}

func TestBuildSingletonCoordinatorDemandProjection_DoesNotTreatUnevaluatedFieldsAsExpressions(t *testing.T) {
	tests := []struct {
		name      string
		operator  string
		rejection string
	}{
		{
			name: "filter predicate",
			operator: `filter:
        source: payload.job
        predicate: entity.verticals
        condition: |-
                     true
        store_as: metadata.filtered`,
			rejection: `filter field "predicate"`,
		},
		{
			name: "reduce params",
			operator: `reduce:
        source: payload.job
        operation: count
        params:
          value: entity.verticals
        store_as: metadata.reduced`,
			rejection: `reduce field "params"`,
		},
		{
			name: "filter source shadowed by items from",
			operator: `filter:
        source: entity.verticals
        items_from: payload.job
        condition: |-
                     true
        store_as: metadata.filtered`,
		},
		{
			name: "reduce source shadowed by items from",
			operator: `reduce:
        source: entity.verticals
        items_from: payload.job
        operation: count
        store_as: metadata.reduced`,
		},
		{
			name: "count source shadowed by items from",
			operator: `count:
        source: entity.verticals
        items_from: payload.job
        store_as: metadata.counted`,
		},
		{
			name: "guard check shadowed by checks",
			operator: `guard:
        check: entity.verticals
        checks:
          - id: ready
            check: payload.job != null`,
		},
		{
			name: "nested query row",
			operator: `query:
        - source: entity.verticals
          store_as: metadata.rows`,
			rejection: "must be a mapping, got sequence",
		},
		{
			name: "activity approval decision",
			operator: `activity:
        tool: review
        approval:
          decision: entity.release`,
		},
		{
			name: "on complete activity input",
			operator: `on_complete:
        - condition: |-
                       true
          activity:
            tool: review
            input:
              value: entity.verticals`,
			rejection: "on_complete.activity is unsupported",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.rejection != "" {
				// Retired inert forms cannot create demand because they no longer
				// reach the executable projection at all.
				handler, err := admitBootHandlerFixture(t, tc.operator)
				if err == nil || !strings.Contains(err.Error(), tc.rejection) {
					t.Fatalf("admission error=%v, want %s", err, tc.rejection)
				}
				if handler.Query != nil || handler.Filter != nil || handler.Reduce != nil {
					t.Fatalf("rejected form reached demand projection: %#v", handler)
				}
				return
			}
			bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      `+tc.operator+`
`)

			for _, demand := range BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle)) {
				if demand.Target == "entity.verticals" || demand.Field == "verticals" {
					t.Fatalf("unevaluated field created singleton coordinator demand: %#v", demand)
				}
			}
		})
	}
}

func TestBuildSingletonCoordinatorDemandProjection_TreatsLoopFromAsStageIdentifier(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
stages:
  entity.verticals: {}
  review: {}
  exhausted: {final: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 2
    escape:
      advances_to: exhausted
pins:
  inputs:
    - job.received
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      loop: {start: revision, from: entity.verticals}
      advances_to: review
`)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if findingContainsAll(report.Errors(), "expression_field_reference_validation", "loop.from", "entity.verticals") {
		t.Fatalf("literal loop source stage failed expression validation: %#v", report.Errors())
	}

	for _, demand := range BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle)) {
		if demand.Target == "entity.verticals" || demand.Field == "verticals" {
			t.Fatalf("literal loop source stage created singleton coordinator demand: %#v", demand)
		}
	}
}

func TestRun_CountArrivalJoinDoesNotRequireContainedCoordinatorState(t *testing.T) {
	const schema = `name: coordinator
stages:
  active: {}
  done: {final: true}
  failed: {final: true}
pins:
  inputs:
    - job.received
`
	const nodes = `
coordinator-node:
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {count: 1, by: payload.vertical_id}
        output: payload.job
        on_complete: {advances_to: done}
        deadline: {after: 1h, from: stage_entry}
        on_deadline: {advances_to: failed}
`
	tests := []struct {
		name     string
		entities string
	}{
		{name: "declared stateless primary", entities: "coordinator_state: {}\n"},
		{name: "unused scalar", entities: "coordinator_state:\n  unused_label: text\n"},
		{name: "unused list", entities: "coordinator_state:\n  unused_members: '[text]'\n"},
		{name: "unused map", entities: "coordinator_state:\n  unused_verticals: map[text]VerticalState\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSingletonCoordinatorFixtureBundle(t, schema, tc.entities, "", nodes)
			source := semanticview.Wrap(bundle)
			plans := source.WorkflowJoins()
			if len(plans) != 1 {
				t.Fatalf("count arrival declaration was not admitted exactly: %#v", plans)
			}
			plan := plans[0]
			if plan.Mode != runtimecontracts.WorkflowJoinModeArrival || plan.Spec.Members.Count == nil || *plan.Spec.Members.Count != 1 || plan.Spec.Members.From != "" || plan.Spec.Members.By != "payload.vertical_id" {
				t.Fatalf("count arrival membership was not admitted exactly: %#v", plan)
			}
			if plan.Spec.Output != "payload.job" || plan.Spec.Deadline == nil || plan.Spec.Deadline.From != runtimecontracts.JoinDeadlineFromStageEntry || plan.Spec.OnDeadline.AdvancesTo != "failed" {
				t.Fatalf("count arrival output/deadline was not admitted exactly: %#v", plan)
			}
			if demands := BuildSingletonCoordinatorDemandProjection(source); len(demands) != 0 {
				t.Fatalf("count arrival or unused primary fields created coordinator demand: %#v", demands)
			}
			report := Run(context.Background(), source, Options{})
			if reportContains(report.Errors(), "singleton_coordinator_validation", "") || reportContains(report.Errors(), "join_validation", "") {
				t.Fatalf("count arrival refused without contained coordinator state: %#v", report.Errors())
			}
		})
	}
}

func loadCanonicalSingletonCoordinatorFixtureBundle(t *testing.T, variant canonicalrouting.SingletonCoordinatorPilotVariant) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.CopySingletonCoordinatorPilot(t, variant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func loadSingletonCoordinatorFixtureBundle(t *testing.T, flowSchema, flowEntities, flowAgents, flowNodes string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: singleton-coordinator-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "schema.yaml"), strings.TrimSpace(flowSchema)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "types.yaml"), singletonCoordinatorTypesYAML())
	writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "entities.yaml"), strings.TrimSpace(flowEntities)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "events.yaml"), `
job.received:
  vertical_id: text
  job: Job
`)
	if strings.TrimSpace(flowAgents) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "agents.yaml"), strings.TrimSpace(flowAgents)+"\n")
	}
	if strings.TrimSpace(flowNodes) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "coordinator", "nodes.yaml"), strings.TrimSpace(flowNodes)+"\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func singletonCoordinatorTypesYAML() string {
	return `
types:
  VerticalState:
    status: text
    active_jobs: "[Job]"
  Job:
    id: text
    title: text
`
}

func singletonCoordinatorEntitiesYAML() string {
	return `
coordinator_state:
  verticals: map[text]VerticalState
`
}
