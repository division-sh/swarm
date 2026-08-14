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
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, singletonCoordinatorEntitiesYAML(), "", "{}\n")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("unexpected singleton_coordinator_validation error: %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessSingletonWithIndependentAgentMemory(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state: {}
`, `
memory-agent:
  id: memory-agent
  role: analyst
  intent: {inline: "Analyze coordinator jobs."}
  model: regular
  memory: true
  subscriptions: [job.received]
`, "{}\n")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("agent memory must not create coordinator demand for a stateless singleton, got %#v", report.Errors())
	}
}

func TestRun_RejectsSingletonCoordinatorTemplateInstanceMix(t *testing.T) {

	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
instance: vertical_id
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, singletonCoordinatorEntitiesYAML(), "", "{}\n")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "singleton_coordinator_validation", "must not declare template instance") {
		t.Fatalf("expected singleton_coordinator_validation template instance error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSingletonCoordinatorUnresolvedContainedType(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state:
  verticals: map[text]MissingType
`, "", "{}\n")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "singleton_coordinator_validation", "MissingType") {
		t.Fatalf("expected singleton_coordinator_validation unresolved contained type error, got %#v", report.Errors())
	}
}

func TestRun_DoesNotTreatBareStaticFlowAsSingletonCoordinator(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: static
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state:
  status: text
`, "", "{}\n")

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "singleton_coordinator_validation", "") {
		t.Fatalf("bare mode: static must not be interpreted as singleton coordinator, got %#v", report.Errors())
	}
}

func TestRun_SingletonCoordinatorRejectsDynamicContainedTargetPath(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, singletonCoordinatorEntitiesYAML(), "", `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      data_accumulation:
        writes:
          - op: set
            target: entity.verticals[payload.vertical_id]
            key:
              ref: payload.vertical_id
            value:
              status: active
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
		if demand.Field != "audit_log" || demand.NodeID != "coordinator-indexer" || demand.EventType != "lead.observed" || demand.SourceFile == "" {
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

func TestBuildSingletonCoordinatorDemandProjection_CoversScopedReadersAndIntrinsicJoins(t *testing.T) {
	tests := []struct {
		name       string
		entities   string
		nodes      string
		wantKind   string
		wantTarget string
	}{
		{
			name: "group_by key",
			entities: `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`,
			nodes: `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      group_by:
        items_from: payload.job
        key: entity.verticals
        store_as: metadata.grouped
`,
			wantKind:   "entity_read.group_by.key",
			wantTarget: "entity.verticals",
		},
		{
			name: "accumulate window",
			entities: `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`,
			nodes: `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      accumulate:
        into: jobs
        from: payload.job
        window: entity.verticals
`,
			wantKind:   "entity_read.accumulate.window",
			wantTarget: "entity.verticals",
		},
		{
			name:     "payload backed join",
			entities: "coordinator_state: {}\n",
			nodes: `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {from: payload.job, by: payload.vertical_id}
        output: payload.job
        on_complete: {advances_to: done}
        timeout: {after: 1h, advances_to: failed}
`,
			wantKind:   "workflow_join",
			wantTarget: "active",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
initial_state: active
states: [active, done, failed]
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, tc.entities, "", tc.nodes)
			demands := BuildSingletonCoordinatorDemandProjection(semanticview.Wrap(bundle))
			for _, demand := range demands {
				if demand.Kind == tc.wantKind && demand.Target == tc.wantTarget && demand.FlowID == "coordinator" && demand.NodeID == "coordinator-node" && demand.SourceFile != "" {
					return
				}
			}
			t.Fatalf("demands = %#v, want kind %q target %q with exact scoped provenance", demands, tc.wantKind, tc.wantTarget)
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
		if demand.FlowID == "a" && demand.NodeID == "shared-node" && demand.Field == "items" && demand.SourceFile != "" {
			return
		}
	}
	t.Fatalf("duplicate scoped-node demands = %#v, want flow a shared-node items write", demands)
}

func TestBuildSingletonCoordinatorDemandProjection_DoesNotTreatQuerySelectAsExpression(t *testing.T) {
	bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  id: coordinator-node
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
		name     string
		operator string
	}{
		{
			name: "filter predicate",
			operator: `filter:
        source: payload.job
        predicate: entity.verticals
        condition: "true"
        store_as: metadata.filtered`,
		},
		{
			name: "reduce params",
			operator: `reduce:
        source: payload.job
        operation: count
        params:
          value:
            ref: entity.verticals
        store_as: metadata.reduced`,
		},
		{
			name: "filter source shadowed by items from",
			operator: `filter:
        source: entity.verticals
        items_from: payload.job
        condition: "true"
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
			name: "nested query row",
			operator: `query:
        - source: entity.verticals
          store_as: metadata.rows`,
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
        - condition: "true"
          activity:
            tool: review
            input:
              value:
                ref: entity.verticals`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSingletonCoordinatorFixtureBundle(t, `
name: coordinator
mode: singleton
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  id: coordinator-node
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
mode: singleton
stages:
  entity.verticals: {initial: true}
  review: {}
  exhausted: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 2
    escape:
      advances_to: exhausted
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`, `
coordinator_state:
  verticals:
    type: map[text]VerticalState
    initial: {}
`, "", `
coordinator-node:
  id: coordinator-node
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

func TestRun_IntrinsicJoinDemandRejectsStatelessAndAcceptsStatefulCoordinator(t *testing.T) {
	const schema = `
name: coordinator
mode: singleton
initial_state: active
states: [active, done, failed]
pins:
  inputs:
    events: [job.received]
  outputs:
    events: []
`
	const nodes = `
coordinator-node:
  id: coordinator-node
  execution_type: system_node
  subscribes_to: [job.received]
  event_handlers:
    job.received:
      join:
        stage: active
        members: {from: payload.job, by: payload.vertical_id}
        output: payload.job
        on_complete: {advances_to: done}
        timeout: {after: 1h, advances_to: failed}
`
	tests := []struct {
		name       string
		entities   string
		wantDemand bool
	}{
		{name: "stateless", entities: "coordinator_state: {}\n", wantDemand: true},
		{name: "stateful", entities: "coordinator_state:\n  verticals: map[text]VerticalState\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadSingletonCoordinatorFixtureBundle(t, schema, tc.entities, "", nodes)
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			got := reportContains(report.Errors(), "singleton_coordinator_validation", "workflow_join")
			if got != tc.wantDemand {
				t.Fatalf("singleton coordinator errors = %#v, want workflow_join demand failure %t", report.Errors(), tc.wantDemand)
			}
		})
	}
}

func TestRun_FanInDemandRejectsStatelessSingletonAtInputRow(t *testing.T) {
	bundle := loadCanonicalSingletonCoordinatorFixtureBundle(t, canonicalrouting.SingletonCoordinatorPilotStatelessFanIn)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "coordinator demand requires at least one typed contained map/list field") {
		t.Fatalf("fan-in stateless singleton errors = %#v, want exact input-row coordinator demand failure", report.Errors())
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: singleton-coordinator-fixture
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: coordinator
    flow: coordinator
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: singleton-coordinator-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "schema.yaml"), strings.TrimSpace(flowSchema)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "types.yaml"), singletonCoordinatorTypesYAML())
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "entities.yaml"), strings.TrimSpace(flowEntities)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "events.yaml"), `
job.received:
  vertical_id: text
  job: Job
`)
	if strings.TrimSpace(flowAgents) == "" {
		flowAgents = "{}\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "agents.yaml"), strings.TrimSpace(flowAgents)+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "coordinator", "nodes.yaml"), strings.TrimSpace(flowNodes)+"\n")
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
