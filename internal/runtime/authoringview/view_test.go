package authoringview

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatereply"
)

func TestBuildShowsReplyPairedTopology(t *testing.T) {
	source := templatereply.LoadSource(t, templatereply.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)
	edges := interFlowRouteEdges(view.RoutingTopology)
	if len(edges) != 2 {
		t.Fatalf("reply route edges = %#v, want request and response", edges)
	}
	roles := map[string]*routingtopology.Reply{}
	for _, edge := range edges {
		if edge.Resolution == nil || edge.Resolution.Reply == nil {
			t.Fatalf("reply topology missing from edge %#v", edge)
		}
		roles[edge.Resolution.Reply.Role] = edge.Resolution.Reply
	}
	for _, role := range []string{"request", "response"} {
		reply := roles[role]
		if reply == nil || reply.RequestOutputPin != templatereply.RequesterRequestPin || reply.ReplyInputPin != templatereply.RequesterReplyPin || reply.ProviderInputPin != templatereply.ProviderRequestPin || reply.ProviderOutputPin != templatereply.ProviderReplyPin {
			t.Fatalf("reply role %s = %#v", role, reply)
		}
	}
}

func TestBuildShowsTemplateInstanceRouteKeysAndCarries(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	if view.Root.PrimaryEntity != nil || view.Root.PrimaryEntityError != "" {
		t.Fatalf("root primary entity for valid no-root fixture = entity %#v error %q, want none", view.Root.PrimaryEntity, view.Root.PrimaryEntityError)
	}

	scoring := flowByID(t, view, "scoring")
	if scoring.PrimaryEntity == nil || scoring.PrimaryEntity.Type != "validation" {
		t.Fatalf("scoring primary entity = %#v, want validation", scoring.PrimaryEntity)
	}
	if scoring.TemplateInstance == nil {
		t.Fatalf("scoring template instance missing")
	}
	if got := strings.Join(scoring.TemplateInstance.By, ","); got != "account_id" {
		t.Fatalf("scoring instance.by = %q, want account_id", got)
	}
	if scoring.TemplateInstance.OnMissing != "create" || scoring.TemplateInstance.OnConflict != "reuse" {
		t.Fatalf("scoring instance policy = %#v, want create/reuse", scoring.TemplateInstance)
	}

	producer := flowByID(t, view, "producer")
	output := outputPinByName(t, producer, "validation_requested")
	if output.Key != "account_id" || !containsString(output.Carries, "account_id") {
		t.Fatalf("producer output key/carries = %#v, want account_id carried", output)
	}

	edges := interFlowRouteEdges(view.RoutingTopology)
	if len(edges) != 1 {
		t.Fatalf("inter-flow route edge count = %d, want 1: %#v", len(edges), edges)
	}
	edge := edges[0]
	if edge.Producer.FlowID != "producer" || edge.Boundary == nil || edge.Boundary.OutputPin != "validation_requested" {
		t.Fatalf("route producer/boundary = %#v, want producer.validation_requested", edge)
	}
	if edge.Consumer.FlowID != "scoring" || edge.Boundary.InputPin != "validation_requested" {
		t.Fatalf("route consumer/boundary = %#v, want scoring.validation_requested", edge)
	}
	if edge.Resolution == nil || edge.Resolution.Mode != "instance_key" {
		t.Fatalf("route resolution = %#v, want instance_key", edge.Resolution)
	}
	if edge.Resolution.InstanceKey == nil {
		t.Fatalf("route instance key missing")
	}
	if got := strings.Join(edge.Resolution.InstanceKey.Fields, ","); got != "account_id" {
		t.Fatalf("route instance key fields = %q, want account_id", got)
	}
	if len(edge.Resolution.InstanceKey.Mappings) != 1 || edge.Resolution.InstanceKey.Mappings[0].Source != "account_id" || edge.Resolution.InstanceKey.Mappings[0].Target != "account_id" || edge.Resolution.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route implicit mapping = %#v, want account_id -> account_id explicit=false", edge.Resolution.InstanceKey.Mappings)
	}
}

func TestBuildDiagnosticsPreservesRemediationAndEvidence(t *testing.T) {
	report := runtimebootverify.Report{}
	report.Add(runtimebootverify.Finding{
		CheckID:     "timer_validation",
		Severity:    runtimebootverify.SeverityHardInvalidity,
		Location:    "reminder",
		Message:     "timer reminder start_on boot does not support cancel_on state:done",
		Remediation: "remove cancel_on from boot timer",
		Evidence:    []string{" timer: reminder ", "", "cancel_on: state:done"},
	})

	diagnostics := buildDiagnostics(nil, &report)
	if len(diagnostics) != 1 {
		t.Fatalf("diagnostics = %#v, want one", diagnostics)
	}
	got := diagnostics[0]
	if got.Remediation != "remove cancel_on from boot timer" {
		t.Fatalf("remediation = %q", got.Remediation)
	}
	if len(got.Evidence) != 2 || got.Evidence[0] != "timer: reminder" || got.Evidence[1] != "cancel_on: state:done" {
		t.Fatalf("evidence = %#v", got.Evidence)
	}
}

func TestBuildShowsDefaultedTemplateInstancePolicies(t *testing.T) {
	source := loadDefaultedTemplatePolicySource(t)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	scoring := flowByID(t, view, "scoring")
	if scoring.TemplateInstance == nil {
		t.Fatalf("scoring template instance missing")
	}
	if got := strings.Join(scoring.TemplateInstance.By, ","); got != "account_id" {
		t.Fatalf("scoring instance.by = %q, want account_id", got)
	}
	if scoring.TemplateInstance.OnMissing != "create" || scoring.TemplateInstance.OnConflict != "reject" {
		t.Fatalf("scoring defaulted instance policy = %#v, want create/reject", scoring.TemplateInstance)
	}
	if diagnosticByCheckIDOrNil(view, "template_instance_validation") != nil {
		t.Fatalf("template_instance_validation diagnostic present for defaulted policies: %#v", view.Diagnostics)
	}
}

func TestBuildShowsRootPrimaryEntity(t *testing.T) {
	source := loadRootPrimaryEntitySource(t)
	view := mustBuild(t, source, nil)

	if view.Root.PrimaryEntity == nil {
		t.Fatalf("root primary entity missing: %#v", view.Root)
	}
	if view.Root.PrimaryEntity.Type != "workspace" {
		t.Fatalf("root primary entity type = %q, want workspace", view.Root.PrimaryEntity.Type)
	}
	if view.Root.PrimaryEntity.Fields["org_id"] != "text" {
		t.Fatalf("root primary entity fields = %#v, want org_id text", view.Root.PrimaryEntity.Fields)
	}
	if view.Root.PrimaryEntity.SourceFile == "" || !strings.HasSuffix(view.Root.PrimaryEntity.SourceFile, "entities.yaml") {
		t.Fatalf("root primary entity source file = %q, want entities.yaml", view.Root.PrimaryEntity.SourceFile)
	}
}

func TestBuildShowsRequiredAgentProvenance(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "analysis",
		Paths: runtimecontracts.FlowContractPaths{
			ID:         "analysis",
			SchemaFile: "flows/analysis/schema.yaml",
			AgentsFile: "flows/analysis/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{RequiredAgentsDeclared: true},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {Subscriptions: []string{"analysis.requested"}},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{
			RootSchemaFile:    "schema.yaml",
			ProjectAgentsFile: "agents.yaml",
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"root-agent": {
				Subscriptions: []string{"root.requested"},
				EmitEvents:    []string{"root.done"},
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analysis": flow.Schema,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{flow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analysis": &flow,
			},
		},
	}
	view := mustBuild(t, semanticview.Wrap(bundle), nil)

	if view.Root.RequiredAgents.Source != runtimecontracts.RequiredAgentSourceInferred ||
		len(view.Root.RequiredAgents.Agents) != 1 ||
		view.Root.RequiredAgents.Agents[0].Role != "root-agent" ||
		view.Root.RequiredAgents.Agents[0].Source != runtimecontracts.RequiredAgentSourceInferred {
		t.Fatalf("root required_agents view = %#v, want inferred root-agent", view.Root.RequiredAgents)
	}
	analysis := flowByID(t, view, "analysis")
	if analysis.RequiredAgents.Source != runtimecontracts.RequiredAgentSourceExplicit {
		t.Fatalf("flow required_agents source = %q, want explicit", analysis.RequiredAgents.Source)
	}
	if len(analysis.RequiredAgents.Agents) != 0 {
		t.Fatalf("flow required_agents agents = %#v, want explicit empty boundary", analysis.RequiredAgents.Agents)
	}
}

func TestBuildStageGraphShowsStageTimersAndTimedEdges(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "awaiting_review", Initial: true},
					{ID: "expired", Terminal: true},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Timers: []runtimecontracts.WorkflowTimerContract{
				{
					ID:         "awaiting_review.review.sla_escalated",
					Stage:      "awaiting_review",
					Event:      "review.sla_escalated",
					Owner:      "runtime",
					StageOwned: true,
					Delay:      "48h",
					StartOn:    "state:awaiting_review",
				},
				{
					ID:         "awaiting_review.expired",
					Stage:      "awaiting_review",
					Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
					Owner:      "runtime",
					StageOwned: true,
					AdvancesTo: "expired",
					Delay:      "{{marginal_park_days}}d",
					StartOn:    "state:awaiting_review",
				},
			},
		},
	}
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{"": runtimecontracts.BuildWorkflowStageTopology(
		"", "awaiting_review", []string{"awaiting_review", "expired"}, []string{"expired"}, nil, bundle.Semantics.Timers, nil,
	)}

	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(view.StageGraphs) != 1 {
		t.Fatalf("StageGraphs = %#v, want one root graph", view.StageGraphs)
	}
	graph := view.StageGraphs[0]
	if len(graph.Timers) != 2 {
		t.Fatalf("graph timers = %#v, want both stage timers including emit-only", graph.Timers)
	}
	if graph.Timers[0].TimerID != "awaiting_review.expired" && graph.Timers[1].TimerID != "awaiting_review.expired" {
		t.Fatalf("graph timers = %#v, want advances_to timer visible", graph.Timers)
	}
	var timedEdge StageGraphEdgeView
	for _, edge := range graph.Edges {
		if edge.TimerID == "awaiting_review.expired" {
			timedEdge = edge
			break
		}
	}
	if timedEdge.TimerID == "" {
		t.Fatalf("graph edges = %#v, want timed transition edge", graph.Edges)
	}
	if !timedEdge.Timed || timedEdge.After != "{{marginal_park_days}}d" || timedEdge.To != "expired" {
		t.Fatalf("timed edge = %#v, want after-labeled transition to expired", timedEdge)
	}
}

func TestBuildStageGraphShowsFanOutMultiplicity(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "waiting", Initial: true},
					{ID: "awaiting_line_items"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "waiting",
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"order.accepted": {
						CreateEntity: true,
						FanOut: &runtimecontracts.FanOutSpec{
							ItemsFrom: "payload.line_items",
							As:        "line_item",
							Identity:  "line_item.id",
							Emit:      runtimecontracts.EmitSpec{Event: "line_item.requested"},
						},
						AdvancesTo: "awaiting_line_items",
					},
				},
			},
		},
	}

	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(view.StageGraphs) != 1 {
		t.Fatalf("StageGraphs = %#v, want one root graph", view.StageGraphs)
	}
	graph := view.StageGraphs[0]
	if len(graph.FanOuts) != 1 {
		t.Fatalf("graph fan_outs = %#v, want one fan-out edge", graph.FanOuts)
	}
	got := graph.FanOuts[0]
	if got.Emit != "line_item.requested" || got.ItemsFrom != "payload.line_items" || got.ItemAlias != "line_item" || got.Identity != "line_item.id" {
		t.Fatalf("fan-out view = %#v, want multiplicity metadata", got)
	}
	if len(got.From) != 1 || got.From[0] != "waiting" {
		t.Fatalf("fan-out from = %#v, want initial stage", got.From)
	}
	if got.Source != "handler.fan_out" || got.NodeID != "dispatcher" || got.EventType != "order.accepted" {
		t.Fatalf("fan-out source = %#v, want handler fan_out dispatcher/order.accepted", got)
	}
}

func TestBuildStageGraphShowsJoinCompleteAndTimeoutEdges(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "awaiting", Initial: true}, {ID: "ready"}, {ID: "attention", Terminal: true}}}},
		Semantics:  runtimecontracts.WorkflowSemanticView{InitialStage: "awaiting"},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"join-node": {EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"item.completed": {Join: &runtimecontracts.JoinSpec{
			ID: "line_items", Stage: "awaiting",
			OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "ready"},
			Timeout:    runtimecontracts.JoinTimeoutSpec{After: "24h", Outcome: runtimecontracts.HandlerRuleEntry{AdvancesTo: "attention"}},
		}}}}},
	}
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{"": runtimecontracts.BuildWorkflowStageTopology(
		"", "awaiting", []string{"awaiting", "ready", "attention"}, []string{"attention"},
		[]runtimecontracts.HandlerTransitionSemantic{{ID: "join-node:item.completed", NodeID: "join-node", EventType: "item.completed", Join: bundle.Nodes["join-node"].EventHandlers["item.completed"].Join}}, nil, nil,
	)}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatal(err)
	}
	graph := view.StageGraphs[0]
	var complete, timeout StageGraphEdgeView
	for _, edge := range graph.Edges {
		switch edge.Source {
		case string(runtimecontracts.HandlerAdvanceCarrierJoinOnComplete):
			complete = edge
		case string(runtimecontracts.HandlerAdvanceCarrierJoinTimeout):
			timeout = edge
		}
	}
	if len(complete.From) != 1 || complete.From[0] != "awaiting" || complete.To != "ready" {
		t.Fatalf("complete edge = %#v", complete)
	}
	if len(timeout.From) != 1 || timeout.From[0] != "awaiting" || timeout.To != "attention" || !timeout.Timed || timeout.After != "24h" || timeout.TimerID != "line_items" {
		t.Fatalf("timeout edge = %#v", timeout)
	}
}

func TestBuildStageGraphShowsBoundedLoopBackEdgeAndEscape(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{
			{ID: "queued", Initial: true}, {ID: "drafting"}, {ID: "review"}, {ID: "escalated", Terminal: true},
		}}},
		Semantics: runtimecontracts.WorkflowSemanticView{InitialStage: "queued", Loops: []runtimecontracts.WorkflowLoopPlan{{
			ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3},
			Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escalated"}, EntryStage: "drafting", RegionStages: []string{"drafting", "review"},
			Operations: []runtimecontracts.WorkflowLoopOperationPlan{
				{NodeID: "loop-node", HandlerEvent: "work.started", Kind: runtimecontracts.LoopOperationStart, From: "queued", AdvancesTo: "drafting"},
				{NodeID: "loop-node", HandlerEvent: "review.revision_requested", Kind: runtimecontracts.LoopOperationRepeat, From: "review", AdvancesTo: "drafting"},
			},
		}}},
	}
	loopTransitions := []runtimecontracts.HandlerTransitionSemantic{
		{ID: "loop-node:work.started", NodeID: "loop-node", EventType: "work.started", AdvancesTo: "drafting", Loop: &runtimecontracts.LoopOperationSpec{Start: "revision", From: "queued"}},
		{ID: "loop-node:review.revision_requested", NodeID: "loop-node", EventType: "review.revision_requested", AdvancesTo: "drafting", Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "review"}},
		{ID: "loop-node:draft.ready", NodeID: "loop-node", EventType: "draft.ready", AdvancesTo: "review", Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"}},
	}
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{"": runtimecontracts.BuildWorkflowStageTopology(
		"", "queued", []string{"queued", "drafting", "review", "escalated"}, []string{"escalated"}, loopTransitions, nil, bundle.Semantics.Loops,
	)}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatal(err)
	}
	graph := view.StageGraphs[0]
	var repeat, escape StageGraphEdgeView
	for _, edge := range graph.Edges {
		if edge.LoopID != "revision" || edge.LoopOperation != "repeat" {
			continue
		}
		if edge.LoopEscape {
			escape = edge
		} else {
			repeat = edge
		}
	}
	if repeat.To != "drafting" || len(repeat.From) != 1 || repeat.From[0] != "review" || repeat.MaxAttempts != "3" {
		t.Fatalf("repeat edge = %#v", repeat)
	}
	if escape.To != "escalated" || !escape.LoopEscape || escape.MaxAttempts != "3" {
		t.Fatalf("escape edge = %#v", escape)
	}
}

func TestBuildShowsEffectiveAgentPlatformDefaultProvenance(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "analysis",
		Paths: runtimecontracts.FlowContractPaths{
			ID:         "analysis",
			SchemaFile: "flows/analysis/schema.yaml",
			AgentsFile: "flows/analysis/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{Name: "analysis"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Model:         "regular",
				Subscriptions: []string{"analysis.requested"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Paths: runtimecontracts.ContractPaths{
			RootSchemaFile:    "schema.yaml",
			ProjectAgentsFile: "agents.yaml",
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"root-agent": {
				Model:         "regular",
				Subscriptions: []string{"root.requested"},
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analysis": flow.Schema,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{flow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analysis": &flow,
			},
		},
	}

	view := mustBuild(t, semanticview.Wrap(bundle), nil)

	rootAgent := agentByID(t, view.Root.Agents, "root-agent")
	assertDefaultedAgentField(t, rootAgent, "type", runtimecontracts.DefaultAgentType)
	assertDefaultedAgentField(t, rootAgent, "mode", runtimecontracts.DefaultAgentMode)
	assertDefaultedAgentField(t, rootAgent, "max_turns_per_task", runtimecontracts.DefaultAgentMaxTurnsPerTask)
	assertDefaultedAgentField(t, rootAgent, "workspace_class", "")
	if got := rootAgent.Fields["model"].Source; got != runtimecontracts.AgentFieldSourceAuthored {
		t.Fatalf("root-agent model source = %q, want authored", got)
	}

	analysis := flowByID(t, view, "analysis")
	flowAgent := agentByID(t, analysis.Agents, "analyzer")
	assertDefaultedAgentField(t, flowAgent, "type", runtimecontracts.DefaultAgentType)
	assertDefaultedAgentField(t, flowAgent, "mode", runtimecontracts.DefaultAgentMode)
	assertDefaultedAgentField(t, flowAgent, "max_turns_per_task", runtimecontracts.DefaultAgentMaxTurnsPerTask)
	assertDefaultedAgentField(t, flowAgent, "workspace_class", "")
}

func assertDefaultedAgentField(t testing.TB, agent AgentView, field string, want any) {
	t.Helper()
	got, ok := agent.Fields[field]
	if !ok {
		t.Fatalf("agent %s field %s missing in %#v", agent.ID, field, agent.Fields)
	}
	if got.Value != want {
		t.Fatalf("agent %s field %s value = %#v, want %#v", agent.ID, field, got.Value, want)
	}
	if got.Source != runtimecontracts.AgentFieldSourcePlatformDefault {
		t.Fatalf("agent %s field %s source = %q, want %q", agent.ID, field, got.Source, runtimecontracts.AgentFieldSourcePlatformDefault)
	}
}

func TestBuildShowsRouteIssueAndAuthoredDiagnosticLocation(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{BadConnectMapping: true})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	var issue *routingtopology.Issue
	for i := range view.RoutingTopology.Issues {
		if view.RoutingTopology.Issues[i].Failure == "route_plan_instance_key_adapter_invalid" {
			issue = &view.RoutingTopology.Issues[i]
			break
		}
	}
	if issue == nil {
		t.Fatalf("route issues = %#v, want route_plan_instance_key_adapter_invalid", view.RoutingTopology.Issues)
	}
	if issue.AuthoredLocation == "" || !strings.Contains(issue.AuthoredLocation, "package.yaml:") {
		t.Fatalf("route issue authored location = %q, want exact package.yaml:line", issue.AuthoredLocation)
	}

	diag := diagnosticByCheckID(t, view, "composition_connect_validation")
	if diag.AuthoredLocation == "" {
		t.Fatalf("diagnostic authored location empty: %#v", diag)
	}
	if !strings.Contains(diag.Message, "connect producer.validation_requested -> scoring.validation_requested") {
		t.Fatalf("diagnostic message = %q, want connect context", diag.Message)
	}
}

func TestBuildRoutingTopologyWithReportProjectsOnlyExistingDanglingDiagnostics(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fixture source has no contract bundle")
	}
	report := runtimebootverify.Report{Findings: []runtimebootverify.Finding{
		{CheckID: "event_consumer_exists", Severity: "warning", Location: "orphan.done", Message: "'orphan.done' emitted but nobody subscribes", Remediation: "Declare a consumer."},
		{CheckID: "payload_completeness", Severity: "error", Location: "node", Message: "different policy fact"},
	}}
	topology := BuildRoutingTopologyWithReport(source, bundle, &report)
	if len(topology.Issues) != 1 {
		t.Fatalf("topology issues = %#v, want only route-related dangling diagnostic", topology.Issues)
	}
	issue := topology.Issues[0]
	if issue.CheckID != "event_consumer_exists" || issue.Failure != issue.CheckID || issue.Severity != "warning" || issue.Remediation != "Declare a consumer." {
		t.Fatalf("dangling issue = %#v, want existing typed diagnostic fields", issue)
	}
	if len(issue.ID) != len("issue-")+16 {
		t.Fatalf("dangling issue id = %q, want stable public issue digest", issue.ID)
	}
}

func TestBuildShowsSingletonContainedOperations(t *testing.T) {
	source := singletoncoordinatorpilot.LoadSource(t, singletoncoordinatorpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	flow := flowByID(t, view, singletoncoordinatorpilot.FlowID)
	if flow.SingletonCoordinator == nil {
		t.Fatalf("singleton coordinator view missing")
	}
	if flow.SingletonCoordinator.PrimaryEntity != singletoncoordinatorpilot.EntityType {
		t.Fatalf("singleton primary entity = %q, want %s", flow.SingletonCoordinator.PrimaryEntity, singletoncoordinatorpilot.EntityType)
	}
	if !containsContainedField(flow.SingletonCoordinator.ContainedState, "lead_index", "map") {
		t.Fatalf("singleton contained state = %#v, want lead_index map", flow.SingletonCoordinator.ContainedState)
	}
	if !containsContainedField(flow.SingletonCoordinator.ContainedState, "audit_log", "list") {
		t.Fatalf("singleton contained state = %#v, want audit_log list", flow.SingletonCoordinator.ContainedState)
	}
	if len(flow.ContainedOperations) < 5 {
		t.Fatalf("contained operation count = %d, want at least 5: %#v", len(flow.ContainedOperations), flow.ContainedOperations)
	}
	mapSet := containedOperationByTargetAndOp(t, flow, "entity.lead_index", "set")
	if mapSet.MapKeyType != "text" || mapSet.MapValueType != "LeadScore" || mapSet.SourceFile == "" {
		t.Fatalf("lead_index set view = %#v, want typed map target and source file", mapSet)
	}
	listAppend := containedOperationByTargetAndOp(t, flow, "entity.audit_log", "append")
	if listAppend.ListItemType != "AuditEntry" || listAppend.SourceFile == "" {
		t.Fatalf("audit_log append view = %#v, want typed list target and source file", listAppend)
	}
}

func TestBuildShowsFinalFlowInstanceAuthoringFixture(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	if !view.Equivalence.ProjectionOnly {
		t.Fatalf("equivalence projection_only = false, want true")
	}
	if !containsString(view.Equivalence.CanonicalOwners, "runtime/core/pinrouting.LowerCompositionConnectRoutePlans") {
		t.Fatalf("canonical owners = %#v, want pinrouting owner", view.Equivalence.CanonicalOwners)
	}
	if !containsString(view.Equivalence.CanonicalOwners, "runtime/semanticview.BuildAuthoredEventEndpointCensus") || !containsString(view.Equivalence.CanonicalOwners, "runtime/routingtopology.Build") {
		t.Fatalf("canonical owners = %#v, want endpoint census and routing topology owners", view.Equivalence.CanonicalOwners)
	}

	account := flowByID(t, view, finalflowinstanceauthoring.TemplateFlowID)
	if account.PrimaryEntity == nil || account.PrimaryEntity.Type != finalflowinstanceauthoring.TemplateEntityType {
		t.Fatalf("account primary entity = %#v, want %s", account.PrimaryEntity, finalflowinstanceauthoring.TemplateEntityType)
	}
	if account.TemplateInstance == nil || strings.Join(account.TemplateInstance.By, ",") != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("account template instance = %#v, want %s", account.TemplateInstance, finalflowinstanceauthoring.TemplateInstanceBy)
	}

	producer := flowByID(t, view, finalflowinstanceauthoring.ProducerFlowID)
	output := outputPinByName(t, producer, finalflowinstanceauthoring.ProducerOutputPin)
	if output.Key != finalflowinstanceauthoring.TemplatePayloadKey || !containsString(output.Carries, finalflowinstanceauthoring.TemplatePayloadKey) {
		t.Fatalf("producer output = %#v, want %s key/carry", output, finalflowinstanceauthoring.TemplatePayloadKey)
	}
	edges := interFlowRouteEdges(view.RoutingTopology)
	if len(edges) != 1 {
		t.Fatalf("inter-flow route edge count = %d, want 1: %#v", len(edges), edges)
	}
	edge := edges[0]
	if edge.Producer.FlowID != finalflowinstanceauthoring.ProducerFlowID || edge.Consumer.FlowID != finalflowinstanceauthoring.TemplateFlowID {
		t.Fatalf("route endpoints = %#v -> %#v, want final fixture producer to template", edge.Producer, edge.Consumer)
	}
	if edge.Resolution == nil || edge.Resolution.InstanceKey == nil || len(edge.Resolution.InstanceKey.Mappings) != 1 ||
		edge.Resolution.InstanceKey.Mappings[0].Source != finalflowinstanceauthoring.TemplatePayloadKey ||
		edge.Resolution.InstanceKey.Mappings[0].Target != finalflowinstanceauthoring.TemplateInstanceBy ||
		!edge.Resolution.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("route instance key = %#v, want explicit renamed mapping", edge.Resolution)
	}

	coordinator := flowByID(t, view, finalflowinstanceauthoring.CoordinatorFlowID)
	if coordinator.SingletonCoordinator == nil || coordinator.SingletonCoordinator.PrimaryEntity != finalflowinstanceauthoring.CoordinatorEntityType {
		t.Fatalf("coordinator singleton view = %#v, want primary %s", coordinator.SingletonCoordinator, finalflowinstanceauthoring.CoordinatorEntityType)
	}
	if !containsContainedField(coordinator.SingletonCoordinator.ContainedState, "lead_index", "map") ||
		!containsContainedField(coordinator.SingletonCoordinator.ContainedState, "audit_log", "list") {
		t.Fatalf("coordinator contained state = %#v, want lead_index map and audit_log list", coordinator.SingletonCoordinator.ContainedState)
	}
	mapSet := containedOperationByTargetAndOp(t, coordinator, "entity.lead_index", "set")
	if mapSet.MapKeyType != "text" || mapSet.MapValueType != "LeadScore" {
		t.Fatalf("lead_index set view = %#v, want typed map target", mapSet)
	}
	listAppend := containedOperationByTargetAndOp(t, coordinator, "entity.audit_log", "append")
	if listAppend.ListItemType != "AuditEntry" {
		t.Fatalf("audit_log append view = %#v, want typed list target", listAppend)
	}
}

func TestBuildScansFlowLocalDuplicateNodeIDsForContainedOperations(t *testing.T) {
	source := loadDuplicateNodeIDContainedOpsSource(t)
	view := mustBuild(t, source, nil)

	for _, flowID := range []string{"alpha", "beta"} {
		flow := flowByID(t, view, flowID)
		op := containedOperationByTargetAndOp(t, flow, "entity.items", "set")
		if op.NodeID != "indexer" {
			t.Fatalf("%s contained operation node = %q, want indexer", flowID, op.NodeID)
		}
		if op.MapKeyType != "text" || op.MapValueType != "Item" {
			t.Fatalf("%s contained operation = %#v, want typed map target", flowID, op)
		}
		if op.SourceFile == "" || !strings.HasSuffix(op.SourceFile, filepath.Join("flows", flowID, "nodes.yaml")) {
			t.Fatalf("%s source file = %q, want flow-local nodes.yaml", flowID, op.SourceFile)
		}
	}
}

func mustBuild(t testing.TB, source semanticview.Source, report *runtimebootverify.Report) View {
	t.Helper()
	view, err := Build(context.Background(), source, BuildOptions{BootReport: report})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return view
}

func flowByID(t testing.TB, view View, id string) FlowView {
	t.Helper()
	for _, flow := range view.Flows {
		if flow.ID == id {
			return flow
		}
	}
	t.Fatalf("flow %q not found in %#v", id, view.Flows)
	return FlowView{}
}

func outputPinByName(t testing.TB, flow FlowView, name string) OutputPinView {
	t.Helper()
	for _, pin := range flow.OutputPins {
		if pin.Name == name {
			return pin
		}
	}
	t.Fatalf("output pin %q not found in %#v", name, flow.OutputPins)
	return OutputPinView{}
}

func agentByID(t testing.TB, agents []AgentView, id string) AgentView {
	t.Helper()
	for _, agent := range agents {
		if agent.ID == id {
			return agent
		}
	}
	t.Fatalf("agent %q not found in %#v", id, agents)
	return AgentView{}
}

func diagnosticByCheckID(t testing.TB, view View, checkID string) DiagnosticView {
	t.Helper()
	if diagnostic := diagnosticByCheckIDOrNil(view, checkID); diagnostic != nil {
		return *diagnostic
	}
	t.Fatalf("diagnostic %q not found in %#v", checkID, view.Diagnostics)
	return DiagnosticView{}
}

func diagnosticByCheckIDOrNil(view View, checkID string) *DiagnosticView {
	for _, diagnostic := range view.Diagnostics {
		if diagnostic.CheckID == checkID {
			diagnostic := diagnostic
			return &diagnostic
		}
	}
	return nil
}

func containedOperationByTargetAndOp(t testing.TB, flow FlowView, target, op string) ContainedOperationView {
	t.Helper()
	for _, operation := range flow.ContainedOperations {
		if operation.Target == target && operation.Operation == op {
			return operation
		}
	}
	t.Fatalf("contained operation %s %s not found in %#v", op, target, flow.ContainedOperations)
	return ContainedOperationView{}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func interFlowRouteEdges(topology routingtopology.Topology) []routingtopology.Edge {
	out := make([]routingtopology.Edge, 0)
	for _, edge := range topology.Edges {
		if edge.Scope == routingtopology.DeliveryScopeInterFlowConnect {
			out = append(out, edge)
		}
	}
	return out
}

func loadRootPrimaryEntitySource(t testing.TB) semanticview.Source {
	t.Helper()
	root := writeRootPrimaryEntityContracts(t)
	repo := authoringViewRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadDefaultedTemplatePolicySource(t testing.TB) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "package.yaml"), `
name: defaulted-template-policy
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: template
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: defaulted-template-policy\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
mode: template
instance:
  by: account_id
pins:
  inputs:
    events: []
  outputs:
    events: []
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "tools.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "agents.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
account:
  account_id: uuid
`)
	repo := authoringViewRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeRootPrimaryEntityContracts(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "package.yaml"), `
name: root-primary-entity
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: root-primary-entity\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "entities.yaml"), `
workspace:
  org_id: text
  region: text
`)
	return root
}

func loadDuplicateNodeIDContainedOpsSource(t testing.TB) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "package.yaml"), `
name: duplicate-node-contained-ops
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: alpha
    flow: alpha
    mode: singleton
  - id: beta
    flow: beta
    mode: singleton
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: duplicate-node-contained-ops\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeDuplicateNodeIDFlow(t, root, "alpha")
	writeDuplicateNodeIDFlow(t, root, "beta")

	repo := authoringViewRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeDuplicateNodeIDFlow(t testing.TB, root, flowID string) {
	t.Helper()
	dir := filepath.Join(root, "flows", flowID)
	writeAuthoringViewTestFile(t, filepath.Join(dir, "schema.yaml"), `
name: `+flowID+`
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: observed
        event: observed
  outputs:
    events: []
`)
	writeAuthoringViewTestFile(t, filepath.Join(dir, "policy.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(dir, "tools.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(dir, "agents.yaml"), "{}\n")
	writeAuthoringViewTestFile(t, filepath.Join(dir, "types.yaml"), `
types:
  Item:
    name: text
`)
	writeAuthoringViewTestFile(t, filepath.Join(dir, "entities.yaml"), `
state:
  id: text
  items: map[text]Item
`)
	writeAuthoringViewTestFile(t, filepath.Join(dir, "events.yaml"), `
observed:
  id: text
  item_id: text
  item: Item
`)
	writeAuthoringViewTestFile(t, filepath.Join(dir, "nodes.yaml"), `
indexer:
  id: indexer
  execution_type: system_node
  subscribes_to: [observed]
  event_handlers:
    observed:
      select_entity:
        by:
          id: payload.id
      data_accumulation:
        writes:
          - source_field: id
            target_field: id
          - op: set
            target: entity.items
            key:
              ref: payload.item_id
            value:
              name: payload.item.name
`)
}

func writeAuthoringViewTestFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func authoringViewRepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func containsContainedField(fields []SingletonContainedFieldView, name, kind string) bool {
	for _, field := range fields {
		if field.Name == name && field.Kind == kind {
			return true
		}
	}
	return false
}
