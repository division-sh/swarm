package authoringview

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/flowownedprojectagent"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatereply"
)

func TestBuildShowsReplyPairedTopology(t *testing.T) {
	source := templatereply.LoadSource(t, templatereply.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)
	edges := interFlowRouteEdges(view.RoutingTopology)
	roles := map[string]*routingtopology.Reply{}
	for _, edge := range edges {
		if edge.Resolution == nil || edge.Resolution.Reply == nil {
			continue
		}
		roles[edge.Resolution.Reply.Role] = edge.Resolution.Reply
	}
	if len(roles) != 2 {
		t.Fatalf("reply route edges = %#v, want request and response roles", edges)
	}
	for _, role := range []string{"request", "response"} {
		reply := roles[role]
		if reply == nil || reply.RequestOutputPin != templatereply.RequesterRequestPin || reply.ReplyInputPin != templatereply.RequesterReplyPin || reply.ProviderInputPin != templatereply.ProviderRequestPin || reply.ProviderOutputPin != templatereply.ProviderReplyPin {
			t.Fatalf("reply role %s = %#v", role, reply)
		}
	}
}

func TestBuildIncludesHarnessInputSourceAndOutputSink(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	worker := flowByID(t, view, "worker")
	if len(worker.InputPins) != 1 || worker.InputPins[0].Source != "harness" {
		t.Fatalf("worker input pins = %#v, want effective source harness", worker.InputPins)
	}
	if got := outputPinByName(t, worker, "work.completed").Sink; got != "harness" {
		t.Fatalf("worker output sink = %q, want harness", got)
	}
}

func TestBuildShowsApprovedOutwardEffectAsCanonicalApprovalPoint(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{
		ID: "send_support_reply", Tool: "telegram_send",
		Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "support_reply"},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"support": {
				ID:            "support",
				ExecutionType: runtimecontracts.SystemNodeExecutionType,
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"support.reply_drafted": handler},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"support": {"support.reply_drafted": handler},
			},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"support": {ID: "support", ExecutionType: runtimecontracts.SystemNodeExecutionType},
			},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"telegram_send": runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
				runtimecontracts.WithToolEffect(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				runtimecontracts.WithToolSchemas(
					runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
					runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
				),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://telegram.example/send"}),
			),
		},
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(view.ApprovalPoints) != 1 {
		t.Fatalf("approval points = %#v, want one", view.ApprovalPoints)
	}
	got := view.ApprovalPoints[0]
	if got.NodeID != identitytest.RootNode(t, "support").Key() || got.HandlerEvent != "support.reply_drafted" || got.Source != "handler.activity" ||
		got.ActivityID != "send_support_reply" || got.Tool != "telegram_send" || got.Decision != "support_reply" ||
		got.EffectClass != string(runtimecontracts.ActivityEffectClassNonIdempotentWrite) {
		t.Fatalf("approval point = %#v", got)
	}
}

func TestBuildStageGraphShowsFanInBarrierEffectiveJoinProvenance(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical fan-in barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)
	view, err := Build(context.Background(), source, BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var joins []StageGraphJoinView
	for _, graph := range view.StageGraphs {
		if graph.FlowID == "portfolio" {
			joins = graph.Joins
			break
		}
	}
	if len(joins) != 1 {
		t.Fatalf("portfolio joins = %#v, want one", joins)
	}
	join := joins[0]
	if join.MembersBy != "payload.operating_id" || join.MembersBySource != "resolution.dedup_by" ||
		join.WindowBy != "payload.period_id" || join.WindowBySource != "resolution.window" || join.FanInPin != "operating.reported" {
		t.Fatalf("effective join readback = %#v", join)
	}
}

func TestBuildStageGraphShowsSameFlowPackageQualifiedJoins(t *testing.T) {
	flow := runtimecontracts.FlowContractView{Path: "orders", Paths: runtimecontracts.FlowContractPaths{ID: "orders", PackageKey: "root/flows/orders"}}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	first, err := runtimeidentity.AdmitExecutableNodeDeclaration("packages/a", "orders", "shared")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtimeidentity.AdmitExecutableNodeDeclaration("packages/b", "orders", "shared")
	if err != nil {
		t.Fatal(err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"orders": &root.Children[0]}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowStates: map[string][]string{"orders": {"awaiting", "done"}},
			Joins: []runtimecontracts.WorkflowJoinPlan{
				{Node: second, HandlerEvent: "item.completed", Spec: runtimecontracts.JoinSpec{ID: "second", Stage: "awaiting"}},
				{Node: first, HandlerEvent: "item.completed", Spec: runtimecontracts.JoinSpec{ID: "first", Stage: "awaiting"}},
			},
		},
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(view.StageGraphs) != 1 || len(view.StageGraphs[0].Joins) != 2 {
		t.Fatalf("stage graphs = %#v, want both package-qualified joins", view.StageGraphs)
	}
	joins := view.StageGraphs[0].Joins
	if joins[0].PackageKey != "packages/a" || joins[0].NodeID != "shared" || joins[1].PackageKey != "packages/b" || joins[1].NodeID != "shared" {
		t.Fatalf("package-qualified join readback = %#v", joins)
	}
}

func TestBuildShowsTemplateInstanceRouteIdentityAndProjection(t *testing.T) {
	source := templateflowpilot.LoadSource(t, templateflowpilot.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	if view.Root.PrimaryEntity != nil || view.Root.PrimaryEntityError != "" {
		t.Fatalf("root primary entity for valid no-root fixture = entity %#v error %q, want none", view.Root.PrimaryEntity, view.Root.PrimaryEntityError)
	}

	account := flowByID(t, view, "account")
	if account.PrimaryEntity == nil || account.PrimaryEntity.Type != "account_state" {
		t.Fatalf("account primary entity = %#v, want account_state", account.PrimaryEntity)
	}
	if account.TemplateInstance == nil {
		t.Fatalf("account template instance missing")
	}
	if got := account.TemplateInstance.Field; got != "account_id" {
		t.Fatalf("account instance field = %q, want account_id", got)
	}

	producer := flowByID(t, view, "producer")
	output := outputPinByName(t, producer, "account.ready")
	if output.Event != "account.ready" || output.PinDigest == "" {
		t.Fatalf("producer output = %#v, want canonical immutable account.ready pin", output)
	}

	edges := interFlowRouteEdges(view.RoutingTopology)
	if len(edges) != 1 {
		t.Fatalf("inter-flow route edge count = %d, want 1: %#v", len(edges), edges)
	}
	edge := edges[0]
	if edge.Producer.FlowID != "producer" || edge.Boundary == nil || edge.Boundary.OutputPin != "account.ready" {
		t.Fatalf("route producer/boundary = %#v, want producer.account.ready", edge)
	}
	if edge.Consumer.FlowID != "account" || edge.Boundary.InputPin != "account.ready" {
		t.Fatalf("route consumer/boundary = %#v, want account.account.ready", edge)
	}
	if edge.Resolution == nil || edge.Resolution.Mode != "select-or-create" {
		t.Fatalf("route resolution = %#v, want select-or-create", edge.Resolution)
	}
	if edge.Resolution.InstanceKey == nil {
		t.Fatalf("route instance key missing")
	}
	if got := edge.Resolution.InstanceKey.Field; got != "account_id" {
		t.Fatalf("route instance key field = %q, want account_id", got)
	}
	if edge.Resolution.InstanceKey.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) || edge.Resolution.InstanceKey.SourcePath != "payload.account_id" || edge.Resolution.InstanceKey.DerivedFrom == "" {
		t.Fatalf("route derived source = %#v, want payload.account_id with provenance", edge.Resolution.InstanceKey)
	}
}

func TestBuildShowsDistinctProducerAndReceiverSchemasForIntrinsicProjection(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateCreateMintedKey),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load generated-key artifact: %v", err)
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	validator := flowByID(t, view, "validator")
	if len(validator.InputPins) != 1 {
		t.Fatalf("validator input pins = %#v, want one", validator.InputPins)
	}
	pin := validator.InputPins[0]
	if pin.ProducerSchemaDigest == "" || pin.ReceiverSchemaDigest == "" || pin.ProducerSchemaDigest == pin.ReceiverSchemaDigest {
		t.Fatalf("validator schema roles = producer:%q receiver:%q, want distinct immutable digests", pin.ProducerSchemaDigest, pin.ReceiverSchemaDigest)
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

func TestBuildShowsScalarTemplateInstanceWithoutPolicyFacts(t *testing.T) {
	source := loadDefaultedTemplatePolicySource(t)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	scoring := flowByID(t, view, "scoring")
	if scoring.TemplateInstance == nil {
		t.Fatalf("scoring template instance missing")
	}
	if got := scoring.TemplateInstance.Field; got != "account_id" {
		t.Fatalf("scoring instance field = %q, want account_id", got)
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
	view := mustBuild(t, loadAuthoringAgentProvenanceSource(t, true), nil)

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

func TestBuildProjectsFlowOwnedProjectAgentAndInferredProvenance(t *testing.T) {
	source := flowownedprojectagent.LoadSource(t, runtimecontracts.FlowModeTemplate, false)
	view := mustBuild(t, source, nil)
	support := flowByID(t, view, "support")
	if len(support.Agents) != 1 || support.Agents[0].ID != "public-worker-left" || !strings.HasSuffix(filepath.ToSlash(support.Agents[0].SourceFile), "flows/support/left/agents.yaml") {
		t.Fatalf("support agents = %#v, want exact project declaration and source", support.Agents)
	}
	if len(support.RequiredAgents.Agents) != 1 || support.RequiredAgents.Agents[0].Role != "worker" || !strings.HasSuffix(filepath.ToSlash(support.RequiredAgents.Agents[0].SourceFile), "flows/support/left/agents.yaml") {
		t.Fatalf("support required agents = %#v, want inferred project declaration provenance", support.RequiredAgents)
	}
	if len(view.Root.Agents) != 0 {
		t.Fatalf("root agents = %#v, project declaration must remain flow-owned", view.Root.Agents)
	}
}

func TestBuildPreservesDistinctFlowOwnedProjectDeclarations(t *testing.T) {
	source := flowownedprojectagent.LoadSource(t, runtimecontracts.FlowModeTemplate, true)
	view := mustBuild(t, source, nil)
	support := flowByID(t, view, "support")
	if len(support.Agents) != 2 || support.Agents[0].ID != "public-worker-left" || support.Agents[1].ID != "public-worker-right" || support.Agents[0].SourceFile == support.Agents[1].SourceFile {
		t.Fatalf("support agents = %#v, want both distinct physical declarations", support.Agents)
	}
	if len(support.RequiredAgents.Agents) != 2 || support.RequiredAgents.Agents[0].SourceFile == support.RequiredAgents.Agents[1].SourceFile {
		t.Fatalf("support required agents = %#v, want both declaration sources", support.RequiredAgents)
	}
	if support.RequiredAgents.SourceFile != "" {
		t.Fatalf("aggregate required-agent source = %q, want no lossy singular source for multiple files", support.RequiredAgents.SourceFile)
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
	if timedEdge.HandlerEvent != "" {
		t.Fatalf("timed edge handler origin = %q, want empty", timedEdge.HandlerEvent)
	}
}

func TestBuildStageGraphShowsDecisionGateOutcomes(t *testing.T) {
	gates := []runtimecontracts.WorkflowGatePlan{{
		Stage: "awaiting_launch_approval", Decision: "launch_review", Title: "Launch review",
		Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
			"approve": {Verdict: "approve", Label: "Approve", AdvancesTo: "operating", Emit: runtimecontracts.EmitSpec{Event: "opco.launched"}},
			"reject":  {Verdict: "reject", Label: "Reject", AdvancesTo: "building", Emit: runtimecontracts.EmitSpec{Event: "launch.rejected"}},
		},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{
			{ID: "awaiting_launch_approval", Initial: true}, {ID: "operating", Terminal: true}, {ID: "building"},
		}}},
		Semantics: runtimecontracts.WorkflowSemanticView{InitialStage: "awaiting_launch_approval", Gates: gates},
	}
	bundle.Semantics.StageTopologies = map[string]runtimecontracts.WorkflowStageTopology{"": runtimecontracts.BuildWorkflowStageTopology(
		"", "awaiting_launch_approval", []string{"awaiting_launch_approval", "operating", "building"}, []string{"operating"}, nil, nil, nil, gates,
	)}

	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatal(err)
	}
	graph := view.StageGraphs[0]
	if len(graph.Gates) != 1 {
		t.Fatalf("graph gates = %#v, want launch_review", graph.Gates)
	}
	gate := graph.Gates[0]
	if gate.Stage != "awaiting_launch_approval" || gate.Decision != "launch_review" || gate.Authority != "stage_activation_cas" || gate.ReminderInterval != "24h" || gate.InputDraftTTL != "15m" {
		t.Fatalf("gate = %#v, want effective authority and cadence", gate)
	}
	if len(gate.Outcomes) != 2 || gate.Outcomes[0].Verdict != "approve" || gate.Outcomes[0].AdvancesTo != "operating" || gate.Outcomes[0].Emit != "opco.launched" || gate.Outcomes[1].Verdict != "reject" || gate.Outcomes[1].AdvancesTo != "building" || gate.Outcomes[1].Emit != "launch.rejected" {
		t.Fatalf("gate outcomes = %#v, want sorted authored outcomes", gate.Outcomes)
	}
	edges := map[string]StageGraphEdgeView{}
	for _, edge := range graph.Edges {
		if edge.Source == "gate" {
			edges[edge.Verdict] = edge
		}
	}
	if edges["approve"].To != "operating" || edges["approve"].DecisionID != "launch_review" || edges["reject"].To != "building" || edges["reject"].DecisionID != "launch_review" {
		t.Fatalf("gate edges = %#v, want one labeled edge per verdict", edges)
	}
}

func TestBuildStageGraphShowsFanOutMultiplicity(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"order.accepted": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"line_items": {Type: "[LineItem]"},
				}},
			},
		},
		RootTypes: runtimecontracts.TypeCatalogDocument{Types: map[string]runtimecontracts.NamedTypeDecl{
			"LineItem": {Fields: map[string]runtimecontracts.TypeFieldSpec{"id": {Type: "text"}}},
		}},
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
							ElementID: contractelementidentity.MintContractElementID(),
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
	effective, err := bundle.ResolveFanOutEffectiveSemantics(identitytest.RootNode(t, "dispatcher"), "order.accepted", *bundle.Nodes["dispatcher"].EventHandlers["order.accepted"].FanOut)
	if err != nil {
		t.Fatalf("ResolveFanOutEffectiveSemantics: %v", err)
	}
	if effective.Identity != "line_item.id" {
		t.Fatalf("effective fan-out identity = %q, want line_item.id", effective.Identity)
	}
	root := t.TempDir()
	packageFile := filepath.Join(root, "package.yaml")
	platformFile := filepath.Join(root, "platform-spec.yaml")
	if err := os.WriteFile(packageFile, []byte("name: authoring-fan-out-test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(platformFile, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle.Paths = runtimecontracts.ContractPaths{ContractsRoot: root, ProjectPackageFile: packageFile, PlatformSpecFile: platformFile}

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
	if got.Emit != "line_item.requested" || got.ItemsFrom != "payload.line_items" || got.ItemAlias != "line_item" || got.Identity != "line_item.id" || got.MaxItems != runtimecontracts.DefaultFanOutMaxItems {
		t.Fatalf("fan-out view = %#v, want multiplicity metadata", got)
	}
	if len(got.From) != 1 || got.From[0] != "waiting" {
		t.Fatalf("fan-out from = %#v, want initial stage", got.From)
	}
	if got.Source != "handler.fan_out" || got.NodeID != identitytest.RootNode(t, "dispatcher").Key() || got.EventType != "order.accepted" {
		t.Fatalf("fan-out source = %#v, want handler fan_out dispatcher/order.accepted", got)
	}
}

func TestBuildStageGraphShowsJoinCompleteAndTimeoutEdges(t *testing.T) {
	joinNode := identitytest.RootNode(t, "join-node")
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
		[]runtimecontracts.HandlerTransitionSemantic{{ID: "join-node:item.completed", Node: joinNode, EventType: "item.completed", Join: bundle.Nodes["join-node"].EventHandlers["item.completed"].Join}}, nil, nil,
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
	if timeout.HandlerEvent != "item.completed" || timeout.EventType != "platform.join_timeout" {
		t.Fatalf("timeout provenance = %#v", timeout)
	}
}

func TestBuildStageGraphShowsDeliveryJoinCompletionFromHandlerScope(t *testing.T) {
	elementID, err := contractelementidentity.ParseContractElementID("cf377b4f-e952-4ddb-9ecc-a1f380af032d")
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.RootNode(t, "dispatcher")
	join := runtimecontracts.JoinSpec{
		ID: "all-delivered", Members: runtimecontracts.JoinMembersSpec{FromFanOut: elementID},
		OnComplete: runtimecontracts.HandlerRuleEntry{AdvancesTo: "done"}, OnCompleteFound: true,
	}
	topology := runtimecontracts.BuildWorkflowStageTopology(
		"", "active", []string{"active", "done"}, []string{"done"},
		[]runtimecontracts.HandlerTransitionSemantic{{Node: node, EventType: "batch.requested", Join: &join}}, nil, nil,
	)
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{{ID: "active", Initial: true}, {ID: "done", Terminal: true}}}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			InitialStage: "active", Stages: []runtimecontracts.WorkflowStageContract{{ID: "active"}, {ID: "done"}}, TerminalStages: []string{"done"},
			StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"": topology},
		},
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(view.StageGraphs) != 1 || view.StageGraphs[0].FlowID != "root" {
		t.Fatalf("stage graphs = %#v, want root graph", view.StageGraphs)
	}
	var completion StageGraphEdgeView
	for _, edge := range view.StageGraphs[0].Edges {
		if edge.Source == string(runtimecontracts.HandlerAdvanceCarrierJoinOnComplete) {
			completion = edge
		}
	}
	if len(completion.From) != 1 || completion.From[0] != "active" || completion.To != "done" || completion.NodeID != node.Key() {
		t.Fatalf("delivery join authoring edge = %#v", completion)
	}
}

func TestBuildStageGraphShowsBoundedLoopBackEdgeAndEscape(t *testing.T) {
	loopNode := identitytest.RootNode(t, "loop-node")
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{StageDeclarations: runtimecontracts.FlowStageDeclarations{Declared: true, Entries: []runtimecontracts.FlowStageDeclaration{
			{ID: "queued", Initial: true}, {ID: "drafting"}, {ID: "review"}, {ID: "escalated", Terminal: true},
		}}},
		Semantics: runtimecontracts.WorkflowSemanticView{InitialStage: "queued", Loops: []runtimecontracts.WorkflowLoopPlan{{
			ID: "revision", RevisionField: "revision_id", MaxAttempts: runtimecontracts.LoopAttemptLimit{Literal: 3},
			Escape: runtimecontracts.LoopEscapeSpec{AdvancesTo: "escalated"}, EntryStage: "drafting", RegionStages: []string{"drafting", "review"},
			Operations: []runtimecontracts.WorkflowLoopOperationPlan{
				{Node: loopNode, HandlerEvent: "work.started", Kind: runtimecontracts.LoopOperationStart, From: "queued", AdvancesTo: "drafting"},
				{Node: loopNode, HandlerEvent: "review.revision_requested", Kind: runtimecontracts.LoopOperationRepeat, From: "review", AdvancesTo: "drafting"},
			},
		}}},
	}
	loopTransitions := []runtimecontracts.HandlerTransitionSemantic{
		{ID: "loop-node:work.started", Node: loopNode, EventType: "work.started", AdvancesTo: "drafting", Loop: &runtimecontracts.LoopOperationSpec{Start: "revision", From: "queued"}},
		{ID: "loop-node:review.revision_requested", Node: loopNode, EventType: "review.revision_requested", AdvancesTo: "drafting", Loop: &runtimecontracts.LoopOperationSpec{Repeat: "revision", From: "review"}},
		{ID: "loop-node:draft.ready", Node: loopNode, EventType: "draft.ready", AdvancesTo: "review", Loop: &runtimecontracts.LoopOperationSpec{Admit: "revision", From: "drafting"}},
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
	view := mustBuild(t, loadAuthoringAgentProvenanceSource(t, false), nil)

	rootAgent := agentByID(t, view.Root.Agents, "root-agent")
	assertDefaultedAgentField(t, rootAgent, "type", runtimecontracts.DefaultAgentType)
	assertDefaultedAgentField(t, rootAgent, "memory", false)
	assertDefaultedAgentField(t, rootAgent, "memory_source", agentmemory.SourcePlatformDefault)
	assertDefaultedAgentField(t, rootAgent, "max_turns_per_task", runtimecontracts.DefaultAgentMaxTurnsPerTask)
	assertDefaultedAgentField(t, rootAgent, "workspace_class", "")
	if got := rootAgent.Fields["model"].Source; got != runtimecontracts.AgentFieldSourceAuthored {
		t.Fatalf("root-agent model source = %q, want authored", got)
	}

	analysis := flowByID(t, view, "analysis")
	flowAgent := agentByID(t, analysis.Agents, "analyzer")
	assertDefaultedAgentField(t, flowAgent, "type", runtimecontracts.DefaultAgentType)
	assertDefaultedAgentField(t, flowAgent, "memory", false)
	assertDefaultedAgentField(t, flowAgent, "memory_source", agentmemory.SourcePlatformDefault)
	assertDefaultedAgentField(t, flowAgent, "max_turns_per_task", runtimecontracts.DefaultAgentMaxTurnsPerTask)
	assertDefaultedAgentField(t, flowAgent, "workspace_class", "")
}

func TestBuildRendersEffectivePublicAgentNameInsteadOfLocalCoordinate(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"local-worker": {ID: "public-worker", Model: "regular"},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"local-worker": {Kind: "agent", LocalID: "local-worker", Full: "swarm-test://local-worker"},
			},
		},
	}
	view := mustBuild(t, semanticviewtest.WrapRootAgents(bundle), nil)
	if len(view.Root.Agents) != 1 || view.Root.Agents[0].ID != "public-worker" {
		t.Fatalf("root agents = %#v, want effective public-worker readback", view.Root.Agents)
	}
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
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyCompositionConnect(t, canonicalrouting.CompositionConnectMissingReceiverPin)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load canonical malformed connect: %v", err)
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	var issue *routingtopology.Issue
	for i := range view.RoutingTopology.Issues {
		if view.RoutingTopology.Issues[i].Failure == "receiver_input_pin_missing" {
			issue = &view.RoutingTopology.Issues[i]
			break
		}
	}
	if issue == nil {
		t.Fatalf("route issues = %#v, want receiver_input_pin_missing", view.RoutingTopology.Issues)
	}
	if issue.AuthoredLocation == "" || !strings.Contains(issue.AuthoredLocation, "package.yaml:") {
		t.Fatalf("route issue authored location = %q, want exact package.yaml:line", issue.AuthoredLocation)
	}

	diag := diagnosticByCheckID(t, view, "composition_connect_validation")
	if diag.AuthoredLocation == "" {
		t.Fatalf("diagnostic authored location empty: %#v", diag)
	}
	if !strings.Contains(diag.Message, "connect event deploy.done from producer to consumer rename missing.event") {
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

func TestBuildShowsStatelessSingletonWithoutCoordinatorError(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load stateless standing singleton: %v", err)
	}
	source := semanticview.Wrap(bundle)
	view := mustBuild(t, source, nil)
	flow := flowByID(t, view, "telegram-ingress")
	if flow.Mode != runtimecontracts.FlowModeSingleton || flow.PrimaryEntity == nil {
		t.Fatalf("stateless singleton view = %#v", flow)
	}
	if flow.SingletonCoordinator != nil || flow.SingletonError != "" {
		t.Fatalf("stateless singleton coordinator projection = coordinator:%#v error:%q", flow.SingletonCoordinator, flow.SingletonError)
	}
	if _, exists := flow.PrimaryEntity.Fields["active_chats"]; exists {
		t.Fatalf("stateless singleton still exposes removed active_chats ceremony: %#v", flow.PrimaryEntity.Fields)
	}
}

func TestBuildShowsIntrinsicJoinCoordinatorFailureWithExactProvenance(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopySingletonCoordinatorPilot(t, canonicalrouting.SingletonCoordinatorPilotStatelessPayloadJoin)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load stateless join singleton: %v", err)
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)
	flow := flowByID(t, view, "coordinator")
	if flow.SingletonCoordinator != nil || flow.SingletonError == "" {
		t.Fatalf("stateless join readback = coordinator:%#v error:%q", flow.SingletonCoordinator, flow.SingletonError)
	}
	for _, diagnostic := range view.Diagnostics {
		if diagnostic.CheckID == "singleton_coordinator_validation" && diagnostic.Location == identitytest.FlowNode(t, "coordinator", "coordinator-node").Key() && strings.Contains(diagnostic.Message, "handler job.received workflow_join") {
			return
		}
	}
	t.Fatalf("singleton join diagnostics = %#v, want exact node/handler workflow_join provenance", view.Diagnostics)
}

func TestBuildDoesNotInferCoordinatorFromUnusedContainedField(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	entitiesPath := filepath.Join(root, "flows", "telegram-ingress", "entities.yaml")
	entities := []byte("telegram_service:\n  unused_index:\n    type: map[text]json\n    initial: {}\n")
	if err := os.WriteFile(entitiesPath, entities, 0o644); err != nil {
		t.Fatalf("write standing singleton entities: %v", err)
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load standing singleton with unused map: %v", err)
	}
	view := mustBuild(t, semanticview.Wrap(bundle), nil)
	flow := flowByID(t, view, "telegram-ingress")
	if flow.SingletonCoordinator != nil || flow.SingletonError != "" {
		t.Fatalf("unused contained field inferred coordinator semantics = coordinator:%#v error:%q", flow.SingletonCoordinator, flow.SingletonError)
	}
	if _, exists := flow.PrimaryEntity.Fields["unused_index"]; !exists {
		t.Fatalf("primary entity omitted unused contained field: %#v", flow.PrimaryEntity.Fields)
	}
}

func TestBuildShowsFinalFlowInstanceAuthoringFixture(t *testing.T) {
	source := finalflowinstanceauthoring.LoadSource(t, finalflowinstanceauthoring.Options{})
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	view := mustBuild(t, source, &report)

	if !view.Equivalence.ProjectionOnly {
		t.Fatalf("equivalence projection_only = false, want true")
	}
	if !containsString(view.Equivalence.CanonicalOwners, "runtime/core/pinrouting.CompileConnectGraph") {
		t.Fatalf("canonical owners = %#v, want pinrouting owner", view.Equivalence.CanonicalOwners)
	}
	if !containsString(view.Equivalence.CanonicalOwners, "runtime/semanticview.BuildAuthoredEventEndpointCensus") || !containsString(view.Equivalence.CanonicalOwners, "runtime/routingtopology.Build") {
		t.Fatalf("canonical owners = %#v, want endpoint census and routing topology owners", view.Equivalence.CanonicalOwners)
	}

	account := flowByID(t, view, finalflowinstanceauthoring.TemplateFlowID)
	if account.PrimaryEntity == nil || account.PrimaryEntity.Type != finalflowinstanceauthoring.TemplateEntityType {
		t.Fatalf("account primary entity = %#v, want %s", account.PrimaryEntity, finalflowinstanceauthoring.TemplateEntityType)
	}
	if account.TemplateInstance == nil || account.TemplateInstance.Field != finalflowinstanceauthoring.TemplateInstanceBy {
		t.Fatalf("account template instance = %#v, want %s", account.TemplateInstance, finalflowinstanceauthoring.TemplateInstanceBy)
	}

	producer := flowByID(t, view, finalflowinstanceauthoring.ProducerFlowID)
	output := outputPinByName(t, producer, finalflowinstanceauthoring.ProducerOutputPin)
	if output.Event != finalflowinstanceauthoring.ProducerOutput || output.PinDigest == "" {
		t.Fatalf("producer output = %#v, want canonical immutable event pin", output)
	}
	edges := interFlowRouteEdges(view.RoutingTopology)
	if len(edges) != 1 {
		t.Fatalf("inter-flow route edge count = %d, want 1: %#v", len(edges), edges)
	}
	edge := edges[0]
	if edge.Producer.FlowID != finalflowinstanceauthoring.ProducerFlowID || edge.Consumer.FlowID != finalflowinstanceauthoring.TemplateFlowID {
		t.Fatalf("route endpoints = %#v -> %#v, want final fixture producer to template", edge.Producer, edge.Consumer)
	}
	if edge.Resolution == nil || edge.Resolution.InstanceKey == nil ||
		edge.Resolution.InstanceKey.SourceKind != string(runtimecontracts.FlowInputInstanceSourcePayload) ||
		edge.Resolution.InstanceKey.SourcePath != "payload."+finalflowinstanceauthoring.TemplatePayloadKey ||
		edge.Resolution.InstanceKey.DerivedFrom == "" {
		t.Fatalf("route instance key = %#v, want derived receiver carry source", edge.Resolution)
	}

}

func TestBuildScansFlowLocalDuplicateNodeIDsForContainedOperations(t *testing.T) {
	source := loadDuplicateNodeIDContainedOpsSource(t)
	view := mustBuild(t, source, nil)

	for _, flowID := range []string{"alpha", "beta"} {
		flow := flowByID(t, view, flowID)
		op := containedOperationByTargetAndOp(t, flow, "entity.items", "set")
		if op.NodeID != identitytest.FlowNode(t, flowID, "indexer").Key() {
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
		if pin.Event == name {
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

func loadAuthoringAgentProvenanceSource(t testing.TB, explicitFlowRequiredAgents bool) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeAuthoringViewTestFile(t, filepath.Join(root, "package.yaml"), `
name: authoring-agent-provenance
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - {id: analysis, flow: analysis, mode: static}
`)
	writeAuthoringViewTestFile(t, filepath.Join(root, "schema.yaml"), "name: authoring-agent-provenance\n")
	writeAuthoringViewTestFile(t, filepath.Join(root, "agents.yaml"), `
root-agent:
  role: root-agent
  intent: {inline: Coordinate root analysis work.}
  model: regular
  subscriptions: [root.requested]
  emit_events: [root.done]
`)
	flowSchema := "name: analysis\nmode: static\n"
	if explicitFlowRequiredAgents {
		flowSchema += "required_agents: []\n"
	}
	flowRoot := filepath.Join(root, "flows", "analysis")
	writeAuthoringViewTestFile(t, filepath.Join(flowRoot, "schema.yaml"), flowSchema)
	writeAuthoringViewTestFile(t, filepath.Join(flowRoot, "agents.yaml"), `
analyzer:
  role: analyzer
  intent: {inline: Analyze requested work.}
  model: regular
  subscriptions: [analysis.requested]
`)

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
	writeAuthoringViewTestFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
mode: template
instance: account_id
`)
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
      - observed
`)
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
