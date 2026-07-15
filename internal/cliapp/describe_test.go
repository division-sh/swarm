package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func writeStandingTelegramServeFixture(t testing.TB, telegramBaseURL string) string {
	t.Helper()
	return canonicalrouting.CopyStandingTelegramServe(t, telegramBaseURL)
}

func TestDescribeCommandJSONRendersExpandedAuthoringView(t *testing.T) {
	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{})
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("describe --json stderr = %q, want empty", stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode describe json: %v\n%s", err, stdout.String())
	}
	if view.SourceAuthority != "projection_only_existing_contract_owners" {
		t.Fatalf("source authority = %q, want projection marker", view.SourceAuthority)
	}
	if view.Root.PrimaryEntity != nil || view.Root.PrimaryEntityError != "" {
		t.Fatalf("root primary entity for valid no-root fixture = entity %#v error %q, want none", view.Root.PrimaryEntity, view.Root.PrimaryEntityError)
	}
	if view.RoutingTopology.SchemaVersion != "routing-topology/v1" || !view.RoutingTopology.ProjectionOnly {
		t.Fatalf("routing topology identity = %#v, want routing-topology/v1 projection", view.RoutingTopology)
	}
	if len(view.RoutingTopology.Edges) == 0 {
		t.Fatalf("routing topology edges = %#v, want routed template edge", view.RoutingTopology.Edges)
	}
	if len(view.Flows) != 2 {
		t.Fatalf("flow count = %d, want 2", len(view.Flows))
	}
}

func TestDescribeCommandRendersStandingIngressDeclaration(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe standing ingress code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode standing describe json: %v\n%s", err, stdout.String())
	}
	if len(view.Flows) != 2 {
		t.Fatalf("standing describe flows = %#v", view.Flows)
	}
	var flow authoringview.FlowView
	for _, candidate := range view.Flows {
		if candidate.ID == "telegram-ingress" {
			flow = candidate
			break
		}
	}
	if flow.Activation != "standing" || flow.Ingress == nil || flow.Ingress.Alias != "chat" || len(flow.Ingress.Providers) != 1 {
		t.Fatalf("standing describe flow = %#v", flow)
	}
	provider := flow.Ingress.Providers[0]
	if provider.Provider != "telegram" || provider.SigningSecret != "webhook_signing.telegram" || provider.AdmissionKind != "pack-required" || provider.PackID != "" || provider.RequestAuthentication != "" {
		t.Fatalf("standing describe provider = %#v", provider)
	}
	if len(view.RoutingTopology.RootInputSources) != 1 {
		t.Fatalf("standing root input sources = %#v", view.RoutingTopology.RootInputSources)
	}
	source := view.RoutingTopology.RootInputSources[0]
	if source.Kind != routingtopology.RootInputSourceStandingIngress || source.Alias != "chat" || source.Provider != "telegram" || source.Target.FlowID != "telegram-ingress" || source.Target.FlowPath != "telegram-ingress" || source.Admission.Kind != "pack-required" || source.Admission.PackID != "" {
		t.Fatalf("standing root input source = %#v", source)
	}
	for _, edge := range view.RoutingTopology.Edges {
		if edge.Scope != routingtopology.DeliveryScopeTypedPubSub && edge.Scope != routingtopology.DeliveryScopeInterFlowConnect {
			t.Fatalf("standing ingress created third delivery scope: %#v", edge)
		}
	}
}

func TestDescribeRoutesProjectsStandingIngressAsRootInputAdmission(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	var jsonOut, jsonErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot, "--json"}, &jsonOut, &jsonErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes --json code=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
	}
	var topology routingtopology.Topology
	if err := json.Unmarshal(jsonOut.Bytes(), &topology); err != nil {
		t.Fatalf("decode standing routes topology: %v\n%s", err, jsonOut.String())
	}
	if len(topology.RootInputSources) != 1 || topology.RootInputSources[0].ID == "" {
		t.Fatalf("standing route root input sources = %#v", topology.RootInputSources)
	}

	var humanOut, humanErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes code=%d stdout=%s stderr=%s", code, humanOut.String(), humanErr.String())
	}
	for _, want := range []string{"root input sources:", "[standing_ingress] chat/telegram -> flow telegram-ingress admission=pack-required"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("standing routes human output missing %q:\n%s", want, humanOut.String())
		}
	}
}

func TestDescribeRoutesUsesVersionedTopologyAndMatchesFullDescribe(t *testing.T) {
	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{})
	var routeJSON, routeErr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe", "routes", "--contracts", contractsRoot, "--json",
	}, &routeJSON, &routeErr, defaultRootCommandOptions())
	if code != 0 || routeErr.Len() != 0 {
		t.Fatalf("describe routes --json code=%d stdout=%s stderr=%s", code, routeJSON.String(), routeErr.String())
	}
	var topology routingtopology.Topology
	if err := json.Unmarshal(routeJSON.Bytes(), &topology); err != nil {
		t.Fatalf("decode routes topology: %v\n%s", err, routeJSON.String())
	}
	if topology.SchemaVersion != routingtopology.SchemaVersion || !topology.ProjectionOnly {
		t.Fatalf("topology identity = %#v", topology)
	}
	if len(topology.Edges) == 0 {
		t.Fatalf("topology edges = %#v, want executable routes", topology.Edges)
	}

	var fullJSON, fullErr bytes.Buffer
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe", "--contracts", contractsRoot, "--json",
	}, &fullJSON, &fullErr, defaultRootCommandOptions())
	if code != 0 || fullErr.Len() != 0 {
		t.Fatalf("describe --json code=%d stdout=%s stderr=%s", code, fullJSON.String(), fullErr.String())
	}
	var full describeCommandOutput
	if err := json.Unmarshal(fullJSON.Bytes(), &full); err != nil {
		t.Fatalf("decode full describe: %v", err)
	}
	if !reflect.DeepEqual(full.RoutingTopology, topology) {
		t.Fatalf("full describe routing topology diverged from describe routes\nfull=%#v\nroutes=%#v", full.RoutingTopology, topology)
	}
}

func TestDescribeRoutesHumanAndJSONAreDeterministic(t *testing.T) {

	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{})
	var firstJSON, firstHuman string
	for i := 0; i < 5; i++ {
		var jsonOut, jsonErr bytes.Buffer
		if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot, "--json"}, &jsonOut, &jsonErr, defaultRootCommandOptions()); code != 0 {
			t.Fatalf("iteration %d json code=%d stderr=%s", i, code, jsonErr.String())
		}
		var humanOut, humanErr bytes.Buffer
		if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
			t.Fatalf("iteration %d human code=%d stderr=%s", i, code, humanErr.String())
		}
		if i == 0 {
			firstJSON, firstHuman = jsonOut.String(), humanOut.String()
		} else if jsonOut.String() != firstJSON || humanOut.String() != firstHuman {
			t.Fatalf("describe routes output changed on iteration %d", i)
		}
	}
	for _, want := range []string{"routing topology: routing-topology/v1", "typed_pubsub", "typed pub/sub: match=exact boundary=same_flow", "inter_flow_connect", "resolution:"} {
		if !strings.Contains(firstHuman, want) {
			t.Fatalf("human routes missing %q:\n%s", want, firstHuman)
		}
	}
}

func TestDescribeRoutesProjectsImportBoundaryWildcardProofOnHumanAndJSONSurfaces(t *testing.T) {
	contractsRoot := filepath.Join(RepoRoot(), "tests", "tier11-flow-composition", "test-wildcard-deep-subscription")
	var jsonOut, jsonErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot, "--json"}, &jsonOut, &jsonErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes --json code=%d stderr=%s", code, jsonErr.String())
	}
	var topology routingtopology.Topology
	if err := json.Unmarshal(jsonOut.Bytes(), &topology); err != nil {
		t.Fatalf("decode topology: %v", err)
	}
	found := false
	for _, edge := range topology.Edges {
		if edge.Event.Canonical != "child/grandchild/task.done" || edge.Consumer.NodeID != "collector" {
			continue
		}
		found = edge.Scope == routingtopology.DeliveryScopeTypedPubSub && edge.TypedPubSub != nil && edge.TypedPubSub.Match == "pattern" && edge.TypedPubSub.Boundary == "import_boundary" && edge.TypedPubSub.Authorization != nil
	}
	if !found {
		t.Fatalf("topology edges = %#v, want import-boundary wildcard proof", topology.Edges)
	}

	var humanOut, humanErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes code=%d stderr=%s", code, humanErr.String())
	}
	if !strings.Contains(humanOut.String(), "typed pub/sub: match=pattern boundary=import_boundary") {
		t.Fatalf("human routes omitted import proof:\n%s", humanOut.String())
	}
}

func TestDescribeRoutesCarriesExistingDanglingEventDiagnostic(t *testing.T) {
	contractsRoot := filepath.Join(RepoRoot(), "tests", "tier8-boot-verification", "test-boot-event-no-consumer")
	var jsonOut, jsonErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot, "--json"}, &jsonOut, &jsonErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes --json code=%d stderr=%s", code, jsonErr.String())
	}
	var topology routingtopology.Topology
	if err := json.Unmarshal(jsonOut.Bytes(), &topology); err != nil {
		t.Fatalf("decode routes topology: %v", err)
	}
	if len(topology.Issues) != 1 || topology.Issues[0].CheckID != "event_consumer_exists" || topology.Issues[0].Severity != runtimebootverify.SeveritySemanticDriftWarn {
		t.Fatalf("dangling route issues = %#v, want existing event_consumer_exists warning", topology.Issues)
	}

	var humanOut, humanErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes code=%d stderr=%s", code, humanErr.String())
	}
	for _, want := range []string{"event_consumer_exists [semantic_drift_warning]", "orphan.unconsumed", "route issues:"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("human dangling route output missing %q:\n%s", want, humanOut.String())
		}
	}
}

func TestDescribeRoutesHumanRendersTypedConnectIssueWithExactSource(t *testing.T) {
	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{BadConnectMapping: true})
	var humanOut, humanErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes code=%d stderr=%s", code, humanErr.String())
	}
	for _, want := range []string{"route issues:", "route_plan_instance_resolution_invalid", "package.yaml:"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("human route issue missing %q:\n%s", want, humanOut.String())
		}
	}
}

func TestDescribeRoutesRendersFindingLinkedLegacyQualifiedSubscriptions(t *testing.T) {
	contractsRoot := filepath.Join(RepoRoot(), "tests", "tier11-flow-composition", "test-child-flow-absolute-path")
	var jsonOut, jsonErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot, "--json"}, &jsonOut, &jsonErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes --json code=%d stderr=%s", code, jsonErr.String())
	}
	var topology routingtopology.Topology
	if err := json.Unmarshal(jsonOut.Bytes(), &topology); err != nil {
		t.Fatalf("decode routes topology: %v", err)
	}
	if len(topology.LegacyQualifiedSubscriptions) != 1 {
		t.Fatalf("legacy subscriptions = %#v, want one", topology.LegacyQualifiedSubscriptions)
	}
	legacy := topology.LegacyQualifiedSubscriptions[0]
	if legacy.FindingID == "" || legacy.CanonicalEdge || !legacy.RuntimeDelivery {
		t.Fatalf("legacy subscription = %#v, want finding-linked non-edge runtime debt", legacy)
	}
	linked := false
	for _, issue := range topology.Issues {
		if issue.ID == legacy.FindingID && issue.CheckID == "legacy_qualified_subscription" {
			linked = true
		}
	}
	if !linked {
		t.Fatalf("legacy finding %q not present in issues: %#v", legacy.FindingID, topology.Issues)
	}

	var humanOut, humanErr bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", "routes", "--contracts", contractsRoot}, &humanOut, &humanErr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe routes code=%d stderr=%s", code, humanErr.String())
	}
	for _, want := range []string{"legacy qualified subscriptions:", "disposition=legacy_qualified_subscription", "runtime delivery=true canonical edge=false", "finding="} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("human routes missing %q:\n%s", want, humanOut.String())
		}
	}
}

func TestDescribeCommandDiagnosticsCarryRemediationAndEvidence(t *testing.T) {
	contractsRoot := writeVerifyBootTimerCommandFixture(t, "state:done")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var output describeCommandOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode describe json: %v\n%s", err, stdout.String())
	}
	if len(output.Diagnostics) == 0 {
		t.Fatalf("describe diagnostics = %#v, want timer_validation", output.Diagnostics)
	}
	var timerDiagnostic *authoringview.DiagnosticView
	for i := range output.Diagnostics {
		if output.Diagnostics[i].CheckID == "timer_validation" {
			timerDiagnostic = &output.Diagnostics[i]
			break
		}
	}
	if timerDiagnostic == nil {
		t.Fatalf("describe diagnostics = %#v, want timer_validation", output.Diagnostics)
	}
	if strings.TrimSpace(timerDiagnostic.Remediation) == "" {
		t.Fatalf("timer diagnostic missing remediation: %#v", *timerDiagnostic)
	}
	if len(timerDiagnostic.Evidence) == 0 || !strings.Contains(strings.Join(timerDiagnostic.Evidence, "\n"), "cancel_on: state:done") {
		t.Fatalf("timer diagnostic evidence = %#v, want cancel_on evidence", timerDiagnostic.Evidence)
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	text := stdout.String()
	for _, want := range []string{
		"[BLOCKER] timer_validation @",
		"remediation:",
		"evidence:",
		"cancel_on: state:done",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("describe text missing %q:\n%s", want, text)
		}
	}
}

func TestDescribeMissingContractsIsValidationExit(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{"describe"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation {
		t.Fatalf("describe missing contracts code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("describe missing contracts stdout = %q, want empty", stdout.String())
	}
	if got := stderr.String(); !strings.Contains(got, "ERROR: a contracts directory is required.") || !strings.Contains(got, "Remediation: Pass a contracts directory") {
		t.Fatalf("describe missing contracts stderr = %q", got)
	}
}

func TestDescribeCommandRendersDefaultedTemplateInstancePolicies(t *testing.T) {
	contractsRoot := writeDescribeDefaultedTemplatePolicyContracts(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("describe stderr = %q, want empty", stderr.String())
	}
	if !strings.Contains(stdout.String(), "instance: by=account_id on_missing=create on_conflict=reject") {
		t.Fatalf("describe output missing defaulted policy readback:\n%s", stdout.String())
	}
}

func TestDescribeCommandJSONRendersRootPrimaryEntity(t *testing.T) {
	contractsRoot := writeDescribeRootPrimaryEntityContracts(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("describe --json stderr = %q, want empty", stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode describe json: %v\n%s", err, stdout.String())
	}
	if view.Root.PrimaryEntity == nil {
		t.Fatalf("root primary entity missing: %#v", view.Root)
	}
	if view.Root.PrimaryEntity.Type != "workspace" {
		t.Fatalf("root primary entity type = %q, want workspace", view.Root.PrimaryEntity.Type)
	}
	if view.Root.PrimaryEntity.Fields["org_id"] != "text" {
		t.Fatalf("root primary entity fields = %#v, want org_id text", view.Root.PrimaryEntity.Fields)
	}
}

func TestDescribeCommandGraphRendersStageGraph(t *testing.T) {
	contractsRoot := writeDescribeStageGraphContracts(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--graph",
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --graph --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var output describeCommandOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode describe graph json: %v\n%s", err, stdout.String())
	}
	if len(output.StageGraphs) != 1 {
		t.Fatalf("stage graphs = %#v, want one support graph", output.StageGraphs)
	}
	graph := output.StageGraphs[0]
	if graph.FlowID != "support" || graph.FlowPath != "support" {
		t.Fatalf("graph identity = %#v, want support namespace", graph)
	}
	if len(graph.Nodes) != 4 {
		t.Fatalf("graph nodes = %#v, want waiting/active/review/timed_out", graph.Nodes)
	}
	if !graph.Nodes[0].Initial || graph.Nodes[0].ID != "waiting" {
		t.Fatalf("first graph node = %#v, want waiting initial", graph.Nodes[0])
	}
	var terminalReview bool
	for _, node := range graph.Nodes {
		if node.ID == "review" && node.Terminal {
			terminalReview = true
		}
	}
	if !terminalReview {
		t.Fatalf("graph nodes = %#v, want terminal review", graph.Nodes)
	}
	if len(graph.Edges) == 0 {
		t.Fatalf("graph edges missing: %#v", graph)
	}
	var foundOpenedAdvance bool
	var foundJoinComplete bool
	var foundJoinTimeout bool
	var foundTimedAdvance bool
	for _, edge := range graph.Edges {
		if edge.Source == "handler.advances_to" && edge.NodeID == "support-node" && edge.EventType == "ticket.opened" && edge.To == "active" {
			foundOpenedAdvance = true
		}
		if edge.Source == "handler.join.on_complete" && edge.NodeID == "support-node" && edge.EventType == "ticket.closed" && edge.To == "review" {
			foundJoinComplete = true
		}
		if edge.Source == "handler.join.timeout" && edge.NodeID == "support-node" && edge.EventType == "platform.join_timeout" && edge.TimerID == "active" && edge.After == "1h" && edge.To == "timed_out" {
			foundJoinTimeout = true
		}
		if edge.Source == "timer" && edge.TimerID == "support.active.timed_out" && edge.After == "72h" && edge.To == "timed_out" {
			foundTimedAdvance = true
		}
	}
	if !foundOpenedAdvance {
		t.Fatalf("graph edges = %#v, want ticket.opened handler.advances_to edge to active", graph.Edges)
	}
	if !foundJoinComplete {
		t.Fatalf("graph edges = %#v, want ticket.closed handler.join.on_complete edge to review", graph.Edges)
	}
	if !foundJoinTimeout {
		t.Fatalf("graph edges = %#v, want timed platform.join_timeout edge to timed_out", graph.Edges)
	}
	if !foundTimedAdvance {
		t.Fatalf("graph edges = %#v, want stage timer edge to timed_out with delay", graph.Edges)
	}
	if len(graph.Timers) != 2 {
		t.Fatalf("graph timers = %#v, want emit and advance stage timers", graph.Timers)
	}
	if len(graph.FanOuts) != 1 {
		t.Fatalf("graph fan_outs = %#v, want ticket.opened collection fan-out", graph.FanOuts)
	}
	fanOut := graph.FanOuts[0]
	if fanOut.Emit != "line_item.requested" || fanOut.ItemsFrom != "payload.line_items" || fanOut.ItemAlias != "line_item" || fanOut.Identity != "line_item" {
		t.Fatalf("graph fan_out = %#v, want line_item multiplicity metadata", fanOut)
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--graph",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --graph code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	text := stdout.String()
	for _, want := range []string{
		"stage graph:",
		"flow support (support):",
		"waiting [initial]",
		"review [terminal]",
		"handler.advances_to support-node on ticket.opened",
		"handler.join.on_complete support-node on ticket.closed",
		"handler.join.timeout support-node on platform.join_timeout after 1h timer active",
		"timer runtime on timer:support.active.timed_out after 72h timer support.active.timed_out",
		"active after 48h emit ticket.sla_escalated (timer support.active.ticket.sla_escalated)",
		"active after 72h advances_to timed_out (timer support.active.timed_out)",
		"waiting ->xN line_item.requested items_from payload.line_items as line_item identity line_item max_items 1000 (handler.fan_out support-node on ticket.opened)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("describe --graph output missing %q:\n%s", want, text)
		}
	}
}

func TestDescribeFanInBarrierShowsEffectiveJoinProvenance(t *testing.T) {
	contractsRoot := canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier)

	t.Run("json", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
			"describe", "--contracts", contractsRoot, "--graph", "--json",
		}, &stdout, &stderr, defaultRootCommandOptions())
		if code != 0 {
			t.Fatalf("describe barrier json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		var output describeCommandOutput
		if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
			t.Fatalf("decode describe barrier json: %v\n%s", err, stdout.String())
		}
		var joins []authoringview.StageGraphJoinView
		for _, graph := range output.StageGraphs {
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
			join.WindowBy != "payload.period_id" || join.WindowBySource != "resolution.window" {
			t.Fatalf("barrier json provenance = %#v", join)
		}
	})

	t.Run("human", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
			"describe", "--contracts", contractsRoot, "--graph",
		}, &stdout, &stderr, defaultRootCommandOptions())
		if code != 0 {
			t.Fatalf("describe barrier code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		for _, want := range []string{
			"members entity.expected_operating_ids by payload.operating_id <- resolution.dedup_by",
			"window entity.period_id by payload.period_id <- resolution.window",
			"fan_in_pin operating_reported",
		} {
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("describe barrier output missing %q:\n%s", want, stdout.String())
			}
		}
	})
}

func TestVerifyCommandAcceptsJoinTransitionCarrierFixture(t *testing.T) {
	contractsRoot := writeDescribeStageGraphContracts(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"verify",
		"--contracts", contractsRoot,
		"--config", writeTestVerifyRuntimeConfig(t),
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("verify --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("verify --json stderr = %q, want empty", stderr.String())
	}
	output := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if !output.OK {
		t.Fatalf("verify --json output = %#v, want ok", output)
	}
	for _, warning := range output.Warnings {
		if warning.CheckID == "semantic_drift_unreachable_state" && (strings.Contains(warning.Message, "review") || strings.Contains(warning.Message, "timed_out")) {
			t.Fatalf("verify warnings = %#v, want accumulator carrier states reachable", output.Warnings)
		}
	}
}

func TestVerifyCommandRejectsTypeInvalidJoinCompletion(t *testing.T) {
	contractsRoot := writeDescribeStageGraphContracts(t)
	nodesPath := filepath.Join(contractsRoot, "flows", "support", "nodes.yaml")
	nodes, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(nodes), "      join:\n        stage: active", "      join:\n        complete_when: join.missing > 1\n        remaining: ignore\n        stage: active", 1)
	if updated == string(nodes) {
		t.Fatal("join fixture mutation did not match")
	}
	writeDescribeTestFile(t, nodesPath, updated)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"verify",
		"--contracts", contractsRoot,
		"--config", writeTestVerifyRuntimeConfig(t),
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("verify --json code = 0 stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("verify --json stderr = %q, want structured stdout failure", stderr.String())
	}
	output := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if output.OK || len(output.Errors) == 0 {
		t.Fatalf("verify --json output = %#v, want hard invalidity", output)
	}
	found := false
	for _, finding := range output.Errors {
		if finding.CheckID == "join_validation" && strings.Contains(finding.Message, "join.missing > 1") && strings.Contains(finding.Message, "no matching overload") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verify --json errors = %#v, want typed join_validation rejection", output.Errors)
	}
}

func TestVerifyCommandRejectsTypeInvalidNamedJoinResult(t *testing.T) {
	contractsRoot := writeDescribeStageGraphContracts(t)
	flowRoot := filepath.Join(contractsRoot, "flows", "support")
	writeDescribeTestFile(t, filepath.Join(flowRoot, "types.yaml"), `
types:
  JoinResult:
    value: text
`)
	eventsPath := filepath.Join(flowRoot, "events.yaml")
	events, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	writeDescribeTestFile(t, eventsPath, strings.Replace(string(events), "  result: string", "  result: JoinResult", 1))
	nodesPath := filepath.Join(flowRoot, "nodes.yaml")
	nodes, err := os.ReadFile(nodesPath)
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(nodes), "      join:\n        stage: active", "      join:\n        complete_when: join.results[0] > 1\n        remaining: ignore\n        stage: active", 1)
	if updated == string(nodes) {
		t.Fatal("join fixture mutation did not match")
	}
	writeDescribeTestFile(t, nodesPath, updated)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"verify", "--contracts", contractsRoot, "--config", writeTestVerifyRuntimeConfig(t), "--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("verify --json code = 0 stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	output := decodeOutputJSON[verifyCommandResult](t, stdout.String())
	if !reportContainsVerifyError(output.Errors, "join_validation", "no matching overload") {
		t.Fatalf("verify --json errors = %#v, want named JoinResult typed rejection", output.Errors)
	}
}

func reportContainsVerifyError(findings []verifyFindingOutput, checkID, message string) bool {
	for _, finding := range findings {
		if finding.CheckID == checkID && strings.Contains(finding.Message, message) {
			return true
		}
	}
	return false
}

func writeDescribeDefaultedTemplatePolicyContracts(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "package.yaml"), `
name: defaulted-template-policy
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: scoring
    flow: scoring
    mode: template
`)
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "name: defaulted-template-policy\n")
	writeDescribeTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
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
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "scoring", "entities.yaml"), `
account:
  account_id: uuid
`)
	return root
}

func writeDescribeStageGraphContracts(t testing.TB) string {
	t.Helper()
	return canonicalrouting.CopyDescribeStageGraph(t)
}

func writeDescribeRootPrimaryEntityContracts(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "package.yaml"), `
name: root-primary-entity
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "name: root-primary-entity\n")
	writeDescribeTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "entities.yaml"), `
workspace:
  org_id: text
  region: text
`)
	return root
}

func writeDescribeTestFile(t testing.TB, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
