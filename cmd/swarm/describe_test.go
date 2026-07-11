package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestDescribeCommandJSONRendersExpandedAuthoringView(t *testing.T) {
	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{})
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	if len(view.ConnectRoutePlans) != 1 {
		t.Fatalf("connect route plans = %#v, want one", view.ConnectRoutePlans)
	}
	plan := view.ConnectRoutePlans[0]
	if plan.ResolutionKind != "instance_key" || plan.InstanceKey == nil {
		t.Fatalf("route plan = %#v, want instance_key plan", plan)
	}
	if plan.Source.Key != "account_id" || len(plan.Source.Carries) == 0 {
		t.Fatalf("route source = %#v, want output key/carries", plan.Source)
	}
	if len(view.Flows) != 2 {
		t.Fatalf("flow count = %d, want 2", len(view.Flows))
	}
}

func TestDescribeCommandDiagnosticsCarryRemediationAndEvidence(t *testing.T) {
	contractsRoot := writeVerifyBootTimerCommandFixture(t, "state:done")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	code = executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	if code != cliExitValidation {
		t.Fatalf("describe missing contracts code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
	code = executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
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
		"waiting ->xN line_item.requested items_from payload.line_items as line_item identity line_item (handler.fan_out support-node on ticket.opened)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("describe --graph output missing %q:\n%s", want, text)
		}
	}
}

func TestVerifyCommandAcceptsJoinTransitionCarrierFixture(t *testing.T) {
	contractsRoot := writeDescribeStageGraphContracts(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"verify",
		"--contracts", contractsRoot,
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
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"verify",
		"--contracts", contractsRoot,
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
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "package.yaml"), `
name: stage-graph
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
`)
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "name: stage-graph\n")
	writeDescribeTestFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
stages:
  waiting:
    initial: true
  active:
    timers:
      - after: 48h
        emit: ticket.sla_escalated
      - after: 72h
        advances_to: timed_out
  review:
    terminal: true
  timed_out:
    terminal: true
`)
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  swarm:
    source: external
  entity_id: string
  line_items: "[text]"
ticket.closed:
  swarm:
    source: external
  entity_id: string
  line_item_id: string
  result: string
ticket.sla_escalated:
  swarm:
    consumer: [operator]
  entity_id: string
line_item.requested:
  swarm:
    consumer: [worker]
  line_item_id: string
  line_item_index: integer
accumulate.timeout:
  swarm:
    source: platform
`)
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "entities.yaml"), `
ticket:
  expected_line_item_ids:
    type: "[text]"
    initial: []
`)
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
  event_handlers:
    ticket.opened:
      create_entity: true
      fan_out:
        items_from: payload.line_items
        as: line_item
        identity: line_item
        emit:
          event: line_item.requested
          fields:
            line_item_id: line_item
            line_item_index: fan_out.index
      advances_to: active
    ticket.closed:
      join:
        stage: active
        members:
          from: entity.expected_line_item_ids
          by: payload.line_item_id
        output: payload.result
        on_complete:
          advances_to: review
        timeout:
          after: 1h
          advances_to: timed_out
`)
	return root
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
