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
	if len(graph.Nodes) != 3 {
		t.Fatalf("graph nodes = %#v, want waiting/active/done", graph.Nodes)
	}
	if !graph.Nodes[0].Initial || graph.Nodes[0].ID != "waiting" {
		t.Fatalf("first graph node = %#v, want waiting initial", graph.Nodes[0])
	}
	var terminalDone bool
	for _, node := range graph.Nodes {
		if node.ID == "done" && node.Terminal {
			terminalDone = true
		}
	}
	if !terminalDone {
		t.Fatalf("graph nodes = %#v, want terminal done", graph.Nodes)
	}
	if len(graph.Edges) == 0 {
		t.Fatalf("graph edges missing: %#v", graph)
	}
	var foundOpenedAdvance bool
	for _, edge := range graph.Edges {
		if edge.Source == "handler.advances_to" && edge.NodeID == "support-node" && edge.EventType == "ticket.opened" && edge.To == "active" {
			foundOpenedAdvance = true
		}
	}
	if !foundOpenedAdvance {
		t.Fatalf("graph edges = %#v, want ticket.opened handler.advances_to edge to active", graph.Edges)
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
		"done [terminal]",
		"handler.advances_to support-node on ticket.opened",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("describe --graph output missing %q:\n%s", want, text)
		}
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
  active: {}
  done:
    terminal: true
`)
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "tools.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  entity_id: string
ticket.closed:
  entity_id: string
`)
	writeDescribeTestFile(t, filepath.Join(root, "flows", "support", "entities.yaml"), `
ticket: {}
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
      advances_to: active
    ticket.closed:
      advances_to: done
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
