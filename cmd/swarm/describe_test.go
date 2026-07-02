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

func writeDescribeRootPrimaryEntityContracts(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "package.yaml"), `
name: root-primary-entity
version: "1.0.0"
platform_version: ">=1.6.0"
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
