package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentDeclarationRecordsCanonicalizeDeepPackageBackedFlowProjection(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: canonical-agent-records
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - {path: parent}
`)
	writeAgentDeclarationBaseFiles(t, root, "canonical-agent-records")
	writeFixtureFile(t, filepath.Join(root, "parent", "package.yaml"), `
name: parent
version: "1.0.0"
packages:
  - {path: child}
`)
	writeFixtureFile(t, filepath.Join(root, "parent", "child", "package.yaml"), `
name: child
version: "1.0.0"
flows:
  - {id: support, flow: support, mode: static}
`)
	flowRoot := filepath.Join(root, "parent", "child", "flows", "support")
	writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: support\nmode: static\ninitial_state: active\nstates: [active]\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), `
worker:
  id: public-worker
  role: worker
  intent: {inline: Exercise canonical physical declaration ownership.}
  model: regular
  memory: false
  subscriptions: [work.requested]
`)
	for _, file := range []string{"events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		writeFixtureFile(t, filepath.Join(flowRoot, file), "{}\n")
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	rawOccurrences := 0
	for _, view := range bundle.ProjectViews() {
		if _, ok := view.Agents["worker"]; ok {
			rawOccurrences++
		}
	}
	for _, view := range bundle.FlowViews() {
		if _, ok := view.Agents["worker"]; ok {
			rawOccurrences++
		}
	}
	if rawOccurrences < 2 {
		t.Fatalf("raw declaration occurrences = %d, want loader projection in both package and flow views", rawOccurrences)
	}

	records := bundle.AgentDeclarationRecords()
	if len(records) != 1 {
		t.Fatalf("records = %#v, want one physical declaration", records)
	}
	record := records[0]
	if record.LogicalID != "worker" || record.OwnerFlowID != "support" || record.Source.Layer != "flow" ||
		record.Source.PackageKey != "parent/child" || record.Source.FlowID != "support" ||
		!strings.HasSuffix(filepath.ToSlash(record.Source.File), "parent/child/flows/support/agents.yaml") {
		t.Fatalf("record = %#v, want exact child-package flow declaration", record)
	}
	project, ok := bundle.ProjectViewByKey("parent/child/flows/support")
	if !ok || record.OwnerURI == "" || record.OwnerURI != project.AgentURIs["worker"] {
		t.Fatalf("record owner = %q, project owners = %#v", record.OwnerURI, project.AgentURIs)
	}
	if got := bundle.PackageOwningFlowID("parent/child/flows/support"); got != "support" {
		t.Fatalf("PackageOwningFlowID = %q, want support", got)
	}

	records[0].Entry.Subscriptions[0] = "mutated"
	records[0].Entry.AuthoredFields["hostile"] = true
	again := bundle.AgentDeclarationRecords()[0]
	if again.Entry.Subscriptions[0] != "work.requested" || again.Entry.AuthoredFields["hostile"] {
		t.Fatalf("caller mutation escaped immutable record snapshot: %#v", again.Entry)
	}
}

func TestAgentDeclarationRecordsPreserveDistinctSameIDPhysicalDeclarations(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: distinct-agent-records
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - {path: left}
  - {path: right}
`)
	writeAgentDeclarationBaseFiles(t, root, "distinct-agent-records")
	for _, side := range []string{"left", "right"} {
		packageRoot := filepath.Join(root, side)
		writeFixtureFile(t, filepath.Join(packageRoot, "package.yaml"), "name: "+side+"\nversion: \"1.0.0\"\nflows: []\n")
		writeFixtureFile(t, filepath.Join(packageRoot, "agents.yaml"), "worker:\n  role: "+side+"-worker\n  intent: {inline: Exercise distinct physical identity.}\n  model: regular\n  memory: false\n")
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	records := bundle.AgentDeclarationRecords()
	if len(records) != 2 {
		t.Fatalf("records = %#v, want both same-ID physical declarations", records)
	}
	if records[0].LogicalID != "worker" || records[1].LogicalID != "worker" || records[0].OwnerURI == records[1].OwnerURI || records[0].Source.File == records[1].Source.File {
		t.Fatalf("distinct records collapsed: %#v", records)
	}
}

func writeAgentDeclarationBaseFiles(t *testing.T, root, name string) {
	t.Helper()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: "+name+"\n")
	for _, file := range []string{"agents.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		writeFixtureFile(t, filepath.Join(root, file), "{}\n")
	}
}
