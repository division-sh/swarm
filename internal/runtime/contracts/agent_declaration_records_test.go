package contracts

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentDeclarationRecordsCanonicalizeDeepFilesystemFlowProjection(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeAgentDeclarationBaseFiles(t, root, "canonical-agent-records")
	flowRoot := filepath.Join(root, "parent", "child", "support")
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

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	rawOccurrences := 0
	for _, view := range bundle.FlowViews() {
		if _, ok := view.Agents["worker"]; ok {
			rawOccurrences++
		}
	}
	if rawOccurrences != 1 {
		t.Fatalf("raw declaration occurrences = %d, want one filesystem-owned declaration", rawOccurrences)
	}

	records := bundle.AgentDeclarationRecords()
	if len(records) != 1 {
		t.Fatalf("records = %#v, want one physical declaration", records)
	}
	record := records[0]
	if record.LogicalID != "worker" || record.OwnerFlowID != "parent/child/support" || record.Source.Family != "agents" ||
		record.Source.FlowPath != "parent/child/support" ||
		!strings.HasSuffix(filepath.ToSlash(record.Source.File), "parent/child/support/agents.yaml") {
		t.Fatalf("record = %#v, want exact filesystem flow declaration", record)
	}
	view, ok := bundle.FlowTree.ByPath["parent/child/support"]
	if !ok || record.OwnerURI == "" || record.OwnerURI != view.AgentURIs["worker"] {
		t.Fatalf("record owner = %q, flow owners = %#v", record.OwnerURI, view.AgentURIs)
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
	writeAgentDeclarationBaseFiles(t, root, "distinct-agent-records")
	for _, side := range []string{"left", "right"} {
		flowRoot := filepath.Join(root, side)
		writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: "+side+"\n")
		writeFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), "worker:\n  role: "+side+"-worker\n  intent: {inline: Exercise distinct physical identity.}\n  model: regular\n  memory: false\n")
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
}
