package contracts

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaticDataCatalogUsesExactOwningFlowAndFreezesBytes(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writeStaticDataCatalogFixture(t, []byte("same bytes\n"), []byte("same bytes\n"), true)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load static data fixture: %v", err)
	}
	items := bundle.StaticData()
	if len(items) != 2 {
		t.Fatalf("static data = %#v, want two exact flow-scoped facts", items)
	}
	if !bundle.DataProjectionRequired() {
		t.Fatal("compiled flow_data_access did not require a workspace projection")
	}
	if items[0].StaticID == items[1].StaticID {
		t.Fatalf("same relative filename across flows aliased to %s", items[0].StaticID)
	}
	if items[0].ContentDigest != items[1].ContentDigest {
		t.Fatalf("identical bytes produced different content digests: %#v", items)
	}
	if items[0].OwnerFlowID == items[1].OwnerFlowID || items[0].Ref.CanonicalInputLabel == items[1].Ref.CanonicalInputLabel {
		t.Fatalf("static identities lost owning-flow/catalog coordinate: %#v", items)
	}

	for _, record := range bundle.AgentDeclarationRecords() {
		grants := bundle.StaticDataForAgent(record.Source.PackageKey, record.OwnerFlowID, record.LogicalID)
		if len(grants) != 1 || grants[0].OwnerFlowID != record.OwnerFlowID || !bytes.Equal(grants[0].Content, []byte("same bytes\n")) {
			t.Fatalf("agent %s/%s static grants = %#v", record.OwnerFlowID, record.LogicalID, grants)
		}
	}

	if err := os.WriteFile(filepath.Join(root, "flows", "alpha", "data", "resume.md"), []byte("mutated\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "flows", "beta", "data")); err != nil {
		t.Fatal(err)
	}
	for _, item := range bundle.StaticData() {
		if !bytes.Equal(item.Content, []byte("same bytes\n")) {
			t.Fatalf("compiled static bytes changed with source filesystem: %#v", item)
		}
	}
}

func TestStaticDataCatalogRejectsInvalidUTF8OnlyWhenReferenced(t *testing.T) {
	repo := repoRootForContractsTest(t)
	invalid := []byte{0xff, 0xfe, 0xfd}
	referenced := writeStaticDataCatalogFixture(t, invalid, []byte("valid\n"), true)
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, referenced, DefaultPlatformSpecFile(repo)); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("referenced invalid UTF-8 error = %v", err)
	}

	unreferenced := writeStaticDataCatalogFixture(t, invalid, []byte("valid\n"), false)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, unreferenced, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("unreferenced raw bytes should remain admissible: %v", err)
	}
	items := bundle.StaticData()
	if len(items) != 1 || items[0].OwnerFlowID != "beta" {
		t.Fatalf("compiled static projection = %#v, want only referenced beta file", items)
	}
}

func writeStaticDataCatalogFixture(t *testing.T, alpha, beta []byte, alphaAccess bool) string {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: static-catalog-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: alpha
    flow: alpha
    mode: static
  - id: beta
    flow: beta
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: static-catalog-test\n")
	for _, name := range []string{"agents.yaml", "events.yaml", "policy.yaml", "tools.yaml"} {
		writeFixtureFile(t, filepath.Join(root, name), "{}\n")
	}
	writeStaticDataCatalogFlow(t, root, "alpha", "11111111-1111-4111-8111-111111111111", alpha, alphaAccess)
	writeStaticDataCatalogFlow(t, root, "beta", "22222222-2222-4222-8222-222222222222", beta, true)
	return root
}

func writeStaticDataCatalogFlow(t *testing.T, root, flowID, agentID string, content []byte, access bool) {
	t.Helper()
	flowRoot := filepath.Join(root, "flows", flowID)
	writeFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: "+flowID+"\nversion: \"1.0.0\"\nflows: []\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: "+flowID+"\nmode: static\n")
	writeFixtureFile(t, filepath.Join(flowRoot, "events.yaml"), "{}\n")
	agent := "worker:\n  id: " + agentID + "\n  role: worker\n  intent: {inline: \"Read admitted static data.\"}\n"
	if access {
		agent += "  flow_data_access:\n    - resume.md\n"
	}
	writeFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), agent)
	path := filepath.Join(flowRoot, "data", "resume.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}
