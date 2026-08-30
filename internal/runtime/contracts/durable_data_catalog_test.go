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
	if items[0].FlowPath == items[1].FlowPath || items[0].Ref.CanonicalInputLabel == items[1].Ref.CanonicalInputLabel {
		t.Fatalf("static identities lost owning-flow/catalog coordinate: %#v", items)
	}

	for _, record := range bundle.AgentDeclarationRecords() {
		grants := bundle.StaticDataForAgent(record.OwnerFlowID, record.LogicalID)
		if len(grants) != 1 || grants[0].FlowPath != record.OwnerFlowID || !bytes.Equal(grants[0].Content, []byte("same bytes\n")) {
			t.Fatalf("agent %s/%s static grants = %#v", record.OwnerFlowID, record.LogicalID, grants)
		}
	}

	if err := os.WriteFile(filepath.Join(root, "alpha", "data", "resume.md"), []byte("mutated\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "beta", "data")); err != nil {
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
	if len(items) != 1 || items[0].FlowPath != "beta" {
		t.Fatalf("compiled static projection = %#v, want only referenced beta file", items)
	}
}

func writeStaticDataCatalogFixture(t *testing.T, alpha, beta []byte, alphaAccess bool) string {
	t.Helper()
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: static-catalog-test\n")
	writeStaticDataCatalogFlow(t, root, "alpha", alpha, alphaAccess)
	writeStaticDataCatalogFlow(t, root, "beta", beta, true)
	return root
}

func writeStaticDataCatalogFlow(t *testing.T, root, flowID string, content []byte, access bool) {
	t.Helper()
	flowRoot := filepath.Join(root, flowID)
	writeFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: "+flowID+"\nmode: static\n")
	agent := "worker:\n  role: worker\n  intent: {inline: \"Read admitted static data.\"}\n"
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
