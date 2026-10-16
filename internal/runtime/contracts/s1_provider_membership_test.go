package contracts

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestS1FlowSourcesExcludeProviderDirectories(t *testing.T) {
	root := t.TempDir()
	manifest := "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [inbound.team], persist_dedupe_markers: true}, cannot: [execute code]}\n"
	files := map[string]string{
		"nodes.yaml":                   "coordinator: {type: procedure}\n",
		"team-bot/manifest.yaml":       manifest,
		"team-bot/trigger.yaml":        "event_name: {literal: inbound.team}\n",
		"child/data/value.txt":         "child\n",
		"child/team-bot/manifest.yaml": manifest,
		"child/team-bot/trigger.yaml":  "event_name: {literal: inbound.team}\n",
	}
	for label, body := range files {
		file := filepath.Join(root, label)
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*sourceartifact.AdmittedSourceArtifact{artifact, retained} {
		flows, err := indexFlowSources(source)
		if err != nil {
			t.Fatal(err)
		}
		if len(flows) != 2 || flows["."].FlowPath != "." || flows["child"].FlowPath != "child" {
			t.Fatalf("provider became a flow source: %+v", flows)
		}
		providers := source.ProviderDirectories()
		if len(providers) != 2 || providers[0].FlowPath() != "child" || providers[1].FlowPath() != "." {
			t.Fatalf("provider lost its declaring flow: %+v", providers)
		}
	}
	provider, err := sourceartifact.AdmitDirectory(filepath.Join(root, "team-bot"))
	if err != nil {
		t.Fatal(err)
	}
	if bundle, err := LoadWorkflowContractBundleFromArtifact(root, provider, "", WorkflowContractLoadOptions{}); err == nil || bundle != nil {
		t.Fatalf("provider-only source acquired execution: bundle=%+v err=%v", bundle, err)
	}
}
