package bootverify

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStandingFlowConstructorEligibility(t *testing.T) {
	for _, tc := range []struct {
		name, schema, fields, handler string
		eligible                      bool
	}{
		{name: "keyless root", eligible: true},
		{name: "keyed root", schema: "instance: tenant\n", fields: "  tenant: text\n"},
		{name: "unassigned initial read", fields: "  brief: text\n", handler: "      guard: {check: entity.brief != ''}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: standing-root\nstages: []\n"+tc.schema)
			if tc.fields != "" {
				writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "root_state:\n"+tc.fields)
			}
			if tc.handler != "" {
				writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "work:\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "reader:\n  execution_type: system_node\n  subscribes_to: [work]\n  event_handlers:\n    work:\n"+tc.handler)
			}
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			rebuilt, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, bundle.SourceArtifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, source := range []semanticview.Source{semanticview.Wrap(bundle), semanticview.Wrap(rebuilt)} {
				constructor, err := pipeline.CompileFlowConstructor(source, ".", "")
				if got := err == nil && constructor.Eligible(); got != tc.eligible {
					t.Fatalf("standing eligibility=%v want %v", got, tc.eligible)
				}
			}
		})
	}
	if _, err := pipeline.CompileFlowConstructor(nil, ".", ""); err == nil {
		t.Fatal("standing activation acquired authority without a source")
	}
}
