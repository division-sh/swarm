package contracts

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestOrderedRuleRetirementReachesDiskCatalogAndRetainedChildren(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, scope := range []string{".", "unused-child"} {
		for _, tc := range []struct{ name, body, want string }{
			{"keyed rules", "rules: {fallback: {else: true}}", "DIALECT-RULES-ORDER"},
			{"singleton completion", "on_complete: {emit: done}", "DIALECT-OC-ORDER"},
			{"empty rule ID", "rules: [{id: '', else: true}]", `rule field "id" is not supported`},
			{"null rule description", "rules: [{description: null, else: true}]", `rule field "description" is not supported`},
			{"completion ID", "on_complete: [{id: owner, emit: done}]", `rule field "id" is not supported`},
			{"merged completion description", "_note: &row {description: null, emit: done}\non_complete: [{<<: *row}]", `rule field "description" is not supported`},
		} {
			t.Run(scope+"/"+tc.name, func(t *testing.T) {
				root := t.TempDir()
				writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: ordered-rule-source\n")
				if scope != "." {
					writeFixtureFile(t, filepath.Join(root, scope, "schema.yaml"), "name: unused-child\n")
				}
				writeFixtureFile(t, filepath.Join(root, scope, "events.yaml"), "request:\ndone:\n")
				writeFixtureFile(t, filepath.Join(root, scope, "nodes.yaml"),
					"worker:\n  execution_type: system_node\n  event_handlers:\n    request:\n      "+strings.ReplaceAll(tc.body, "\n", "\n      ")+"\n")
				bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				if bundle != nil || err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "nodes.yaml") {
					t.Fatalf("disk admission must refuse its exact rule owner: bundle=%v err=%v", bundle, err)
				}
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := persisted.Decode()
				if err != nil {
					t.Fatal(err)
				}
				retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
				if err != nil {
					t.Fatal(err)
				}
				for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"catalog": catalog, "retained": retained} {
					if source.BundleHash() != artifact.BundleHash() || string(source.LogicalBlob()) != string(artifact.LogicalBlob()) {
						t.Fatalf("%s changed the exact refused source bytes/hash", name)
					}
					bundle, err := LoadWorkflowContractBundleFromArtifact(repo, source, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
					if bundle != nil || err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "nodes.yaml") {
						t.Fatalf("%s bypassed the exact rule refusal: bundle=%v err=%v", name, bundle, err)
					}
				}
			})
		}
	}
}
