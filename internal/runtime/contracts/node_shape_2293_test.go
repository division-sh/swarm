package contracts

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNodeShapeRetiredCarriersRejectOnPresence(t *testing.T) {
	for _, field := range []string{"execution_type", "subscribes_to", "produces", "event_handlers"} {
		for _, value := range []string{"null", "false", "''", "system_node", "[]", "{}", "{work.requested: {}}"} {
			for _, form := range []string{"direct", "alias", "merge"} {
				t.Run(field+"/"+value+"/"+form, func(t *testing.T) {
					source := fmt.Sprintf("worker: &node\n  %s: %s\n  work.requested: {}\n", field, value)
					switch form {
					case "alias":
						source += "second: *node\n"
					case "merge":
						source += "second:\n  <<: *node\n"
					}
					snapshot, err := yamlsource.Load([]byte(source))
					if err != nil {
						t.Fatal(err)
					}
					nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
					requireNodeShapeCarrierRefusal(t, nodes, err, field)
				})
			}
		}
	}
}

func TestNodeShapeRetiredCarriersRejectDirectoryAndRetainedSource(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, field := range []string{"execution_type", "subscribes_to", "produces", "event_handlers"} {
		t.Run(field, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: retired-carrier\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested:\n")
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), fmt.Sprintf("worker:\n  %s: null\n  work.requested: {}\n", field))
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, ingress := range []string{"directory", "retained"} {
				t.Run(ingress, func(t *testing.T) {
					var bundle *WorkflowContractBundle
					var err error
					if ingress == "directory" {
						bundle, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
					} else {
						bundle, err = LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
					}
					if bundle != nil {
						t.Fatal("retired carrier produced a partial bundle")
					}
					requireNodeShapeCarrierRefusal(t, nil, err, field)
				})
			}
		})
	}
}

func requireNodeShapeCarrierRefusal(t *testing.T, nodes map[string]SystemNodeContract, err error, field string) {
	t.Helper()
	var diagnostic *LoaderDiagnostic
	if nodes != nil || !errors.As(err, &diagnostic) || diagnostic.Code != "contract_loader.undefined_field" ||
		!strings.Contains(diagnostic.Problem, fmt.Sprintf("node field %q", field)) ||
		!strings.Contains(err.Error(), "Handlers are written directly under the node, keyed by event name") ||
		!strings.Contains(err.Error(), "a node receives exactly the events it handles") ||
		diagnostic.Location.File != "nodes.yaml" || diagnostic.Location.Line != 2 ||
		!reflect.DeepEqual(diagnostic.ValidOptions, []string{"gate_state", "state_schema", "state_table", "timers"}) {
		t.Fatalf("retired carrier %q did not fail on presence with exact key, vocabulary and source: nodes=%v error=%v diagnostic=%+v", field, nodes, err, diagnostic)
	}
}
