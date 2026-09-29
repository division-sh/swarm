package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestReviewer2492WholeDocumentExpansion(t *testing.T) {
	for _, tc := range []struct {
		name         string
		declarations int
		merge        bool
		depth        int
		active       bool
		wantReject   bool
	}{
		{"single-control", 1, false, 11, true, false},
		{"under-budget-alias", 4, false, 11, true, false},
		{"under-budget-merge", 4, true, 11, true, false},
		{"repeated-alias", 16, false, 11, true, true},
		{"repeated-merge", 16, true, 11, true, true},
		{"annotation-only-alias", 16, false, 24, false, false},
		{"annotation-only-merge", 16, true, 24, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var source strings.Builder
			source.WriteString("worker0: &worker\n  execution_type: system_node\n  event_handlers:\n    task.ready:\n      _note:\n        a0: &a0 [x, x]\n")
			for i := 1; i <= tc.depth; i++ {
				fmt.Fprintf(&source, "        a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
			}
			if tc.active {
				fmt.Fprintf(&source, "      emit:\n        event: task.done\n        fields:\n          data: *a%d\n", tc.depth)
			}
			for i := 1; i < tc.declarations; i++ {
				if tc.merge {
					fmt.Fprintf(&source, "worker%d: {<<: *worker}\n", i)
				} else {
					fmt.Fprintf(&source, "worker%d: *worker\n", i)
				}
			}
			if tc.name == "repeated-alias" && source.Len() != 755 || tc.name == "repeated-merge" && source.Len() != 845 {
				t.Fatalf("original reviewer source changed: %d bytes", source.Len())
			}
			snapshot, err := yamlsource.Load([]byte(source.String()))
			if err != nil {
				t.Fatal(err)
			}
			root := snapshot.Document("nodes.yaml").Root()
			fields, err := root.Mapping()
			if err != nil {
				t.Fatal(err)
			}
			var annotations []yamlsource.Value
			for _, field := range fields {
				notes, err := nodeHandlerAnnotations(field.Value)
				if err != nil {
					t.Fatal(err)
				}
				annotations = append(annotations, notes...)
			}
			guardErr := root.ValidateExpansion(annotations...)
			if tc.wantReject != (guardErr != nil) {
				t.Fatalf("source-owner control: %v", guardErr)
			}
			directory := t.TempDir()
			path := filepath.Join(directory, "nodes.yaml")
			if err := os.WriteFile(path, []byte(source.String()), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "schema.yaml"), []byte("name: test\n"), 0600); err != nil {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(directory)
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
			retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			loaders := map[string]func() (map[string]SystemNodeContract, error){
				"direct": func() (map[string]SystemNodeContract, error) { return projectNodeDeclarationsValue(root) },
				"file":   func() (map[string]SystemNodeContract, error) { return loadOptionalNodeDeclarations(path) },
			}
			for name, admitted := range map[string]*sourceartifact.AdmittedSourceArtifact{"admitted": artifact, "catalog": catalog, "retained": retained} {
				loaders[name] = func() (map[string]SystemNodeContract, error) {
					return loadOptionalNodeDeclarationsFromSource(admitted, "nodes.yaml")
				}
			}
			for name, load := range loaders {
				t.Run(name, func(t *testing.T) {
					nodes, err := load()
					entries := 0
					for _, node := range nodes {
						entries += len(node.admissionProvenance)
					}
					t.Logf("source bytes=%d declarations=%d provenance=%d admission=%v", source.Len(), tc.declarations, entries, err)
					if tc.wantReject {
						if err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") || !strings.Contains(err.Error(), "$ at ") || !strings.Contains(err.Error(), "nodes.yaml:1:1") || len(nodes) != 0 {
							t.Fatalf("aggregate source must reject before projection with document coordinates: %v", err)
						}
						return
					}
					if err != nil || len(nodes) != tc.declarations {
						t.Fatalf("valid declarations must survive: %d, %v", len(nodes), err)
					}
					wantEntries := 8199 * tc.declarations
					if !tc.active {
						wantEntries = 5 * tc.declarations
					}
					if entries != wantEntries || entries > yamlsource.MaxExpandedNodes {
						t.Fatalf("provenance entries = %d, want %d under document limit", entries, wantEntries)
					}
					if !tc.active {
						for _, node := range nodes {
							if note := node.admissionProvenance["event_handlers[\"task.ready\"]._note"]; note.Origin != EffectiveValueOriginAuthored || note.SourceLine == 0 {
								t.Fatal("annotation outer occurrence lost")
							}
						}
					}
				})
			}
		})
	}
}
