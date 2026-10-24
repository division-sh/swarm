package contracts

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

// Invariant: only declared handler keys create consumers; reach: disk and retained source,
// including retired node carriers, wildcard spelling and reserved-field collisions.
func TestNodeHandlerEventAdmissionDirectoryAndRetainedSource(t *testing.T) {
	for _, tc := range []struct {
		name, events, body, want string
	}{
		{"empty handler", "work.requested:\n", "work.requested: {}", ""},
		{"dotless", "request:\n", "request: {}", ""},
		{"declared id event is not metadata", "id:\n", "id: {}", ""},
		{"wildcard prefix", "work.requested:\n", "'work.*': {}", ""},
		{"wildcard suffix", "work.requested:\n", "'*.requested': {}", ""},
		{"wildcard all", "work.requested:\n", "'*': {}", ""},
		{"undeclared", "work.requested:\n", "other.requested: {}", "undeclared event"},
		{"misspelled field", "work.requested:\n", "state_schma: {}", "undeclared event"},
		{"unmatched wildcard", "work.requested:\n", "'other.*': {}", "undeclared event"},
		{"unsupported wildcard", "work.requested:\n", "'work.pre*': {}", "supported wildcard"},
		{"reserved timer", "timers:\nwork.completed:\n", "timers: {emit: work.completed}", "is a node field, not an event"},
		{"retired state schema", "state_schema:\nwork.completed:\n", "state_schema: {emit: work.completed}", "not supported"},
		{"retired state table", "state_table:\nwork.completed:\n", "state_table: null", "not supported"},
		{"retired gate", "gate_state:\nwork.completed:\n", "gate_state: {emit: {event: work.completed}}", "not supported"},
		{"retired subscription", "work.requested:\n", "subscribes_to: null", "Handlers are written directly"},
		{"retired execution type", "work.requested:\n", "execution_type: null", "Handlers are written directly"},
		{"retired produces", "work.requested:\n", "produces: null", "Handlers are written directly"},
		{"retired container", "work.requested:\n", "event_handlers: null", "Handlers are written directly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: handler-admission\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), tc.events)
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  "+tc.body+"\n")
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, source := range []struct {
				name string
				load func() (*WorkflowContractBundle, error)
			}{
				{"directory", func() (*WorkflowContractBundle, error) {
					return LoadWorkflowContractBundleWithOverrides(contractRepoRoot(t), root, DefaultPlatformSpecFile(contractRepoRoot(t)))
				}},
				{"retained", func() (*WorkflowContractBundle, error) {
					return LoadWorkflowContractBundleFromArtifact(contractRepoRoot(t), retained, DefaultPlatformSpecFile(contractRepoRoot(t)), WorkflowContractLoadOptions{})
				}},
			} {
				t.Run(source.name, func(t *testing.T) {
					bundle, err := source.load()
					if tc.want != "" {
						var diagnostic *LoaderDiagnostic
						located := strings.Contains(fmt.Sprint(err), "nodes.yaml:2:")
						if errors.As(err, &diagnostic) {
							located = diagnostic.Location.File == "nodes.yaml" && diagnostic.Location.Line == 2 && diagnostic.Location.Column > 0
						}
						if bundle != nil || (!errors.Is(err, ErrInvalidField) && diagnostic == nil) || !strings.Contains(fmt.Sprint(err), tc.want) || !located {
							t.Fatalf("bundle=%v error=%v, want %s at nodes.yaml:2", bundle != nil, err, tc.want)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if len(bundle.Nodes["worker"].EventHandlers) != 1 {
						t.Fatalf("empty handler lost: %#v", bundle.Nodes["worker"])
					}
				})
			}
		})
	}
}
