package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func retainedLocationFixture(t testing.TB, name, body string) *WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	file := filepath.Join(root, name)
	if err := os.WriteFile(file, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	return &WorkflowContractBundle{SourceArtifact: artifact}
}

func TestNodeAdmissionAliasMergeAndNestedCoordinates(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		line, column int
	}{
		{"alias", "first: &node\n  event_handlers:\n    task.ready:\n      emit: task.done\nsecond: *node\n", 5, 9},
		{"merge", "first: &node\n  event_handlers:\n    task.ready:\n      emit: task.done\nsecond:\n  <<: *node\n", 6, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
			if err != nil {
				t.Fatal(err)
			}
			if nodes["second"].EventHandlers["task.ready"].Emit.Event != "task.done" {
				t.Fatal("alias/merge lost the executed nested effect")
			}
			bundle := retainedLocationFixture(t, "nodes.yaml", tc.source)
			proof, found := bundle.IntroductionLocation("nodes.yaml", `second.event_handlers["task.ready"].emit`, false)
			if !found || proof.File != "nodes.yaml" || proof.Line != tc.line || proof.Column != tc.column {
				t.Fatalf("authored introduction coordinate: %#v", proof)
			}
		})
	}
	for _, source := range []string{
		"worker:\n  event_handlers:\n    task.ready:\n      emit: {event: task.done, event: task.other}\n",
		"worker:\n  event_handlers:\n    task.ready:\n      emit:\n        event: task.done\n        fields: {value: {nested: 1, nested: 2}}\n",
		"worker: &node\n  event_handlers:\n    task.ready:\n      emit:\n        event: task.done\n        fields: *node\n",
	} {
		snapshot, err := yamlsource.Load([]byte(source))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root()); err == nil || !strings.Contains(err.Error(), "nodes.yaml:") {
			t.Fatalf("nested duplicate/cycle lost source-located rejection: %v", err)
		}
	}
	bundle := retainedLocationFixture(t, "nodes.yaml", "worker: {event_handlers: {}}\n")
	for _, slot := range []string{"missing", "worker.absent", "worker[0]", "worker.", `worker["unterminated]`} {
		if location, found := bundle.IntroductionLocation("nodes.yaml", slot, false); found || location != (yamlsource.Location{}) {
			t.Fatalf("unresolved diagnostic slot %q invented a position: %+v", slot, location)
		}
	}
	for _, source := range []*WorkflowContractBundle{nil, {}, bundle} {
		if location, found := source.IntroductionLocation("absent.yaml", "worker", false); found || location != (yamlsource.Location{}) {
			t.Fatalf("missing retained source invented a position: %+v", location)
		}
	}
}
