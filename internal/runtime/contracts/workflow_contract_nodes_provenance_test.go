package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNodeAdmissionProvenanceComposesWithEvents(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := filepath.Join(repo, "examples", "routing", "template-create-minted-key")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var nodeFound, eventFound bool
	for _, entry := range bundle.EffectiveProvenance().Entries() {
		if strings.HasPrefix(entry.Path, `nodes["producer:producer-node"].event_handlers["validation.triggered"].emit.fields.candidate`) {
			nodeFound = entry.Provenance.Origin == EffectiveValueOriginAuthored &&
				strings.HasSuffix(entry.Provenance.SourceFile, "producer/nodes.yaml") && entry.Provenance.SourceLine == 10
		}
		if strings.HasPrefix(entry.Path, "events[") {
			eventFound = true
		}
	}
	if !nodeFound || !eventFound {
		t.Fatalf("composed provenance missing node or event entry: node=%t event=%t", nodeFound, eventFound)
	}
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
			proof := nodes["second"].admissionProvenance[`event_handlers["task.ready"].emit`]
			if proof.SourceFile != "nodes.yaml" || proof.SourceLine != tc.line || proof.SourceColumn != tc.column {
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
}
