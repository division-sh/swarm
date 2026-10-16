package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestPolicyCaseRepeatedSelectorWithinTupleIsRefused2287(t *testing.T) {
	for _, entrance := range []string{"projector", "handler", "bundle"} {
		t.Run(entrance, func(t *testing.T) {
			for _, repeated := range []bool{false, true} {
				second := "payload.target"
				if repeated {
					second = "payload.mode"
				}
				row := "selectors: [payload.mode, " + second + "], equals: [gold, gold]"
				var err error
				switch entrance {
				case "projector":
					snapshot, loadErr := yamlsource.Load([]byte("{" + row + "}\n"))
					if loadErr != nil {
						t.Fatal(loadErr)
					}
					_, _, err = projectNodePolicyCaseValue(snapshot.Document("nodes.yaml").Root())
				case "handler":
					var handler SystemNodeEventHandler
					err = decodeNodeTestYAML([]byte("rules:\n  - case: {"+row+"}\n    emit: task.done\n  - emit: task.other\n"), &handler)
				case "bundle":
					root := t.TempDir()
					writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: case_tuple\nstages: {done: {final: true}}\n")
					writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\n  mode: text\n  target: text\ntask.done:\ntask.other:\n")
					writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - case: {"+row+"}\n          emit: task.done\n        - emit: task.other\n")
					repo := repoRootForContractsTest(t)
					_, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				}
				if !repeated {
					if err != nil {
						t.Fatal("unique-selector positive control refused", err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), "duplicate") {
					t.Fatalf("repeated-selector tuple admitted instead of duplicate refusal: %v", err)
				}
			}
		})
	}
}
