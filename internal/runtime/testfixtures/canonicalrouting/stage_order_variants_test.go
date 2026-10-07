package canonicalrouting

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestStageOrderedSourceRenameChangesOnlyMetadata(t *testing.T) {
	root := CopyRootIngressLegacyTemplateTargetRoute(t)
	path := filepath.Join(root, "schema.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	RenameStoppedRunReadinessSource(t, root)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Replace(before, []byte("name: routing-root-ingress\n"), []byte("name: audit2277-new-source\n"), 1)
	if !bytes.Equal(after, expected) {
		t.Fatal("metadata-only source replacement altered lifecycle declarations")
	}
}

func TestStageOrderedFixtureConsumersUseClosedMutationOwner(t *testing.T) {
	for _, file := range []string{
		"internal/serveapp/issue2277_audit_probe_test.go",
		"internal/serveapp/selected_fork_mailbox_controls_test.go",
	} {
		raw, err := os.ReadFile(filepath.Join(RepoRoot(t), file))
		if err != nil {
			t.Fatal(err)
		}
		if operation, err := stageFixtureYAMLReparser(raw); err != nil || operation != "" {
			t.Fatalf("%s bypasses the closed ordered mutation owner: operation=%s error=%v", file, operation, err)
		}
	}
}

func TestStageOrderedFixtureConsumerGuardRejectsReparsing(t *testing.T) {
	for _, operation := range []string{"Unmarshal", "Marshal", "NewDecoder", "NewEncoder"} {
		for _, alias := range []string{"yaml", "sourceYAML", "."} {
			call := alias + "." + operation
			if alias == "." {
				call = operation
			}
			raw := []byte("package fixture\nimport " + alias + " \"gopkg.in/yaml.v3\"\nfunc mutate() { " + call + "(nil) }\n")
			if found, err := stageFixtureYAMLReparser(raw); err != nil || found != "gopkg.in/yaml.v3" {
				t.Fatalf("guard missed %s.%s: found=%s error=%v", alias, operation, found, err)
			}
		}
	}
	if found, err := stageFixtureYAMLReparser([]byte("package fixture\nimport \"encoding/json\"\nfunc data() { json.Marshal(nil) }\n")); err != nil || found != "" {
		t.Fatalf("unrelated data serialization was rejected: %s %v", found, err)
	}
}

func stageFixtureYAMLReparser(raw []byte) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", raw, parser.ImportsOnly)
	if err != nil {
		return "", err
	}
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return "", err
		}
		if path == "gopkg.in/yaml.v3" {
			return path, nil
		}
	}
	return "", nil
}
