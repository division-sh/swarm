package canonicalrouting

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	RootIngress             = "root-ingress"
	ParentConnect           = "parent-connect"
	TemplateSelectExisting  = "template-select-existing"
	TemplateSelectOrCreate  = "template-select-or-create"
	TemplateReply           = "template-reply"
	TemplateCreateMintedKey = "template-create-minted-key"
)

// ExampleRoot returns the checked-in positive authoring owner for a routing pattern.
func ExampleRoot(t testing.TB, name string) string {
	t.Helper()
	return filepath.Join(RepoRoot(t), "examples", "routing", name)
}

// CopyExample materializes a canonical example for a focused overlay or negative mutation.
func CopyExample(t testing.TB, name string) string {
	t.Helper()
	target := t.TempDir()
	CopyTree(t, ExampleRoot(t, name), target)
	return target
}

func CopyTree(t testing.TB, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, contents, 0o644)
	})
	if err != nil {
		t.Fatalf("copy canonical routing example %s: %v", source, err)
	}
}

func ReplaceFile(t testing.TB, path, old, replacement string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(contents), old) {
		t.Fatalf("canonical mutation target missing in %s", path)
	}
	updated := strings.Replace(string(contents), old, replacement, 1)
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func WriteFile(t testing.TB, root, relativePath, contents string) {
	t.Helper()
	path := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// MergeMappingFile appends non-conflicting top-level entries to a copied
// canonical artifact. Existing entries and routing declarations are rejected.
func MergeMappingFile(t testing.TB, root, relativePath, additions string) {
	t.Helper()
	path := filepath.Join(root, relativePath)
	doc := readYAMLDocument(t, path)
	existing := requireYAMLMapping(t, path, doc.Content[0])

	decoder := yaml.NewDecoder(bytes.NewBufferString(strings.TrimLeft(additions, "\n")))
	var additionDoc yaml.Node
	if err := decoder.Decode(&additionDoc); err != nil {
		t.Fatalf("parse mapping additions for %s: %v", path, err)
	}
	if len(additionDoc.Content) != 1 {
		t.Fatalf("mapping additions for %s must contain one YAML document", path)
	}
	var extraDoc yaml.Node
	if err := decoder.Decode(&extraDoc); err != io.EOF {
		if err != nil {
			t.Fatalf("parse mapping additions for %s: %v", path, err)
		}
		t.Fatalf("mapping additions for %s must contain one YAML document", path)
	}
	additionRoot := additionDoc.Content[0]
	if yamlNodeContainsAuthoredRouting(additionRoot) {
		t.Fatalf("mapping additions for %s contain routing declarations; consume a canonical route or use an explicitly classified negative mutation", path)
	}
	additionsByKey := requireYAMLMapping(t, path, additionRoot)
	for key := range additionsByKey {
		if _, exists := existing[key]; exists {
			t.Fatalf("mapping addition %q already exists in %s", key, path)
		}
	}
	doc.Content[0].Content = append(doc.Content[0].Content, additionRoot.Content...)
	writeYAMLDocument(t, path, doc)
}

func readYAMLDocument(t testing.TB, path string) *yaml.Node {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(doc.Content) != 1 {
		t.Fatalf("%s must contain one YAML document", path)
	}
	return &doc
}

func writeYAMLDocument(t testing.TB, path string, doc *yaml.Node) {
	t.Helper()
	raw, err := yaml.Marshal(doc.Content[0])
	if err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func requireYAMLMapping(t testing.TB, path string, node *yaml.Node) map[string]*yaml.Node {
	t.Helper()
	if node == nil || node.Kind != yaml.MappingNode {
		t.Fatalf("%s node must be a mapping", path)
	}
	result := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key := yamlScalar(node.Content[i])
		if key == "" {
			t.Fatalf("%s mapping key at index %d must be a scalar", path, i)
		}
		if _, exists := result[key]; exists {
			t.Fatalf("%s mapping key %q is duplicated", path, key)
		}
		result[key] = node.Content[i+1]
	}
	return result
}

func yamlScalar(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.ScalarNode {
		return ""
	}
	return strings.TrimSpace(node.Value)
}

func yamlNodeContainsAuthoredRouting(node *yaml.Node) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "source":
				if value.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(value.Value), "external") {
					return true
				}
			case "broadcast":
				if value.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(value.Value), "true") {
					return true
				}
			case "pins", "connect", "resolution", "instance", "delivery", "address", "target",
				"on_missing", "on_conflict", "select_entity", "select_or_create_entity", "create_flow_instance",
				"subscriptions", "subscriptions_bootstrap", "subscribes_to", "produces", "emit_events",
				"event_handlers", "emit", "fan_out", "auto_emit_on_create", "replies_to", "carries":
				return true
			}
			if yamlNodeContainsAuthoredRouting(value) {
				return true
			}
		}
		return false
	}
	for _, child := range node.Content {
		if yamlNodeContainsAuthoredRouting(child) {
			return true
		}
	}
	return false
}

func RepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
