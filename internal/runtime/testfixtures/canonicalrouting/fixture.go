package canonicalrouting

import (
	"bytes"
	"fmt"
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
	RootIngress             ArtifactID = "root-ingress"
	ParentConnect           ArtifactID = "parent-connect"
	TemplateSelectExisting  ArtifactID = "template-select-existing"
	TemplateSelectOrCreate  ArtifactID = "template-select-or-create"
	TemplateReply           ArtifactID = "template-reply"
	TemplateCreateMintedKey ArtifactID = "template-create-minted-key"
	FanInStream             ArtifactID = "fan-in/stream"
	FanInBarrier            ArtifactID = "fan-in/barrier"
)

// ArtifactID is a checked-in routing artifact identity. The ownership guard
// accepts only IDs declared in artifact_registry.yaml.
type ArtifactID string

// SourceID identifies one explicitly classified non-bundle source constructor.
// Complete routing bundles are owned by ArtifactID and cannot use SourceID as
// an exception from that boundary.
type SourceID string

// SourceToken proves that ExecuteSource ran inside a classified source
// constructor. Its fields are private so a test cannot replace execution with
// a bare ID claim.
type SourceToken struct {
	id   SourceID
	seal *sourceTokenSeal
}

type sourceTokenSeal struct{}

var executedSourceTokenSeal = &sourceTokenSeal{}

// RetiredStaticMutation identifies one deliberately invalid legacy routing
// shape. The closed set prevents negative tests from becoming an arbitrary
// positive bundle-construction API.
type RetiredStaticMutation string

const (
	RetiredStaticCreate             RetiredStaticMutation = "create_entity"
	RetiredStaticSelect             RetiredStaticMutation = "select_entity"
	RetiredStaticSelectOrCreate     RetiredStaticMutation = "select_or_create_entity"
	RetiredStaticMissingAcquisition RetiredStaticMutation = "missing_acquisition"
)

// ParserSnippet bounds parser-focused YAML to one document. Its source remains
// ordinary package evidence for the complete-bundle ownership guard.
type ParserSnippet struct {
	document yaml.Node
}

func NewParserSnippet(t testing.TB, source string) ParserSnippet {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(source), &doc); err != nil {
		t.Fatalf("parse routing snippet: %v", err)
	}
	return ParserSnippet{document: doc}
}

// Decode supplies one parsed document to a parser contract under test. It does
// not exempt the source document from canonical fixture ownership checks.
func (snippet ParserSnippet) Decode(target any) error {
	if len(snippet.document.Content) != 1 {
		return fmt.Errorf("routing snippet must contain one YAML document")
	}
	return snippet.document.Content[0].Decode(target)
}

// ExampleRoot returns the checked-in positive authoring owner for a routing pattern.
func ExampleRoot(t testing.TB, id ArtifactID) string {
	t.Helper()
	root, ok := canonicalExamplePath(id)
	if !ok {
		t.Fatalf("routing artifact %q is not a canonical positive fixture owner", id)
	}
	return filepath.Join(RepoRoot(t), filepath.FromSlash(root))
}

// CopyExample materializes a canonical example for a focused overlay or negative mutation.
func CopyExample(t testing.TB, id ArtifactID) string {
	t.Helper()
	target := t.TempDir()
	copyTree(t, ExampleRoot(t, id), target)
	return target
}

// Prove declares that the calling TestXxx entrypoint executes the checked-in
// artifact. The ownership guard requires this call directly in the test body.
func Prove(t testing.TB, ids ...ArtifactID) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("canonical routing proof must name at least one artifact")
	}
	for _, id := range ids {
		root := checkedArtifactRoot(t, id)
		if _, err := os.Stat(filepath.Join(root, "package.yaml")); err != nil {
			t.Fatalf("routing artifact %q: %v", id, err)
		}
	}
}

// ExecuteSource runs a classified non-bundle source constructor body and
// returns its sealed token. The source function must return this token; proof
// tests directly execute that source function before calling ProveSource.
func ExecuteSource(t testing.TB, id SourceID, constructor func()) SourceToken {
	t.Helper()
	if strings.TrimSpace(string(id)) == "" {
		t.Fatal("canonical routing source ID must not be empty")
	}
	if constructor == nil {
		t.Fatal("canonical routing source constructor must not be nil")
	}
	constructor()
	return SourceToken{id: id, seal: executedSourceTokenSeal}
}

// ProveSource accepts only tokens returned by executed source constructors.
// The ownership guard binds each token to one exact source function and one
// direct executable TestXxx consumer.
func ProveSource(t testing.TB, tokens ...SourceToken) {
	t.Helper()
	if len(tokens) == 0 {
		t.Fatal("canonical routing source proof must name at least one source")
	}
	for _, token := range tokens {
		if token.seal != executedSourceTokenSeal || strings.TrimSpace(string(token.id)) == "" {
			t.Fatal("canonical routing source proof requires an executed source token")
		}
	}
}

func checkedArtifactRoot(t testing.TB, id ArtifactID) string {
	t.Helper()
	if canonical, ok := canonicalExamplePath(id); ok {
		return filepath.Join(RepoRoot(t), filepath.FromSlash(canonical))
	}
	root := filepath.ToSlash(filepath.Clean(strings.TrimSpace(string(id))))
	if root == "." || filepath.IsAbs(root) || strings.HasPrefix(root, "../") {
		t.Fatalf("invalid routing artifact ID %q", id)
	}
	if !strings.Contains(root, "/") {
		root = filepath.ToSlash(filepath.Join("examples", "routing", root))
	}
	return filepath.Join(RepoRoot(t), filepath.FromSlash(root))
}

func canonicalExamplePath(id ArtifactID) (string, bool) {
	requested := filepath.ToSlash(filepath.Clean(strings.TrimSpace(string(id))))
	for _, canonical := range []ArtifactID{
		RootIngress,
		ParentConnect,
		TemplateSelectExisting,
		TemplateSelectOrCreate,
		TemplateReply,
		TemplateCreateMintedKey,
		FanInStream,
		FanInBarrier,
	} {
		root := filepath.ToSlash(filepath.Join("examples", "routing", string(canonical)))
		if requested == string(canonical) || requested == root {
			return root, true
		}
	}
	return "", false
}

func copyTree(t testing.TB, source, target string) {
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

func applyClosedReplacement(t testing.TB, path, old, replacement string) {
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

func writeFixtureFile(path, source string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(source), 0o644)
}

// duplicateFlowForNegativeMutation creates a second receiver solely for a
// fail-closed topology test. Positive fixture construction must use ArtifactID.
func duplicateFlowForNegativeMutation(t testing.TB, root, sourceFlowID, targetFlowID string) {
	t.Helper()
	for label, value := range map[string]string{"source": sourceFlowID, "target": targetFlowID} {
		value = strings.TrimSpace(value)
		if value == "" || filepath.Base(value) != value || value == "." || value == ".." {
			t.Fatalf("negative mutation %s flow ID %q is invalid", label, value)
		}
	}
	copyTree(
		t,
		filepath.Join(root, "flows", sourceFlowID),
		filepath.Join(root, "flows", targetFlowID),
	)
}

// AddRetiredStaticFlowForNegativeMutation adds one closed legacy-static
// invalidity to a copied canonical artifact.
func AddRetiredStaticFlowForNegativeMutation(t testing.TB, root string, mutation RetiredStaticMutation) {
	t.Helper()
	handler := ""
	switch mutation {
	case RetiredStaticCreate:
		handler = "      create_entity: true\n"
	case RetiredStaticSelect:
		handler = "      select_entity:\n        by:\n          legacy_id: payload.legacy_id\n"
	case RetiredStaticSelectOrCreate:
		handler = "      select_or_create_entity:\n        by:\n          legacy_id: payload.legacy_id\n"
	case RetiredStaticMissingAcquisition:
	default:
		t.Fatalf("unsupported retired static mutation %q", mutation)
	}
	applyClosedReplacement(t, filepath.Join(root, "package.yaml"),
		"  - id: account\n    flow: account\n    mode: template\n",
		"  - id: account\n    flow: account\n    mode: template\n  - id: legacy_static\n    flow: legacy_static\n    mode: static\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedNegativeFile(t, root, "flows/legacy_static/"+file, "{}\n")
	}
	writeClosedNegativeFile(t, root, "flows/legacy_static/schema.yaml", `name: legacy_static
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events:
      - name: legacy_seen
        event: legacy.seen
  outputs:
    events: []
`)
	writeClosedNegativeFile(t, root, "flows/legacy_static/events.yaml", `legacy.seen:
  legacy_id: text
  amount: number
`)
	writeClosedNegativeFile(t, root, "flows/legacy_static/entities.yaml", `legacy_record:
  legacy_id:
    type: text
    indexed: true
  amount:
    type: number
    initial: 0
`)
	writeClosedNegativeFile(t, root, "flows/legacy_static/nodes.yaml", `legacy-writer:
  id: legacy-writer
  execution_type: system_node
  subscribes_to: [legacy.seen]
  event_handlers:
    legacy.seen:
`+handler+`      data_accumulation:
        writes:
          - source_field: amount
            target_field: amount
`)
}

// AddRootDefaultEntityIDForNegativeMutation adds the one closed root-static
// caller-selected identity invalidity used by the retirement proof.
func AddRootDefaultEntityIDForNegativeMutation(t testing.TB, root string) {
	t.Helper()
	writeClosedNegativeFile(t, root, "entities.yaml", "subject:\n  display_name: text\n")
	writeClosedNegativeFile(t, root, "events.yaml", "subject.created:\n  entity_id: text\n  display_name: text\n")
	writeClosedNegativeFile(t, root, "schema.yaml", `name: template-select-or-create
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - name: subject_created
        event: subject.created
  outputs:
    events: []
`)
	writeClosedNegativeFile(t, root, "nodes.yaml", `root-node:
  id: root-node
  execution_type: system_node
  subscribes_to: [subject.created]
  event_handlers:
    subject.created:
      data_accumulation:
        writes:
          - source_field: display_name
            target_field: display_name
`)
}

func writeClosedNegativeFile(t testing.TB, root, relativePath, source string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatalf("write closed negative fixture %s: %v", path, err)
	}
}

func AddOverlayFile(t testing.TB, root, relativePath, source string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("overlay file %s already exists", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("inspect overlay file %s: %v", path, err)
	}
	rejectRoutingOverlay(t, path, source)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(source, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// SetOverlayFile creates or replaces a non-routing artifact file.
func SetOverlayFile(t testing.TB, root, relativePath, source string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	rejectRoutingOverlay(t, path, source)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(source, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// ApplyOverlay appends non-conflicting top-level entries to a copied
// canonical artifact. Existing entries and routing declarations are rejected.
func ApplyOverlay(t testing.TB, root, relativePath, additions string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
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
		t.Fatalf("overlay for %s contains routing declarations; consume a canonical route or use an explicit negative mutation", path)
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

func rejectRoutingOverlay(t testing.TB, path, source string) {
	t.Helper()
	if err := routingOverlayError(source); err != nil {
		t.Fatalf("overlay for %s: %v", path, err)
	}
}

func routingOverlayError(source string) error {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(strings.TrimLeft(source, "\n")), &doc); err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}
	if len(doc.Content) == 1 && yamlNodeContainsAuthoredRouting(doc.Content[0]) {
		return fmt.Errorf("contains routing declarations; consume a canonical route or use an explicit negative mutation")
	}
	return nil
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
	return yamlNodeContainsAuthoredRoutingAt(node, "")
}

func yamlNodeContainsAuthoredRoutingAt(node *yaml.Node, parentKey string) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "source":
				if parentKey == "events" || parentKey == "swarm" ||
					value.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(value.Value), "external") {
					return true
				}
			case "consumer":
				if parentKey == "swarm" {
					return true
				}
			case "inputs", "outputs":
				if parentKey == "pins" || parentKey == "requires" || parentKey == "bind" {
					return true
				}
			case "mode":
				if value.Kind == yaml.ScalarNode {
					mode := strings.ToLower(strings.TrimSpace(value.Value))
					if mode == "static" || mode == "template" {
						return true
					}
				}
			case "pins", "connect", "resolution", "instance", "delivery", "address", "target",
				"on_missing", "on_conflict", "select_entity", "select_or_create_entity", "create_flow_instance",
				"subscriptions", "subscriptions_bootstrap", "subscribes_to", "produces", "emit_events",
				"event_handlers", "emit", "fan_out", "auto_emit_on_create", "replies_to", "carries",
				"broadcast", "flows", "bind", "requires":
				return true
			}
			if yamlNodeContainsAuthoredRoutingAt(value, key) {
				return true
			}
		}
		return false
	}
	for _, child := range node.Content {
		if yamlNodeContainsAuthoredRoutingAt(child, parentKey) {
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
