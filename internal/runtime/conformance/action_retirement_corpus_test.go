package conformance

import (
	"bytes"

	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"

	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	yaml "gopkg.in/yaml.v3"
)

const actionRetirementCorpusPath = "internal/runtime/conformance/testdata/action_retirement_corpus_ledger.json"

// This is a lexical source/reference census, not a count of executable actions.
// Per-file dispositions and named proof rows must not be inferred from a match.
var actionRetirementCorpusPattern = regexp.MustCompile(`\b(create_flow_instance|record_evidence|mailbox_write|artifact_repo_commit|config_from|instance_id_from|evidence_target|[A-Za-z0-9_]*(ArtifactRepo|MailboxWrite|ActionSpec|ConfigFrom|EvidenceTarget|HandlerAction|ActionRegistry|ActionRunner|ActionExecution|EmitSurfaceAction|ActionInstruction)[A-Za-z0-9_]*)\b`)

var actionRetirementFieldLine = regexp.MustCompile(`(?m)^\s*(action|template|evidence_target|config_from|instance_id_from)\s*:`)

func TestActionRetirementCorpusHasNoLiveAuthoredActions(t *testing.T) {
	root := conformanceRepoRoot(t)
	for path, raw := range actionRetirementCorpusFiles(t, root, true) {
		if path == "tests/tier4-cross-entity/test-create-entity/nodes.yaml" {
			requireRetiredActionHistoricalFixture(t, root)
			continue
		}
		if filepath.Base(path) == "nodes.yaml" || filepath.Base(path) == "nodes.yml" {
			found, err := actionRetirementAuthoredFields(raw)
			if err != nil || len(found) != 0 {
				t.Errorf("live YAML %s: retired fields=%v err=%v", path, found, err)
			}
		}
		if !strings.HasPrefix(path, "internal/runtime/testfixtures/") || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			continue
		}
		files := token.NewFileSet()
		file, err := parser.ParseFile(files, path, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, ok := actionRetirementConstantString(literal)
			if !ok || !actionRetirementFieldLine.MatchString(text) {
				return true
			}
			found, err := actionRetirementAuthoredFields([]byte(text))
			if len(found) != 0 || err != nil {
				t.Errorf("live Go fixture %s:%d: retired fields=%v err=%v", path, files.Position(literal.Pos()).Line, found, err)
			}
			return true
		})
	}
}

func actionRetirementAuthoredFields(raw []byte) ([]string, error) {
	var doc yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&doc); err != nil && err != io.EOF {
		return nil, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("expected one complete YAML document, trailing content: %v", err)
	}
	var found []string
	seen := map[*yaml.Node]bool{}
	var handler func(*yaml.Node)
	var rows func(*yaml.Node)
	resolve := func(node *yaml.Node) *yaml.Node {
		aliases := map[*yaml.Node]bool{}
		for node != nil && node.Kind == yaml.AliasNode && !aliases[node] {
			aliases[node] = true
			node = node.Alias
		}
		return node
	}
	rows = func(node *yaml.Node) {
		node = resolve(node)
		if node == nil {
			return
		}
		if node.Kind == yaml.SequenceNode {
			for _, row := range node.Content {
				handler(row)
			}
			return
		}
		keyed := node.Kind == yaml.MappingNode && len(node.Content) != 0
		for i := 1; i < len(node.Content) && keyed; i += 2 {
			value := resolve(node.Content[i])
			keyed = value != nil && value.Kind == yaml.MappingNode
		}
		if keyed {
			for i := 1; i < len(node.Content); i += 2 {
				handler(node.Content[i])
			}
		} else {
			handler(node)
		}
	}
	handler = func(node *yaml.Node) {
		node = resolve(node)
		if node == nil || seen[node] || node.Kind != yaml.MappingNode {
			return
		}
		seen[node] = true
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i].Value, node.Content[i+1]
			if retiredHandlerField(key) {
				found = append(found, key)
			}
			switch key {
			case "rules":
				rows(value)
			case "on_complete":
				if resolved := resolve(value); resolved != nil && resolved.Kind == yaml.SequenceNode {
					rows(value)
				} else {
					handler(value)
				}
			case "on_success", "join", "timeout":
				handler(value)
			case "<<":
				if merged := resolve(value); merged != nil && merged.Kind == yaml.SequenceNode {
					for _, entry := range merged.Content {
						handler(entry)
					}
				} else {
					handler(value)
				}
			}
		}
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	root := resolve(doc.Content[0])
	if root == nil || root.Kind != yaml.MappingNode {
		return nil, nil
	}
	// Go fixture constructors also carry node-body and handler fragments.
	nodes := []*yaml.Node{root}
	rootIsNodeMap := false
	for i := 1; i < len(root.Content); i += 2 {
		candidate := resolve(root.Content[i])
		if candidate == nil || candidate.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(candidate.Content); j += 2 {
			rootIsNodeMap = rootIsNodeMap || candidate.Content[j].Value == "event_handlers"
		}
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if !rootIsNodeMap && retiredHandlerField(root.Content[i].Value) {
			handler(root)
		}
		nodes = append(nodes, root.Content[i+1])
	}
	for _, candidate := range nodes {
		node := resolve(candidate)
		if node == nil || node.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(node.Content); j += 2 {
			if node.Content[j].Value != "event_handlers" {
				continue
			}
			handlers := resolve(node.Content[j+1])
			if handlers == nil || handlers.Kind != yaml.MappingNode {
				continue
			}
			for k := 1; k < len(handlers.Content); k += 2 {
				handler(handlers.Content[k])
			}
		}
	}
	sort.Strings(found)
	return found, nil
}

func TestActionRetirementCorpusScannerPreservesHomonymsAndRejectsAliases(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want bool
	}{
		{"worker: {timers: [{action: expire}], event_handlers: {request: {activity: {tool: run, input: {action: payload.action}}, rules: {action: {else: true, emit: done}}}}}", false},
		{"worker: {timers: [{action: expire}], event_handlers: {request: {activity: {tool: run, input: {action: payload.action}}, rules: [{when: payload.action == 'ready', emit: done}]}}}", false},
		{"template: {event_handlers: {request: {emit: done}}}\naction: {event_handlers: {}}", false},
		{"worker: {event_handlers: {request: {action: null}}}", true},
		{"action: new_unknown_action", true},
		{"event_handlers: {request: {on_complete: [{action: ''}]}}", true},
		{"old: &old {config_from: {}}\nworker: {event_handlers: {request: {rules: [*old]}}}", true},
		{"worker: {event_handlers: {request: {join: {timeout: {instance_id_from: ''}}}}}", true},
		{"worker: {event_handlers: {request: {<<: [{evidence_target: null}]}}}", true},
	} {
		got, err := actionRetirementAuthoredFields([]byte(test.raw))
		if err != nil || (len(got) != 0) != test.want {
			t.Fatalf("%s: fields=%v err=%v wantRetired=%v", test.raw, got, err, test.want)
		}
	}
}

func TestActionRetirementCorpusScannerRejectsTruncatedFragments(t *testing.T) {
	for _, raw := range []string{
		"    previous.event:\n      emit: done\nworker:\n  event_handlers:\n    request:\n      action: create_flow_instance\n",
		"worker: {}\n---\nworker: {event_handlers: {request: {action: unknown}}}\n",
	} {
		if _, err := actionRetirementAuthoredFields([]byte(raw)); err == nil {
			t.Fatalf("scanner silently ignored trailing authored content: %s", raw)
		}
	}
}

func actionRetirementCurrentCorpus(t *testing.T, root string) map[string][]byte {
	return actionRetirementCorpusFiles(t, root, false)
}

func TestActionRetirementCorpusExcludesGeneratedTestResults(t *testing.T) {
	root := t.TempDir()
	generated := filepath.Join(root, "test-results", "plan")
	if err := os.MkdirAll(generated, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(generated, "proof-plan.json"), []byte(`{"name":"ActionRunner"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.md"), []byte("ActionRunner"), 0o644); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(root, "nested", "foreign")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "source.md"), []byte("ActionRunner"), 0o644); err != nil {
		t.Fatal(err)
	}
	current := actionRetirementCurrentCorpus(t, root)
	if len(current) != 1 || current["source.md"] == nil {
		t.Fatalf("generated results or foreign checkout entered the source corpus: %v", current)
	}
	if err := os.Remove(filepath.Join(root, "source.md")); err != nil {
		t.Fatal(err)
	}
	if current := actionRetirementCurrentCorpus(t, root); len(current) != 0 {
		t.Fatalf("foreign-only authored source received corpus credit: %v", current)
	}
}

func actionRetirementCorpusFiles(t *testing.T, root string, allAuthored bool) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" || entry.Name() == "test-results" {
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(path) {
		case ".go", ".yaml", ".yml", ".json", ".md":
		default:
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == actionRetirementCorpusPath {
			return nil
		}
		if allAuthored && filepath.Base(path) != "nodes.yaml" && filepath.Base(path) != "nodes.yml" && !(strings.HasPrefix(relative, "internal/runtime/testfixtures/") && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if allAuthored || actionRetirementCorpusPattern.Match(raw) {
			out[relative] = raw
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRetiredActionHistoricalFixtureFailsClosed(t *testing.T) {
	requireRetiredActionHistoricalFixture(t, conformanceRepoRoot(t))
}

func requireRetiredActionHistoricalFixture(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "tests/tier4-cross-entity/test-create-entity")
	raw, err := os.ReadFile(filepath.Join(path, "tests/expected.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Conformance struct {
			Disposition string                               `yaml:"disposition"`
			Retirement  struct{ Reason, Replacement string } `yaml:"retirement"`
		} `yaml:"conformance"`
	}
	if err := yaml.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if expected.Conformance.Disposition != "retired" || expected.Conformance.Retirement.Reason == "" || expected.Conformance.Retirement.Replacement != "tests/tier11-flow-composition/test-dynamic-flow-instance" {
		t.Fatalf("historical fixture lost exact explicit retirement: %#v", expected)
	}
	if _, err := contracts.LoadWorkflowContractBundleWithOverrides(root, path, contracts.DefaultPlatformSpecFile(root)); err == nil || !strings.Contains(err.Error(), "handler field \"action\" is not supported") {
		t.Fatalf("historical action fixture became executable: %v", err)
	}
}
