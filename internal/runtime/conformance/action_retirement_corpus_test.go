package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

const actionRetirementCorpusPath = "internal/runtime/conformance/testdata/action_retirement_corpus_ledger.json"
const actionRetirementHistoricalBase = "34c558b660bf8d7986209dd1dbfebd819da64f18"

// This is a lexical source/reference census, not a count of executable actions.
// Per-file dispositions and named proof rows must not be inferred from a match.
var actionRetirementCorpusPattern = regexp.MustCompile(`\b(create_flow_instance|record_evidence|mailbox_write|artifact_repo_commit|config_from|instance_id_from|evidence_target|[A-Za-z0-9_]*(ArtifactRepo|MailboxWrite|ActionSpec|ConfigFrom|EvidenceTarget|HandlerAction|ActionRegistry|ActionRunner|ActionExecution|EmitSurfaceAction|ActionInstruction)[A-Za-z0-9_]*)\b`)

var actionRetirementFieldLine = regexp.MustCompile(`(?m)^\s*(action|template|evidence_target|config_from|instance_id_from)\s*:`)

type actionRetirementCorpusLedger struct {
	Version           string                       `json:"version"`
	HistoricalBase    string                       `json:"historical_base"`
	Scope             string                       `json:"scope"`
	HistoricalFiles   []actionRetirementCorpusFile `json:"historical_files"`
	NewReferenceFiles map[string]string            `json:"new_reference_files"`
}

type actionRetirementCorpusFile struct {
	Path        string   `json:"path"`
	SHA256      string   `json:"historical_sha256"`
	Markers     []string `json:"historical_markers"`
	Functions   []string `json:"historical_functions,omitempty"`
	Disposition string   `json:"disposition"`
	Proofs      []string `json:"proofs"`
}

func TestActionRetirementCorpusLedgerIsComplete(t *testing.T) {
	t.Run("unresolved_disposition_rejection", func(t *testing.T) {
		for _, disposition := range []string{"", " ", "pending_owner_assertion_review", "migrated_receipt_pending", "PENDING"} {
			if actionRetirementDispositionResolved(disposition) {
				t.Fatalf("unresolved disposition accepted: %q", disposition)
			}
		}
		if !actionRetirementDispositionResolved("Removed action adapter; surviving guard assertions retained. Execution receipts are separate.") {
			t.Fatal("source disposition incorrectly requires an execution receipt")
		}
	})
	root := conformanceRepoRoot(t)
	if os.Getenv("SWARM_GENERATE_ACTION_RETIREMENT_LEDGER") == "1" {
		generateActionRetirementCorpusLedger(t, root)
	}
	raw, err := os.ReadFile(filepath.Join(root, actionRetirementCorpusPath))
	if err != nil {
		t.Fatal(err)
	}
	var ledger actionRetirementCorpusLedger
	if err := json.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	if ledger.Version != "handler-action-retirement/v1" || ledger.HistoricalBase != actionRetirementHistoricalBase || len(ledger.HistoricalFiles) != 163 {
		t.Fatalf("unqualified historical corpus identity: %#v", ledger)
	}
	classified := map[string]bool{}
	for _, row := range ledger.HistoricalFiles {
		if row.Path == "" || classified[row.Path] || len(row.SHA256) != 64 || len(row.Markers) == 0 || !actionRetirementDispositionResolved(row.Disposition) || len(row.Proofs) == 0 {
			t.Fatalf("incomplete/duplicate historical row: %#v", row)
		}
		if _, err := hex.DecodeString(row.SHA256); err != nil {
			t.Fatalf("invalid source digest for %s: %v", row.Path, err)
		}
		classified[row.Path] = true
	}
	for path, disposition := range ledger.NewReferenceFiles {
		if classified[path] || !actionRetirementDispositionResolved(disposition) {
			t.Fatalf("duplicate or unclassified current reference %s", path)
		}
		classified[path] = true
	}
	current := actionRetirementCurrentCorpus(t, root)
	for path := range current {
		if !classified[path] {
			t.Errorf("action-related source/reference missing from complete ledger: %s", path)
		}
	}
	for path := range ledger.NewReferenceFiles {
		if _, ok := current[path]; !ok {
			t.Errorf("stale current-only corpus reference: %s", path)
		}
	}
	t.Logf("historical files=%d; current lexical reference files=%d; current-only files=%d; completeness is NOT migration/qualification", len(ledger.HistoricalFiles), len(current), len(ledger.NewReferenceFiles))
}

func actionRetirementDispositionResolved(disposition string) bool {
	return strings.TrimSpace(disposition) != "" && !strings.Contains(strings.ToLower(disposition), "pending")
}

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
	// Go fixture builders also carry node-body and handler fragments.
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
		{"worker: {timers: [{action: expire}], event_handlers: {request: {activity: {tool: run, input: {action: payload.action}}, rules: {action: {condition: 'true', emit: done}}}}}", false},
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

func actionRetirementCorpusFiles(t *testing.T, root string, allAuthored bool) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "node_modules" {
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
	raw, err = os.ReadFile(filepath.Join(path, "nodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var nodes map[string]contracts.SystemNodeContract
	if err := yaml.Unmarshal(raw, &nodes); err == nil || !strings.Contains(err.Error(), "RETIRED-HANDLER-ACTION") {
		t.Fatalf("historical action fixture became executable: %v", err)
	}
}

// Generation reads the pinned pre-retirement tree, never the new head as the
// denominator. Generated pending dispositions require manual source review and
// cannot pass the checked ledger guard; they are never execution claims.
func generateActionRetirementCorpusLedger(t *testing.T, root string) {
	t.Helper()
	// Rebuilding the historical denominator must not erase reviewed dispositions.
	if _, err := os.Stat(filepath.Join(root, actionRetirementCorpusPath)); !os.IsNotExist(err) {
		t.Fatal("ledger already exists or cannot be inspected; preserve reviewed classifications and regenerate only in an isolated scratch worktree")
	}
	git := func(args ...string) []byte {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		raw, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return raw
	}
	paths := strings.Split(strings.TrimSpace(string(git("ls-tree", "-r", "--name-only", actionRetirementHistoricalBase))), "\n")
	ledger := actionRetirementCorpusLedger{
		Version: "handler-action-retirement/v1", HistoricalBase: actionRetirementHistoricalBase,
		Scope:             "All checked .go/.yaml/.yml/.json/.md files containing exact retired spellings or dedicated action symbols. Lexical historical/reference census, not executable-row counts. Pending rows remain pending; see named proof matrix and per-owner assertion ledger.",
		NewReferenceFiles: map[string]string{},
	}
	// Filter at Git's source boundary, then use the same Go regexp for identity.
	// This avoids thousands of git-show processes without losing a source file.
	candidates := strings.Split(strings.TrimSpace(string(git("grep", "-l", "-E", "create_flow_instance|record_evidence|mailbox_write|artifact_repo_commit|config_from|instance_id_from|evidence_target|ArtifactRepo|MailboxWrite|ActionSpec|ConfigFrom|EvidenceTarget|HandlerAction|ActionRegistry|ActionRunner|ActionExecution|EmitSurfaceAction|ActionInstruction", actionRetirementHistoricalBase, "--", "*.go", "*.yaml", "*.yml", "*.json", "*.md"))), "\n")
	wanted := map[string]bool{}
	for _, path := range candidates {
		wanted[strings.TrimPrefix(path, actionRetirementHistoricalBase+":")] = true
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if !wanted[path] {
			continue
		}
		raw := git("show", actionRetirementHistoricalBase+":"+path)
		matches := actionRetirementCorpusPattern.FindAllString(string(raw), -1)
		if len(matches) == 0 {
			continue
		}
		markerSet := map[string]bool{}
		for _, marker := range matches {
			markerSet[marker] = true
		}
		markers := make([]string, 0, len(markerSet))
		for marker := range markerSet {
			markers = append(markers, marker)
		}
		sort.Strings(markers)
		digest := sha256.Sum256(raw)
		row := actionRetirementCorpusFile{Path: path, SHA256: hex.EncodeToString(digest[:]), Markers: markers, Disposition: "pending_owner_assertion_review", Proofs: []string{"R37"}}
		if strings.HasSuffix(path, ".go") {
			files := token.NewFileSet()
			file, err := parser.ParseFile(files, path, raw, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok {
					continue
				}
				start, end := files.Position(function.Pos()).Offset, files.Position(function.End()).Offset
				if actionRetirementCorpusPattern.Match(raw[start:end]) {
					row.Functions = append(row.Functions, function.Name.Name)
				}
			}
		}
		ledger.HistoricalFiles = append(ledger.HistoricalFiles, row)
		seen[path] = true
	}
	for path := range actionRetirementCurrentCorpus(t, root) {
		if !seen[path] {
			ledger.NewReferenceFiles[path] = "pending_new_reference_review"
		}
	}
	raw, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, actionRetirementCorpusPath), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log(fmt.Sprintf("generated %d historical source rows", len(ledger.HistoricalFiles)))
}
