package contracts

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// Diagnostic-only inventory. It neither updates corpus files nor supplies a
// runtime grammar or an ordinary-handler source-eligibility rule.
type stage2566CensusRow struct {
	File           string   `json:"file"`
	Site           string   `json:"site"`
	Kind           string   `json:"kind"`
	Disposition    string   `json:"disposition"`
	StageOrder     []string `json:"stage_order,omitempty"`
	OldInitial     string   `json:"old_initial,omitempty"`
	InitialFields  int      `json:"initial_fields,omitempty"`
	TerminalFields int      `json:"terminal_fields,omitempty"`
	FinalTimerRows int      `json:"marked_end_timer_rows,omitempty"`
	BeforeHash     string   `json:"before_hash"`
	AfterHash      string   `json:"dry_run_after_hash,omitempty"`
	Detail         string   `json:"detail,omitempty"`
}

func stage2566Digest(body string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(body))) }

const stage2566CorrectedBaseline = "83482f4ad0df7536975f427a25d60f2a997e4e23"

func TestIssue2566CensusAndDryRuns(t *testing.T) {
	repo := repoRootForContractsTest(t)
	command := exec.Command("git", "ls-tree", "-r", "--name-only", stage2566CorrectedBaseline)
	command.Dir = repo
	names, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var rows []stage2566CensusRow
	for _, name := range strings.Fields(string(names)) {
		if !strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".json") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(repo, name))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "initial:") && !strings.Contains(string(body), "terminal:") && !strings.Contains(string(body), "FlowStageDeclaration") && !strings.Contains(string(body), "StageDeclarations") {
			continue
		}
		if strings.HasSuffix(name, ".json") {
			rows = append(rows, stage2566CensusRow{File: name, Site: "whole-file", Kind: "historical-ledger", Disposition: "preserve-evidence-not-live-authoring", BeforeHash: stage2566Digest(string(body))})
			continue
		}
		if !strings.HasSuffix(name, ".go") {
			row := stage2566Analyze(t, name, "whole-file", "disk-yaml", string(body))
			rows = append(rows, row)
			if name == "platform-spec.yaml" {
				rows = append(rows, stage2566EmbeddedSpec(t, name, body)...)
			}
			continue
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, name, body, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, stage2566TypedStageRows(name, body, positions, file)...)
		literalCount := 0
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(text, "initial:") && !strings.Contains(text, "terminal:") {
				return true
			}
			literalCount++
			site := positions.Position(literal.Pos()).String()
			rows = append(rows, stage2566Analyze(t, name, site, "go-literal", text))
			return true
		})
		if literalCount == 0 {
			rows = append(rows, stage2566CensusRow{File: name, Site: "whole-file", Kind: "go-comment-or-code", Disposition: "manual-semantic-or-comment-review", BeforeHash: stage2566Digest(string(body))})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].File+rows[i].Site < rows[j].File+rows[j].Site })
	totals := map[string]int{}
	initial, terminal := 0, 0
	for _, row := range rows {
		totals[row.Disposition]++
		initial += row.InitialFields
		terminal += row.TerminalFields
	}
	t.Logf("source sites=%d; structurally identified stage initial fields=%d terminal fields=%d; dispositions=%v", len(rows), initial, terminal, totals)
	if path := os.Getenv("ISSUE2566_CENSUS_OUTPUT"); path != "" {
		encoded, err := json.MarshalIndent(struct {
			Baseline string               `json:"baseline"`
			Scope    string               `json:"scope"`
			Totals   map[string]int       `json:"totals"`
			Rows     []stage2566CensusRow `json:"rows"`
		}{stage2566CorrectedBaseline, "diagnostic-only corrected D2; remove initial, rename terminal to final; no production changes or final-grammar execution qualification", totals, rows}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func stage2566EmbeddedSpec(t *testing.T, name string, body []byte) []stage2566CensusRow {
	t.Helper()
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		t.Fatal(err)
	}
	queue := []yamlsource.Value{snapshot.Document(name).Root()}
	var rows []stage2566CensusRow
	for len(queue) > 0 {
		value := queue[0]
		queue = queue[1:]
		switch value.Presence() {
		case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
			fields, err := value.Mapping()
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				queue = append(queue, field.Value)
			}
		case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
			items, err := value.Sequence()
			if err != nil {
				t.Fatal(err)
			}
			queue = append(queue, items...)
		case yamlsource.PresenceScalar:
			scalar, err := value.Scalar()
			if err != nil {
				t.Fatal(err)
			}
			if scalar.Tag == "!!str" && strings.Contains(scalar.Value, "stages:") && (strings.Contains(scalar.Value, "initial:") || strings.Contains(scalar.Value, "terminal:")) {
				rows = append(rows, stage2566Analyze(t, name, value.SemanticPath(), "embedded-spec-yaml", scalar.Value))
			}
		}
	}
	return rows
}

func stage2566TypedStageRows(name string, body []byte, positions *token.FileSet, file *ast.File) []stage2566CensusRow {
	var rows []stage2566CensusRow
	seen := map[*ast.CompositeLit]bool{}
	var typeName func(ast.Expr) string
	typeName = func(expr ast.Expr) string {
		switch typed := expr.(type) {
		case *ast.Ident:
			return typed.Name
		case *ast.SelectorExpr:
			return typed.Sel.Name
		case *ast.ArrayType:
			return typeName(typed.Elt)
		}
		return ""
	}
	add := func(literal *ast.CompositeLit) {
		if seen[literal] {
			return
		}
		seen[literal] = true
		initial, terminal := 0, 0
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok {
				continue
			}
			if key.Name == "Initial" {
				initial++
			}
			if key.Name == "Terminal" {
				terminal++
			}
		}
		if initial+terminal == 0 {
			return
		}
		text := string(body[positions.Position(literal.Pos()).Offset:positions.Position(literal.End()).Offset])
		rows = append(rows, stage2566CensusRow{File: name, Site: positions.Position(literal.Pos()).String(), Kind: "typed-go-stage-fixture", Disposition: "remove-typed-initial-rename-terminal-to-final", InitialFields: initial, TerminalFields: terminal, BeforeHash: stage2566Digest(text)})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.CompositeLit); ok && typeName(literal.Type) == "FlowStageDeclaration" {
			if _, array := literal.Type.(*ast.ArrayType); array {
				for _, element := range literal.Elts {
					if child, ok := element.(*ast.CompositeLit); ok {
						add(child)
					}
				}
			} else {
				add(literal)
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok {
			for _, left := range assignment.Lhs {
				text := string(body[positions.Position(left.Pos()).Offset:positions.Position(left.End()).Offset])
				if strings.Contains(text, "StageDeclarations") && (strings.HasSuffix(text, ".Initial") || strings.HasSuffix(text, ".Terminal")) {
					rows = append(rows, stage2566CensusRow{File: name, Site: positions.Position(left.Pos()).String(), Kind: "typed-stage-assignment", Disposition: "replace-typed-marker-mutation-with-explicit-fixture-plan", BeforeHash: stage2566Digest(text)})
				}
			}
		}
		return true
	})
	return rows
}

func stage2566Analyze(t *testing.T, name, site, kind, text string) stage2566CensusRow {
	t.Helper()
	row := stage2566CensusRow{File: name, Site: site, Kind: kind, BeforeHash: stage2566Digest(text)}
	snapshot, err := yamlsource.Load([]byte(text))
	if err != nil {
		row.Disposition, row.Detail = "fragment-or-invalid-yaml-review", err.Error()
		return row
	}
	root := snapshot.Document(name).Root()
	stages, err := root.Lookup("stages")
	if err != nil || stages.Presence == yamlsource.PresenceMissing {
		row.Disposition = "non-root-stage-data-or-fragment-review"
		return row
	}
	entries, err := stages.Value.Mapping()
	if err != nil {
		row.Disposition, row.Detail = "negative-stage-shape-review", err.Error()
		return row
	}
	for _, stage := range entries {
		row.StageOrder = append(row.StageOrder, stage.Name)
		fields, err := stage.Value.Mapping()
		if err != nil {
			continue
		}
		for _, field := range fields {
			if field.Name == "initial" {
				row.InitialFields++
			}
			if field.Name == "terminal" {
				row.TerminalFields++
			}
		}
	}
	if row.InitialFields+row.TerminalFields == 0 {
		row.Disposition = "no-stage-marker-preserve-other-data"
		return row
	}
	before, err := AdmitFlowSchemaValue(root)
	if err != nil {
		row.Disposition, row.Detail = "negative-or-partial-schema-review", err.Error()
		return row
	}
	row.OldInitial = before.LoweredInitialState()
	for _, stage := range before.StageDeclarations.Entries {
		if stage.Terminal {
			row.FinalTimerRows += len(stage.Timers)
		}
	}
	if before.StageDeclarations.InitialCount() != 1 || len(row.StageOrder) == 0 || row.OldInitial != row.StageOrder[0] {
		row.Disposition = "entry-equivalence-or-negative-oracle-review"
		return row
	}
	afterText, err := stage2566DryErase(text, entries)
	if err != nil {
		row.Disposition, row.Detail = "complex-byte-edit-review", err.Error()
		return row
	}
	after, err := admitSchemaFragment(afterText)
	if err != nil {
		t.Fatalf("dry run corrupts %s at %s: %v", name, site, err)
	}
	expected := before.StageDeclarations
	for i := range expected.Entries {
		expected.Entries[i].Initial = false
	}
	if !reflect.DeepEqual(expected, after.StageDeclarations) {
		t.Fatalf("dry run changes stage order/metadata/carriers at %s", site)
	}
	afterEntries := func() []yamlsource.MappingField {
		s, _ := yamlsource.Load([]byte(afterText))
		st, _ := s.Document(name).Root().Lookup("stages")
		fields, _ := st.Value.Mapping()
		return fields
	}()
	finalText, err := stage2566DryRenameFinal(afterText, afterEntries)
	if err != nil {
		row.Disposition, row.Detail = "complex-byte-edit-review", err.Error()
		return row
	}
	finalSnapshot, err := yamlsource.Load([]byte(finalText))
	if err != nil {
		t.Fatalf("final rename corrupts %s at %s: %v", name, site, err)
	}
	finalStages, _ := finalSnapshot.Document(name).Root().Lookup("stages")
	finalEntries, err := finalStages.Value.Mapping()
	if err != nil || len(finalEntries) != len(afterEntries) {
		t.Fatalf("final rename changed declarations at %s: %v", site, err)
	}
	for i, entry := range finalEntries {
		old := afterEntries[i]
		if entry.Name != old.Name {
			t.Fatalf("final rename changed order at %s", site)
		}
		terminal, _ := old.Value.Lookup("terminal")
		final, _ := entry.Value.Lookup("final")
		if terminal.Presence != final.Presence {
			t.Fatalf("final rename changed flag presence at %s", site)
		}
		if terminal.Presence != yamlsource.PresenceMissing {
			beforeScalar, _ := terminal.Value.Scalar()
			afterScalar, _ := final.Value.Scalar()
			if beforeScalar.Value != afterScalar.Value || beforeScalar.Tag != afterScalar.Tag || beforeScalar.Style != afterScalar.Style || beforeScalar.Anchor != afterScalar.Anchor || beforeScalar.Alias != afterScalar.Alias {
				t.Fatalf("final rename changed flag value/style at %s", site)
			}
		}
	}
	second, err := stage2566DryErase(finalText, finalEntries)
	if err == nil {
		second, err = stage2566DryRenameFinal(second, finalEntries)
	}
	if err != nil || second != finalText {
		t.Fatalf("dry run is not byte-idempotent at %s: %v", site, err)
	}
	row.Disposition = "dry-run-entry-and-final-rename-metadata-preserved"
	row.Detail = "initial-removed intermediate re-admitted by current owner; final spelling source-parsed only, runtime grammar not yet implemented"
	row.AfterHash = stage2566Digest(finalText)
	return row
}

type stage2566ByteEdit struct {
	start, end  int
	replacement string
}

// Refuse complex anchors rather than guessing. The final finite script will
// apply reviewed hashes/edits, not this diagnostic discovery function.
func stage2566DryErase(text string, entries []yamlsource.MappingField) (string, error) {
	lines := strings.SplitAfter(text, "\n")
	offsets := make([]int, len(lines))
	for i := 1; i < len(lines); i++ {
		offsets[i] = offsets[i-1] + len(lines[i-1])
	}
	var edits []stage2566ByteEdit
	for _, stage := range entries {
		fields, err := stage.Value.Mapping()
		if err != nil {
			return "", err
		}
		removed := 0
		for _, field := range fields {
			if field.Name != "initial" {
				continue
			}
			if field.FromMerge || stage.FromMerge || field.Value.Location() != field.Value.ResolvedLocation() {
				return "", fmt.Errorf("alias/merge edit needs explicit source-anchor decision")
			}
			scalar, err := field.Value.Scalar()
			if err != nil || scalar.Tag != "!!bool" || (scalar.Value != "true" && scalar.Value != "false") {
				return "", fmt.Errorf("non-simple boolean at stage %s.%s", stage.Name, field.Name)
			}
			lineIndex, column := field.KeyLocation.Line-1, field.KeyLocation.Column-1
			if lineIndex < 0 || lineIndex >= len(lines) {
				return "", fmt.Errorf("invalid key coordinate")
			}
			line := lines[lineIndex]
			if column < 0 || column >= len(line) || !strings.HasPrefix(line[column:], field.Name+":") {
				return "", fmt.Errorf("quoted or complex key needs explicit edit")
			}
			end := column + len(field.Name) + 1
			for end < len(line) && (line[end] == ' ' || line[end] == '\t') {
				end++
			}
			if !strings.HasPrefix(line[end:], scalar.Value) {
				return "", fmt.Errorf("value anchor drift")
			}
			end += len(scalar.Value)
			start := column
			if strings.TrimSpace(line[:column]) == "" {
				if tail := strings.TrimSpace(line[end:]); tail != "" && !strings.HasPrefix(tail, "#") {
					return "", fmt.Errorf("complex block scalar tail")
				}
				start, end = 0, len(line)
			} else {
				for end < len(line) && (line[end] == ' ' || line[end] == '\t') {
					end++
				}
				if end < len(line) && line[end] == ',' {
					end++
					for end < len(line) && (line[end] == ' ' || line[end] == '\t') {
						end++
					}
				} else {
					for start > 0 && (line[start-1] == ' ' || line[start-1] == '\t') {
						start--
					}
					if start > 0 && line[start-1] == ',' {
						start--
					}
				}
			}
			edits = append(edits, stage2566ByteEdit{offsets[lineIndex] + start, offsets[lineIndex] + end, ""})
			removed++
		}
		if removed == len(fields) && removed > 0 {
			index, column := stage.KeyLocation.Line-1, stage.KeyLocation.Column-1
			if index < 0 || index >= len(lines) {
				return "", fmt.Errorf("invalid stage coordinate")
			}
			line := lines[index]
			prefix := stage.Name + ":"
			if !strings.HasPrefix(line[column:], prefix) {
				return "", fmt.Errorf("complex empty-stage key")
			}
			end := column + len(prefix)
			tail := strings.TrimSpace(line[end:])
			if tail == "" || strings.HasPrefix(tail, "#") {
				edits = append(edits, stage2566ByteEdit{offsets[index] + end, offsets[index] + end, " {}"})
			}
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	var disjoint []stage2566ByteEdit
	for _, edit := range edits {
		if len(disjoint) > 0 && edit.end > disjoint[len(disjoint)-1].start {
			previous := &disjoint[len(disjoint)-1]
			if previous.replacement != "" || edit.replacement != "" {
				return "", fmt.Errorf("overlapping non-deletion edits need explicit disposition")
			}
			previous.start = edit.start
			continue
		}
		disjoint = append(disjoint, edit)
	}
	last := len(text) + 1
	for _, edit := range disjoint {
		if edit.end > last {
			return "", fmt.Errorf("overlapping byte edits need explicit disposition")
		}
		text = text[:edit.start] + edit.replacement + text[edit.end:]
		last = edit.start
	}
	return text, nil
}

func stage2566DryRenameFinal(text string, entries []yamlsource.MappingField) (string, error) {
	lines := strings.SplitAfter(text, "\n")
	offsets := make([]int, len(lines))
	for i := 1; i < len(lines); i++ {
		offsets[i] = offsets[i-1] + len(lines[i-1])
	}
	var edits []stage2566ByteEdit
	for _, stage := range entries {
		fields, err := stage.Value.Mapping()
		if err != nil {
			return "", err
		}
		for _, field := range fields {
			if field.Name != "terminal" {
				continue
			}
			if field.FromMerge || stage.FromMerge || field.Value.Location() != field.Value.ResolvedLocation() {
				return "", fmt.Errorf("alias/merge rename needs explicit source-anchor decision")
			}
			line, column := field.KeyLocation.Line-1, field.KeyLocation.Column-1
			if line < 0 || line >= len(lines) || column < 0 || column >= len(lines[line]) || !strings.HasPrefix(lines[line][column:], "terminal:") {
				return "", fmt.Errorf("quoted or complex terminal key needs explicit edit")
			}
			edits = append(edits, stage2566ByteEdit{offsets[line] + column, offsets[line] + column + len("terminal"), "final"})
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, edit := range edits {
		text = text[:edit.start] + edit.replacement + text[edit.end:]
	}
	return text, nil
}

func TestIssue2566DryRunPreservesIndependentInitialAndQuotes(t *testing.T) {
	source := "name: preserve\nstages:\n  waiting:\n    initial: true\n  done:\n    terminal: true\ninstance_variables:\n  variables:\n    note: {type: text, default: 'payload.external'}\n    data: {type: json, default: {initial: true, terminal: false}}\n"
	snapshot, err := yamlsource.Load([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	stages, _ := snapshot.Document("schema.yaml").Root().Lookup("stages")
	entries, _ := stages.Value.Mapping()
	after, err := stage2566DryErase(source, entries)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(after, "waiting: {}") || !strings.Contains(after, "terminal: true") || !strings.Contains(after, "default: 'payload.external'") || !strings.Contains(after, "default: {initial: true, terminal: false}") {
		t.Fatalf("dry run changed independent source: %s", after)
	}
	if _, err := admitSchemaFragment(after); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"stages: {waiting: {initial: true, terminal: false}, done: {terminal: true}}\n",
		"stages: {initial: {initial: true}, terminal: {terminal: true}}\n",
	} {
		row := stage2566Analyze(t, "schema.yaml", "control", "control", source)
		if row.AfterHash == "" {
			t.Fatalf("simple dry-run refused: %#v", row)
		}
	}
}
