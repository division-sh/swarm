// Command rewrite-list-spellings-2287 rewrites declaration carriers, never data or historical snapshots.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

var angle = regexp.MustCompile(`list<([^<>]+)>`)

// These pinned fragments become declarations only after Go concatenation or
// formatting; they cannot be decoded as standalone YAML.
var generatedSourceEdits = map[string][][2]string{
	"internal/runtime/testfixtures/canonicalrouting/singleton_coordinator.go": {
		{"  audit_log: \"[AuditEntry]\"", "  audit_log: [AuditEntry]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/notify_all_children.go": {
		{"  account_ids: \"[text]\"", "  account_ids: [text]"},
		{"  account_ids: \"[NumericAccount]\"", "  account_ids: [NumericAccount]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/notify_all_children_nested_serving.go": {
		{"  task_ids: '[text]'", "  task_ids: [text]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/arrival_join_binding.go": {
		{"  expected: \"[text]\"", "  expected: [text]"},
		{"  missing: \"[text]\"", "  missing: [text]"},
		{"  results: \"[JoinResult]\"", "  results: [JoinResult]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/publication_sites.go": {
		{"  items: '[%s]'", "  items: [%s]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/lifecycle_emitter_static.go": {
		{"  items: \"[text]\"", "  items: [text]"},
	},
	"internal/runtime/pipeline/a2_join_deadline_execution_external_test.go": {
		{"  final_missing: \"[text]\"", "  final_missing: [text]"},
		{"  members_list: \"[text]\"", "  members_list: [text]"},
	},
	"internal/runtime/bootverify/workflow_singleton_coordinator_checks_test.go": {
		{"  unused_members: '[text]'", "  unused_members: [text]"},
	},
	"internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go": {
		{"  account_ids: \"[NumericAccount]\"", "  account_ids: [NumericAccount]"},
	},
	"internal/runtime/testfixtures/canonicalrouting/root_reply.go": {
		{"  request_id: \"[text]\"", "  request_id: [text]"},
	},
	"internal/runtime/contracts/catalog_type_admission_test.go": {
		{"  child: '[]text'", "  child: {type: '[]text'}"},
	},
	"internal/releasee2e/catalog_type_admission_test.go": {
		{"  labels: '[Label]'", "  labels: [Label]"},
		{"items: '[]text'", "items: {type: '[]text'}"},
	},
}

type edit struct {
	start, end int
	text       string
}

func canonical(raw string) string {
	for angle.MatchString(raw) {
		raw = angle.ReplaceAllString(raw, "[$1]")
	}
	return raw
}

func offset(body string, line, column int) int {
	pos := 0
	for i := 1; i < line; i++ {
		next := strings.IndexByte(body[pos:], '\n')
		if next < 0 {
			return -1
		}
		pos += next + 1
	}
	return pos + column - 1
}

func scalarEdit(body string, value yamlsource.Value, replacement string) (edit, bool) {
	s, err := value.Scalar()
	if err != nil || s.Anchor != "" || s.Alias != "" {
		return edit{}, false
	}
	p := offset(body, s.Location.Line, s.Location.Column)
	if p < 0 || p >= len(body) {
		return edit{}, false
	}
	end := p + len(s.Value)
	if s.Style&(yamlsource.SingleQuotedStyle|yamlsource.DoubleQuotedStyle) != 0 {
		if strings.ContainsAny(s.Value, "'\"\\\n") {
			return edit{}, false
		}
		end += 2
	}
	if end > len(body) {
		return edit{}, false
	}
	return edit{p, end, replacement}, true
}

func bareReference(raw string) string {
	if strings.HasSuffix(raw, "?") || strings.Contains(raw, ",") || (strings.HasPrefix(raw, "[") && strings.Contains(raw[1:], "[")) {
		return "{type: " + strconv.Quote(raw) + "}"
	}
	return raw
}

func rewriteField(body string, value yamlsource.Value, edits *[]edit) {
	if value.Presence() == yamlsource.PresenceScalar {
		s, err := value.Scalar()
		if err != nil || s.Tag != "!!str" {
			return
		}
		next := canonical(s.Value)
		quotedBracket := s.Style&(yamlsource.SingleQuotedStyle|yamlsource.DoubleQuotedStyle) != 0 && strings.HasPrefix(s.Value, "[")
		if next != s.Value || quotedBracket {
			if change, ok := scalarEdit(body, value, bareReference(next)); ok {
				*edits = append(*edits, change)
			}
		}
		return
	}
	if value.Presence() != yamlsource.PresenceMapping {
		return
	}
	fields, err := value.Mapping()
	if err != nil {
		return
	}
	for _, field := range fields {
		if field.Name != "type" {
			continue
		}
		s, err := field.Value.Scalar()
		if err != nil {
			continue
		}
		next := canonical(s.Value)
		if next != s.Value {
			if change, ok := scalarEdit(body, field.Value, strconv.Quote(next)); ok {
				*edits = append(*edits, change)
			}
		}
	}
}

func mappings(value yamlsource.Value) []yamlsource.MappingField {
	fields, _ := value.Mapping()
	return fields
}

func mappingValue(value yamlsource.Value, name string) yamlsource.Value {
	for _, field := range mappings(value) {
		if field.Name == name {
			return field.Value
		}
	}
	return yamlsource.Value{}
}

func recordFields(root yamlsource.Value, eventMetadata bool) []yamlsource.Value {
	var values []yamlsource.Value
	for _, record := range mappings(root) {
		for _, field := range mappings(record.Value) {
			if !strings.HasPrefix(field.Name, "_") && (!eventMetadata || (field.Name != "key" && field.Name != "description")) {
				values = append(values, field.Value)
			}
		}
	}
	return values
}

func declarationValues(root yamlsource.Value, kind string) []yamlsource.Value {
	switch kind {
	case "types.yaml":
		return recordFields(mappingValue(root, "types"), false)
	case "entities.yaml":
		return recordFields(root, false)
	case "events.yaml":
		return recordFields(root, true)
	case "nodes.yaml":
		var values []yamlsource.Value
		for _, node := range mappings(root) {
			fields := mappingValue(mappingValue(node.Value, "state_schema"), "fields")
			for _, field := range mappings(fields) {
				values = append(values, field.Value)
			}
			if items, err := fields.Sequence(); err == nil {
				values = append(values, items...)
			}
		}
		return values
	}
	return nil
}

func family(root yamlsource.Value) string {
	for _, field := range mappings(root) {
		if field.Name == "types" || field.Name == "scalars" || field.Name == "enums" {
			return "types.yaml"
		}
	}
	for _, field := range mappings(root) {
		for _, child := range mappings(field.Value) {
			if child.Name == "state_schema" || child.Name == "event_handlers" {
				return "nodes.yaml"
			}
		}
	}
	for _, field := range mappings(root) {
		if strings.Contains(field.Name, ".") {
			return "events.yaml"
		}
	}
	return ""
}

func rewriteYAML(body, kind string) string {
	snapshot, err := yamlsource.Load([]byte(body))
	if err != nil {
		return body
	}
	root := snapshot.Document(kind).Root()
	if kind == "" {
		kind = family(root)
	}
	var edits []edit
	for _, value := range declarationValues(root, kind) {
		rewriteField(body, value, &edits)
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, change := range edits {
		body = body[:change.start] + change.text + body[change.end:]
	}
	return body
}

func sourceFamily(call *ast.CallExpr) string {
	for _, arg := range call.Args {
		found := ""
		ast.Inspect(arg, func(node ast.Node) bool {
			if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				text, _ := strconv.Unquote(lit.Value)
				for _, kind := range []string{"entities.yaml", "events.yaml", "nodes.yaml", "types.yaml"} {
					if strings.Contains(text, kind) {
						found = kind
					}
				}
			}
			return true
		})
		if found != "" {
			return found
		}
	}
	return ""
}

func sourceKeyFamily(key ast.Expr) string {
	if ident, ok := key.(*ast.Ident); ok {
		switch ident.Name {
		case "TypesYAML":
			return "types.yaml"
		case "EntitiesYAML":
			return "entities.yaml"
		case "EventsYAML":
			return "events.yaml"
		case "NodesYAML":
			return "nodes.yaml"
		}
	}
	if lit, ok := key.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		name, _ := strconv.Unquote(lit.Value)
		if strings.HasSuffix(name, ".yaml") {
			return filepath.Base(name)
		}
	}
	return ""
}

func enclosingSourceFamily(lit *ast.BasicLit, parents map[ast.Node]ast.Node) string {
	for parent := parents[lit]; parent != nil; parent = parents[parent] {
		switch parent := parent.(type) {
		case *ast.KeyValueExpr:
			if kind := sourceKeyFamily(parent.Key); kind != "" {
				return kind
			}
		case *ast.CallExpr:
			if kind := sourceFamily(parent); kind != "" {
				return kind
			}
		}
	}
	return ""
}

func rewriteGoLiteral(path string, lit *ast.BasicLit, parents map[ast.Node]ast.Node) (string, bool) {
	text, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	next := text
	// These exact table cells are concatenated into declaration values by
	// CopyFlowConstructorSupply, not passed as programmatic type references.
	if filepath.ToSlash(path) == "internal/runtime/bootverify/flow_constructor_test.go" &&
		(text == "list<map[text]text>" || text == "[map[text]text]") {
		next = bareReference(canonical(text))
	}
	if entry, ok := parents[lit].(*ast.KeyValueExpr); ok {
		if key, ok := entry.Key.(*ast.Ident); ok && (key.Name == "Type" || key.Name == "resultType") {
			next = canonical(text)
		}
	}
	if strings.Contains(text, "\n") {
		next = rewriteYAML(text, enclosingSourceFamily(lit, parents))
	}
	for _, replacement := range generatedSourceEdits[filepath.ToSlash(path)] {
		next = strings.ReplaceAll(next, replacement[0], replacement[1])
	}
	if next == text {
		return "", false
	}
	rendered := strconv.Quote(next)
	if strings.HasPrefix(lit.Value, "`") && !strings.Contains(next, "`") {
		rendered = "`" + next + "`"
	}
	return rendered, true
}

func rewriteGo(path, body string) string {
	fset := token.NewFileSet()
	tree, err := parser.ParseFile(fset, path, body, 0)
	if err != nil {
		return body
	}
	parents := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(tree, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	var edits []edit
	ast.Inspect(tree, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if rendered, changed := rewriteGoLiteral(path, lit, parents); changed {
			edits = append(edits, edit{fset.Position(lit.Pos()).Offset, fset.Position(lit.End()).Offset, rendered})
		}
		return true
	})
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, change := range edits {
		body = body[:change.start] + change.text + body[change.end:]
	}
	return body
}

func main() {
	write := flag.Bool("write", false, "apply declaration-only source changes")
	flag.Parse()
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != "." && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "test-results" || entry.Name() == "worktrees" || entry.Name() == "vendor" || entry.Name() == "scripts") {
				return filepath.SkipDir
			}
			return nil
		}
		kind := filepath.Base(path)
		isGo := strings.HasSuffix(path, ".go")
		if !isGo && kind != "entities.yaml" && kind != "events.yaml" && kind != "types.yaml" && kind != "nodes.yaml" {
			return nil
		}
		if strings.Contains(path, "list_spelling_retirement_test.go") || strings.Contains(path, "list_declaration_value.go") {
			return nil
		}
		before, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var after string
		if isGo {
			after = rewriteGo(path, string(before))
		} else {
			after = rewriteYAML(string(before), kind)
		}
		if after == string(before) {
			return nil
		}
		fmt.Println(filepath.ToSlash(path))
		if *write {
			return os.WriteFile(path, []byte(after), 0644)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
