package contracts

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestA2JoinRetirementBoundaryGuard(t *testing.T) {
	root := a2GuardRepoRoot(t)
	var violations []string
	for _, base := range []string{"internal", "cmd"} {
		err := checkoutsource.WalkDir(root, filepath.Join(root, base), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found, err := a2RetiredGoViolations(filepath.ToSlash(relative), raw)
			violations = append(violations, found...)
			return err
		})
		if err != nil {
			t.Fatalf("scan A2 production boundary %s: %v", base, err)
		}
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("live retired A2 interpreters or accepted fields:\n%s", strings.Join(violations, "\n"))
	}
}

func TestA2JoinRetirementCorpusGuard(t *testing.T) {
	root := a2GuardRepoRoot(t)
	count := 0
	var violations []string
	for _, base := range []string{"examples", "tests", "internal"} {
		err := checkoutsource.WalkDir(root, filepath.Join(root, base), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || (entry.Name() != "schema.yaml" && entry.Name() != "nodes.yaml") {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found, err := a2RetiredYAMLViolations(filepath.ToSlash(relative), string(raw))
			if err != nil {
				return fmt.Errorf("parse corpus %s: %w", relative, err)
			}
			count++
			violations = append(violations, found...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if count == 0 {
		t.Fatal("A2 contract corpus is empty")
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("positive authored corpus retains A2 grammar:\n%s", strings.Join(violations, "\n"))
	}
}

func a2GuardRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate A2 guard source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// This is a syntax census, not a package load: it also sees alternate-build
// implementations, and does not repeatedly type-check the entire runtime.
func a2RetiredGoViolations(path string, raw []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var violations []string
	report := func(node ast.Node, detail string) {
		violations = append(violations, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), detail))
	}
	for _, declaration := range file.Decls {
		ast.Inspect(declaration, func(node ast.Node) bool {
			if name, ok := node.(*ast.Ident); ok && a2RetiredIdentifier(path, name.Name) {
				report(node, "retired semantic identifier "+name.Name)
			}
			return true
		})
		switch declaration := declaration.(type) {
		case *ast.GenDecl:
			for _, specification := range declaration.Specs {
				switch specification := specification.(type) {
				case *ast.TypeSpec:
					structure, ok := specification.Type.(*ast.StructType)
					if !ok {
						continue
					}
					context := a2GoSchemaContext(path, specification.Name.Name)
					for _, field := range structure.Fields.List {
						for _, name := range field.Names {
							if a2RetiredField(context, a2WireField(name.Name)) {
								report(name, context+" retains field "+name.Name)
							}
						}
						if field.Tag != nil {
							tag, err := strconv.Unquote(field.Tag.Value)
							if err != nil {
								return nil, err
							}
							for _, encoding := range []string{"yaml", "json"} {
								key := strings.Split(reflect.StructTag(tag).Get(encoding), ",")[0]
								if a2RetiredField(context, key) {
									report(field.Tag, context+" retains "+encoding+" field "+key)
								}
							}
						}
					}
				case *ast.ValueSpec:
					for index, name := range specification.Names {
						if index < len(specification.Values) {
							a2CheckAllowedFields(specification.Values[index], a2AllowedFieldContext(name.Name), report)
						}
					}
				}
			}
		case *ast.FuncDecl:
			if declaration.Body == nil {
				continue
			}
			ast.Inspect(declaration.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.BasicLit:
					if strings.Contains(path, "/bootverify/") && a2ASTString(node) == "fan_in_input" {
						report(node, "retired singleton fan-in demand interpretation")
					}
				case *ast.CallExpr:
					name := a2ASTName(node.Fun)
					if (name == "nodeValueFields" || name == "schemaValueFields") && len(node.Args) >= 3 {
						// Argument four holds rejection diagnostics, not accepted grammar.
						a2CheckAllowedFields(node.Args[2], a2ASTString(node.Args[1]), report)
					}
				case *ast.CaseClause:
					if declaration.Name.Name == "ParseFlowInputResolutionMode" {
						for _, label := range node.List {
							if a2ASTString(label) == "fan-in" && !a2CaseRefuses(node) {
								report(label, "resolution parser admits retired fan-in mode")
							}
						}
					}
				}
				return true
			})
		}
	}
	// Go tests deliberately contain retired parser-rejection specimens and are
	// excluded above. Non-test generators still count as positive corpus owners.
	if strings.HasPrefix(path, "internal/runtime/testfixtures/") || strings.HasPrefix(path, "internal/cliapp/archetypes/") {
		for _, declaration := range file.Decls {
			scope := ""
			if function, ok := declaration.(*ast.FuncDecl); ok {
				scope = function.Name.Name
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				source := a2ASTString(literal)
				if a2NegativeGeneratorSpecimen(path, scope, source) {
					return true
				}
				found, err := a2RetiredYAMLViolations(path, source)
				// SQL, fragments and prose are not contract documents; the AST owner
				// checks above still cover live interpreters in these generator files.
				if err == nil {
					for _, detail := range found {
						report(literal, "generated contract: "+detail)
					}
				}
				return true
			})
		}
	}
	return violations, nil
}

// These exact byte-pinned syntax/rejection specimens are not executable
// positive sources. Companion controls reject changed bytes, scope and path.
func a2NegativeGeneratorSpecimen(path, scope, source string) bool {
	var digest string
	switch {
	case path == "internal/runtime/testfixtures/canonicalrouting/singleton_coordinator.go" && scope == "RetiredFanInCoordinatorSchema":
		digest = "e5ddef48d633a35f84af0ef9dccaa077b81d528b7cfc1953efd6aa4267a94747"
	case path == "internal/runtime/testfixtures/canonicalrouting/arrival_join_guard_sources.go" && scope == "ArrivalJoinRetirementGuardCases":
		digest = "08e97be9242a1251a056a50fe4e4c166519f0705bb2a87cbe620a373ce2cb8bf"
	default:
		return false
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(source))) == digest
}

func a2RetiredIdentifier(path, name string) bool {
	lower := strings.ToLower(name)
	if strings.Contains(path, "/bootverify/") && (name == "wave1ResolveNamedType" || name == "wave1BuiltinScalar" || name == "wave1ListElementType") {
		return true
	}
	for _, prefix := range []string{"flowinputresolutionmodefanin", "connectrouteplanfanin", "connectfanin", "connectexecutionclaimfanincodec", "cloneconnectrouteplanfanin", "resolvefanin", "validatefanin", "fanininput"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	if strings.Contains(path, "/pinrouting/") || strings.Contains(path, "/routingtopology/") {
		return lower == "fanin"
	}
	return strings.Contains(path, "/accumulator/") && (lower == "effectivespecforhandler" || lower == "effectivespec" || lower == "partitions" || lower == "partition")
}

func a2GoSchemaContext(path, name string) string {
	switch name {
	case "FlowInputPinResolution", "connectExecutionClaimFanInCodec":
		return "input pin resolution"
	case "AccumulateSpec":
		return "accumulate"
	case "JoinSpec":
		return "join"
	case "JoinMembersSpec":
		return "join.members"
	}
	if strings.Contains(path, "/accumulator/") && name == "State" {
		return "accumulate"
	}
	return ""
}

func a2AllowedFieldContext(name string) string {
	switch name {
	case "inputEventPinResolutionFieldOptions":
		return "input pin resolution"
	case "accumulateFieldOptions":
		return "accumulate"
	case "joinFieldOptions":
		return "join"
	case "joinMembersFieldOptions":
		return "join.members"
	case "handlerFieldOptions", "ruleFieldOptions":
		return "handler"
	}
	return ""
}

func a2CheckAllowedFields(expression ast.Expr, context string, report func(ast.Node, string)) {
	if literal, ok := expression.(*ast.CompositeLit); ok {
		for _, element := range literal.Elts {
			if field, ok := element.(*ast.KeyValueExpr); ok && a2RetiredField(context, a2ASTString(field.Key)) {
				report(field.Key, context+" accepts retired field "+a2ASTString(field.Key))
			}
		}
	}
}

func a2WireField(name string) string {
	switch name {
	case "Aggregate", "Aggregation":
		return "aggregation"
	case "DedupBy", "DedupPath", "DedupBySet":
		return "dedup_by"
	case "Window", "WindowPath", "WindowSet":
		return "window"
	case "Partition", "PartitionPath", "Partitions":
		return "partition"
	}
	return strings.ToLower(name)
}

func a2RetiredField(context, name string) bool {
	switch context {
	case "input pin resolution":
		return name == "aggregation" || name == "window" || name == "dedup_by" || name == "singleton"
	case "accumulate", "join.members":
		return name == "window" || name == "dedup_by" || name == "partition"
	case "join":
		return name == "window" || name == "complete_when" || name == "remaining" || name == "timeout"
	case "handler":
		return name == "dedup_by"
	}
	return false
}

func a2ASTString(expression ast.Expr) string {
	if literal, ok := expression.(*ast.BasicLit); ok && literal.Kind == token.STRING {
		value, _ := strconv.Unquote(literal.Value)
		return value
	}
	return ""
}

func a2ASTName(expression ast.Expr) string {
	switch expression := expression.(type) {
	case *ast.Ident:
		return expression.Name
	case *ast.SelectorExpr:
		return expression.Sel.Name
	}
	return ""
}

func a2CaseRefuses(clause *ast.CaseClause) bool {
	if len(clause.Body) == 0 {
		return false
	}
	statement, ok := clause.Body[len(clause.Body)-1].(*ast.ReturnStmt)
	if !ok || len(statement.Results) == 0 {
		return false
	}
	call, ok := statement.Results[len(statement.Results)-1].(*ast.CallExpr)
	return ok && (a2ASTName(call.Fun) == "Errorf" || a2ASTName(call.Fun) == "New" || a2ASTName(call.Fun) == "nodeValueError")
}

func a2RetiredYAMLViolations(path, raw string) ([]string, error) {
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	var violations []string
	for {
		var document yaml.Node
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				return violations, nil
			}
			return nil, err
		}
		if len(document.Content) != 1 {
			continue
		}
		root := a2YAMLFields(document.Content[0])
		report := func(node *yaml.Node, detail string) {
			violations = append(violations, fmt.Sprintf("%s:%d: %s", path, node.Line, detail))
		}
		inputs := a2YAMLFields(a2YAMLFields(root["pins"])["inputs"])["events"]
		if inputs != nil {
			for _, pin := range a2YAMLSequence(inputs) {
				resolution := a2YAMLFields(pin)["resolution"]
				a2CheckYAMLFields(resolution, "input pin resolution", report)
				if mode := a2YAMLFields(resolution)["mode"]; mode != nil && mode.Value == "fan-in" {
					report(mode, "input pin resolution admits retired fan-in mode")
				}
			}
		}
		if connections := root["connect"]; connections != nil {
			for _, connection := range a2YAMLSequence(connections) {
				resolution := a2YAMLFields(connection)["resolution"]
				if resolution != nil && resolution.Value == "fan-in" {
					report(resolution, "connect admits retired fan-in mode")
				}
			}
		}
		for _, declaration := range root {
			for _, handler := range a2YAMLFields(a2YAMLFields(declaration)["event_handlers"]) {
				a2CheckYAMLHandler(handler, report)
			}
		}
	}
}

func a2YAMLFields(node *yaml.Node) map[string]*yaml.Node {
	fields := map[string]*yaml.Node{}
	seen := map[*yaml.Node]bool{}
	var collect func(*yaml.Node)
	collect = func(node *yaml.Node) {
		if node == nil || seen[node] {
			return
		}
		seen[node] = true
		if node.Kind == yaml.AliasNode {
			collect(node.Alias)
			return
		}
		if node.Kind == yaml.SequenceNode {
			for _, child := range node.Content {
				collect(child)
			}
		}
		if node.Kind != yaml.MappingNode {
			return
		}
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Tag == "!!merge" {
				collect(node.Content[index+1])
			}
		}
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Tag != "!!merge" {
				fields[node.Content[index].Value] = node.Content[index+1]
			}
		}
	}
	collect(node)
	return fields
}

func a2YAMLResolved(node *yaml.Node) *yaml.Node {
	seen := map[*yaml.Node]bool{}
	for node != nil && node.Kind == yaml.AliasNode {
		if seen[node] {
			return nil
		}
		seen[node] = true
		node = node.Alias
	}
	return node
}

func a2YAMLSequence(node *yaml.Node) []*yaml.Node {
	if node = a2YAMLResolved(node); node != nil && node.Kind == yaml.SequenceNode {
		return node.Content
	}
	return nil
}

func a2CheckYAMLFields(node *yaml.Node, context string, report func(*yaml.Node, string)) {
	for key, value := range a2YAMLFields(node) {
		if a2RetiredField(context, key) {
			report(value, context+" declares retired field "+key)
		}
	}
}

func a2CheckYAMLHandler(node *yaml.Node, report func(*yaml.Node, string)) {
	seen := map[*yaml.Node]bool{}
	var visit func(*yaml.Node)
	visit = func(node *yaml.Node) {
		if node = a2YAMLResolved(node); node == nil || seen[node] {
			return
		}
		seen[node] = true
		a2CheckYAMLFields(node, "handler", report)
		fields := a2YAMLFields(node)
		a2CheckYAMLFields(fields["accumulate"], "accumulate", report)
		join := fields["join"]
		a2CheckYAMLFields(join, "join", report)
		a2CheckYAMLFields(a2YAMLFields(join)["members"], "join.members", report)
		for _, parent := range []*yaml.Node{node, join} {
			for _, field := range []string{"rules", "on_complete", "on_deadline"} {
				child := a2YAMLResolved(a2YAMLFields(parent)[field])
				if child != nil && child.Kind == yaml.SequenceNode {
					for _, rule := range child.Content {
						visit(rule)
					}
				} else if child != nil {
					visit(child)
				}
			}
		}
	}
	visit(node)
}

func TestA2JoinRetirementGuardRejectsHostileAST(t *testing.T) {
	cases := []struct {
		name, path, code, want string
	}{
		{"aliased removed type", "internal/runtime/engine/hostile.go", `package engine; type hidden = other.ConnectRoutePlanFanIn`, "ConnectRoutePlanFanIn"},
		{"new resolver", "internal/runtime/bus/hostile.go", `package bus; func resolveFanInAgain() {}`, "resolveFanInAgain"},
		{"codec", "internal/runtime/core/pinrouting/hostile.go", `package pinrouting; type replacement struct { FanIn any }`, "FanIn"},
		{"renamed resolution field", "internal/runtime/contracts/hostile.go", "package contracts; type FlowInputPinResolution struct { Value any `yaml:\"aggregation\"` }", "yaml field aggregation"},
		{"accumulator partition", "internal/runtime/accumulator/hostile.go", `package accumulator; type State struct { PartitionPath []string }`, "PartitionPath"},
		{"accumulator compatibility projection", "internal/runtime/accumulator/hostile.go", `package accumulator; func EffectiveSpecForHandler() {}`, "EffectiveSpecForHandler"},
		{"unused accepted grammar", "internal/runtime/contracts/hostile.go", `package contracts; var accumulateFieldOptions = map[string]struct{}{"window": {}}`, "accepts retired field window"},
		{"inline accepted grammar", "internal/runtime/contracts/hostile.go", `package contracts; func decode(v any) { nodeValueFields(v, "accumulate", map[string]struct{}{"dedup_by": {}}) }`, "accepts retired field dedup_by"},
		{"positive mode branch", "internal/runtime/contracts/hostile.go", `package contracts; func ParseFlowInputResolutionMode(v string) (int, error) { switch v { case "fan-in": return 4, nil }; return 0, nil }`, "parser admits retired fan-in"},
		{"singleton demand branch", "internal/runtime/bootverify/hostile.go", `package bootverify; func demand(kind string) bool { return kind == "fan_in_input" }`, "singleton fan-in demand"},
		{"local type parser", "internal/runtime/bootverify/hostile.go", `package bootverify; func wave1ResolveNamedType() {}`, "wave1ResolveNamedType"},
		{"local scalar parser", "internal/runtime/bootverify/hostile.go", `package bootverify; func wave1BuiltinScalar() {}`, "wave1BuiltinScalar"},
		{"local list parser", "internal/runtime/bootverify/hostile.go", `package bootverify; func wave1ListElementType() {}`, "wave1ListElementType"},
		{"generated positive", "internal/runtime/testfixtures/canonicalrouting/hostile.go", "package canonicalrouting; const source = `pins:\n  inputs:\n    events:\n      - event: result\n        resolution: {mode: fan-in}\n`", "generated contract"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			found, err := a2RetiredGoViolations(test.path, []byte(test.code))
			if err != nil || !strings.Contains(strings.Join(found, "\n"), test.want) {
				t.Fatalf("hostile boundary %s escaped guard: %v, %v", test.want, found, err)
			}
		})
	}
}

func TestA2JoinRetirementGuardPreservesHomonymsAndOtherVariants(t *testing.T) {
	code := "package contracts\n" + `
type Business struct { Window string; Aggregation string; DedupBy []string; Singleton bool }
type AccumulateSpec struct { Key string }
func business(v string) string { switch v { case "fan-in": return v }; return "" }
func ParseFlowInputResolutionMode(v string) (int, error) {
    switch v {
    case "fan-in": return 0, fmt.Errorf("retired fan-in")
    case "fan-out": return 5, nil
    case "reply": return 6, nil
    }
    return 0, fmt.Errorf("unsupported")
}
func decode(v any) {
    nodeValueFields(v, "accumulate", map[string]struct{}{"key": {}})
    _ = map[string]any{"window": "business", "aggregation": "sum", "dedup_by": "business"}
}`
	found, err := a2RetiredGoViolations("internal/runtime/contracts/homonyms.go", []byte(code))
	if err != nil || len(found) != 0 {
		t.Fatalf("business homonyms, rejection diagnostics or reply/fan-out overblocked: %v, %v", found, err)
	}
	for _, specimen := range canonicalrouting.ArrivalJoinRetirementGuardCases() {
		if specimen.Name == "homonyms" {
			found, err = a2RetiredYAMLViolations("homonyms.yaml", specimen.Source)
			if err != nil || len(found) != 0 {
				t.Fatalf("business YAML or retained variants overblocked: %v, %v", found, err)
			}
			return
		}
	}
	t.Fatal("homonym guard specimen is missing")
}

func TestA2JoinRetirementGuardNegativeSpecimenIsExactAndRejected(t *testing.T) {
	const path = "internal/runtime/testfixtures/canonicalrouting/singleton_coordinator.go"
	const scope = "RetiredFanInCoordinatorSchema"
	source := canonicalrouting.RetiredFanInCoordinatorSchema()
	a2AssertExactNegativeGeneratorSpecimen(t, path, scope, source)
	var schema FlowSchemaDocument
	if err := decodeNodeTestYAML([]byte(source), &schema); err == nil || !strings.Contains(err.Error(), "resolution") {
		t.Fatalf("whitelisted negative schema is no longer rejected at resolution admission: %v", err)
	}
	if mode, err := ParseFlowInputResolutionMode("fan-in"); err == nil || mode.Valid() {
		t.Fatalf("retired mode admitted independently of other retired fields: %v, %v", mode, err)
	}
}

func a2AssertExactNegativeGeneratorSpecimen(t *testing.T, path, scope, source string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(a2GuardRepoRoot(t), filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == scope {
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if literal, ok := node.(*ast.BasicLit); ok && a2NegativeGeneratorSpecimen(path, scope, a2ASTString(literal)) {
					count++
				}
				return true
			})
		}
	}
	if count != 1 {
		t.Fatalf("negative-generator exception is stale: matched %d exact specimens; update or remove it", count)
	}
	for _, test := range []struct{ path, scope, source string }{
		{path, scope, source + "description: new\n"},
		{path, "positiveFixture", source},
		{"internal/runtime/testfixtures/canonicalrouting/another.go", scope, source},
	} {
		if a2NegativeGeneratorSpecimen(test.path, test.scope, test.source) {
			t.Fatalf("negative whitelist widened beyond exact specimen: %#v", test)
		}
	}
}

func TestA2JoinRetirementGuardRejectsSchemaContexts(t *testing.T) {
	pinFields := map[string]bool{}
	for _, specimen := range canonicalrouting.ArrivalJoinRetirementGuardCases() {
		if !strings.HasPrefix(specimen.Name, "pin/") {
			continue
		}
		found, err := a2RetiredYAMLViolations("schema.yaml", specimen.Source)
		if err != nil || len(found) != 1 || !strings.Contains(found[0], specimen.WantError) {
			t.Fatalf("retired pin field %s escaped context guard: %v, %v", specimen.WantError, found, err)
		}
		pinFields[specimen.WantError] = true
	}
	for _, field := range []string{"aggregation", "window", "dedup_by", "singleton"} {
		if !pinFields[field] {
			t.Fatalf("retired pin field %s lost its guard specimen", field)
		}
	}
	for _, test := range []struct{ field, context string }{
		{"window", "accumulate"}, {"dedup_by", "accumulate"}, {"partition", "accumulate"},
		{"window", "join"}, {"complete_when", "join"}, {"remaining", "join"}, {"timeout", "join"},
	} {
		for _, nesting := range []string{"", "rules:\n        - ", "join:\n        on_complete:\n          "} {
			prefix := "worker:\n  event_handlers:\n    arrived:\n      "
			raw := prefix + nesting + test.context + ": {" + test.field + ": null}\n"
			found, err := a2RetiredYAMLViolations("nodes.yaml", raw)
			if err != nil || len(found) != 1 || !strings.Contains(found[0], test.field) {
				t.Fatalf("retired %s.%s escaped %q: %v, %v", test.context, test.field, nesting, found, err)
			}
		}
	}
	for _, specimen := range canonicalrouting.ArrivalJoinRetirementGuardCases() {
		if specimen.Name == "aliases" {
			a2AssertExactNegativeGeneratorSpecimen(t,
				"internal/runtime/testfixtures/canonicalrouting/arrival_join_guard_sources.go",
				"ArrivalJoinRetirementGuardCases", specimen.Source)
			found, err := a2RetiredYAMLViolations("aliases.yaml", specimen.Source)
			if err != nil || len(found) != 2 {
				t.Fatalf("aliases or merges hid retired schema contexts: %v, %v", found, err)
			}
			return
		}
	}
	t.Fatal("alias guard specimen is missing")
}
