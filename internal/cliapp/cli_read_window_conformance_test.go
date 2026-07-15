package cliapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCLIReadWindowRegistryCoversEveryPublicSinceUntilFlag(t *testing.T) {
	want := map[string]string{
		"swarm run list":      "read_window",
		"swarm run trace":     "read_window",
		"swarm event list":    "read_window",
		"swarm logs":          "read_window",
		"swarm mailbox defer": "future_deadline",
	}
	got := map[string]string{}
	for path, cmd := range visibleCLICommandPaths(t) {
		if cmd.Flags().Lookup("since") == nil && cmd.Flags().Lookup("until") == nil {
			continue
		}
		classification, ok := want[path]
		if !ok {
			t.Errorf("public command %q exposes --since/--until without a read-window classification", path)
			continue
		}
		got[path] = classification
	}
	for path, classification := range want {
		if got[path] != classification {
			t.Errorf("classified command %q = %q, want %q", path, got[path], classification)
		}
	}
}

func TestCLIReadWindowConsumersRouteThroughCanonicalOwner(t *testing.T) {
	want := map[string]bool{
		"diagnosticRunListOptions.params":         false,
		"diagnosticTraceOptions.snapshotParams":   false,
		"eventListCommandOptions.params":          false,
		"runtimeLogCommandOptions.snapshotParams": false,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, declaration := range parsed.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Body == nil {
				continue
			}
			receiver := cliReadWindowReceiverName(fn.Recv.List[0].Type)
			key := receiver + "." + fn.Name.Name
			if _, ok := want[key]; !ok {
				continue
			}
			ownerCalls := 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch called := cliReadWindowCallName(call.Fun); called {
				case "readwindow.Resolve":
					ownerCalls++
				case "validateRFC3339Flag", "parseRFC3339Flag":
					t.Errorf("%s still calls legacy %s", key, called)
				}
				return true
			})
			if ownerCalls != 1 {
				t.Errorf("%s canonical owner calls = %d, want 1", key, ownerCalls)
			}
			want[key] = true
		}
	}
	for key, found := range want {
		if !found {
			t.Errorf("canonical read-window consumer %s not found", key)
		}
	}
}

func cliReadWindowCallName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		owner, ok := value.X.(*ast.Ident)
		if !ok {
			return value.Sel.Name
		}
		return owner.Name + "." + value.Sel.Name
	default:
		return ""
	}
}

func TestCLIReadWindowSpecOwnerAndConsumersStayCanonical(t *testing.T) {
	spec := loadCLISpecification(t)
	foundations := driftMappingValue(spec, "foundations")
	owner := driftMappingValue(foundations, "read_window_input")
	if owner == nil {
		t.Fatal("cli_specification.foundations.read_window_input not found")
	}
	if got := cliReadWindowSpecScalar(owner, "canonical_owner"); got != "platform-spec.yaml#cli_specification.foundations.read_window_input" {
		t.Fatalf("read-window canonical_owner = %q", got)
	}
	accepted := driftMappingValue(owner, "accepted_input")
	relative := cliReadWindowSpecScalar(accepted, "relative")
	for _, required := range []string{"lowercase `h`, `m`, `s`, or `ms`", "Every component and the total MUST be positive", "duration overflow"} {
		if !strings.Contains(relative, required) {
			t.Errorf("relative grammar missing %q: %s", required, relative)
		}
	}

	catalog := driftMappingValue(spec, "command_catalog")
	for _, command := range []string{"runs", "events_list", "logs"} {
		row := driftMappingValue(catalog, command)
		if got := cliReadWindowSpecScalar(row, "read_window_input_contract"); got != "cli_specification.foundations.read_window_input" {
			t.Errorf("command_catalog.%s read-window owner = %q", command, got)
		}
	}
	trace := driftMappingValue(catalog, "trace")
	traceFilter := driftMappingValue(trace, "promoted_trace_filter_contract")
	if got := cliReadWindowSpecScalar(traceFilter, "cli_read_window_input_contract"); got != "cli_specification.foundations.read_window_input" {
		t.Errorf("command_catalog.trace read-window owner = %q", got)
	}
}

func cliReadWindowSpecScalar(node *yaml.Node, key string) string {
	if node == nil {
		return ""
	}
	value := driftMappingValue(node, key)
	if value == nil {
		return ""
	}
	return strings.TrimSpace(value.Value)
}

func cliReadWindowReceiverName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return cliReadWindowReceiverName(value.X)
	default:
		return ""
	}
}
