package conformance

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func TestTemplateInstanceSemanticOwnersRemainTypedAndOpaque(t *testing.T) {
	templateFieldType := reflect.TypeOf(runtimecontracts.TemplateInstanceField{})
	modeType := reflect.TypeOf(runtimecontracts.FlowInputResolutionMode(0))
	sourceType := reflect.TypeOf(runtimecontracts.FlowInputInstanceSource{})
	actionType := reflect.TypeOf(runtimebus.TemplateInstanceLifecycleAction(0))

	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.FlowSchemaDocument{}), "Instance", templateFieldType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.TemplateInstanceContract{}), "Field", templateFieldType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.FlowInputPinResolution{}), "Mode", modeType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}), "Field", templateFieldType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}), "Mode", modeType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}), "Source", sourceType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimebus.TemplateInstanceLifecycleDecision{}), "Action", actionType)

	if templateFieldType.Kind() != reflect.Struct || templateFieldType.NumField() != 1 {
		t.Fatalf("TemplateInstanceField shape = %s/%d fields, want one-field opaque struct", templateFieldType.Kind(), templateFieldType.NumField())
	}
	if field := templateFieldType.Field(0); field.IsExported() || field.Type.Kind() != reflect.String {
		t.Fatalf("TemplateInstanceField storage = %#v, want unexported string admitted only by parser", field)
	}
	for name, semanticType := range map[string]reflect.Type{"FlowInputResolutionMode": modeType, "TemplateInstanceLifecycleAction": actionType} {
		if semanticType.Kind() == reflect.String {
			t.Fatalf("%s regressed to free string", name)
		}
	}

	seen := map[runtimecontracts.FlowInputResolutionMode]struct{}{}
	for _, authored := range []string{"create", "select", "select-or-create", "fan-in", "fan-out", "reply"} {
		mode, err := runtimecontracts.ParseFlowInputResolutionMode(authored)
		if err != nil || !mode.Valid() || mode.String() != authored {
			t.Fatalf("resolution mode %q parse/round trip = %v/%v/%q", authored, mode, err, mode.String())
		}
		if _, duplicate := seen[mode]; duplicate {
			t.Fatalf("resolution mode %q aliases an existing semantic value", authored)
		}
		seen[mode] = struct{}{}
	}
	if _, err := runtimecontracts.ParseFlowInputResolutionMode("create-or-select"); err == nil {
		t.Fatal("unknown resolution mode was admitted")
	}
}

func assertSemanticOwnerFieldType(t *testing.T, owner reflect.Type, fieldName string, want reflect.Type) {
	t.Helper()
	field, ok := owner.FieldByName(fieldName)
	if !ok {
		t.Fatalf("%s.%s is missing", owner, fieldName)
	}
	if field.Type != want {
		t.Fatalf("%s.%s type = %s, want canonical %s", owner, fieldName, field.Type, want)
	}
}

func TestTemplateInstanceSemanticStringConversionsStayAtNamedBoundaries(t *testing.T) {
	root := conformanceRepoRoot(t)
	allowed := map[string]string{
		"internal/runtime/authoringview/view.go":                             "public readback",
		"internal/runtime/bootverify/workflow_composition_connect_checks.go": "verification diagnostics",
		"internal/runtime/bootverify/workflow_flow_boundary_checks.go":       "verification comparison",
		"internal/runtime/bootverify/workflow_template_instance_checks.go":   "verification diagnostics",
		"internal/runtime/bus/connect_route_plan_dispatch.go":                "runtime diagnostics and typed payload projection",
		"internal/runtime/bus/template_instance_lifecycle.go":                "stable encoding and diagnostics",
		"internal/runtime/contracts/workflow_contract_types.go":              "typed value lookup",
		"internal/runtime/contracts/workflow_contract_wave1_accessors.go":    "contract validation",
		"internal/runtime/contracts/workflow_instance_resolution_source.go":  "contract validation diagnostics",
		"internal/runtime/core/pinrouting/connect_route_plan.go":             "stable encoding and typed plan materialization",
		"internal/runtime/routingtopology/topology.go":                       "topology readback",
		"internal/runtime/semanticview/endpoint_census.go":                   "semantic readback",
	}

	err := filepath.WalkDir(filepath.Join(root, "internal", "runtime"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "String" || !isTemplateInstanceSemanticStringReceiver(selector.X) {
				return true
			}
			if reason := allowed[filepath.ToSlash(rel)]; strings.TrimSpace(reason) == "" {
				t.Errorf("semantic String conversion in unclassified boundary %s: %s", filepath.ToSlash(rel), renderASTNode(selector.X))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan semantic string conversions: %v", err)
	}
}

func isTemplateInstanceSemanticStringReceiver(expr ast.Expr) bool {
	switch receiver := expr.(type) {
	case *ast.Ident:
		return receiver.Name == "field" || receiver.Name == "mode" || receiver.Name == "action"
	case *ast.SelectorExpr:
		return receiver.Sel.Name == "Field" || receiver.Sel.Name == "Mode" || receiver.Sel.Name == "Action" || strings.HasPrefix(receiver.Sel.Name, "FlowInputResolutionMode")
	}
	return false
}

func renderASTNode(node ast.Node) string {
	var out bytes.Buffer
	_ = printer.Fprint(&out, token.NewFileSet(), node)
	return out.String()
}
