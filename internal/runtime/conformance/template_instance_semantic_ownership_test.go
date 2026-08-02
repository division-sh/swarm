package conformance

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		if err != nil || !mode.Valid() || runtimecontracts.FlowInputResolutionModeCode(mode) != authored {
			t.Fatalf("resolution mode %q parse/round trip = %v/%v/%q", authored, mode, err, runtimecontracts.FlowInputResolutionModeCode(mode))
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

type promotedRoutingMode struct {
	runtimecontracts.FlowInputResolutionMode
}

func TestCompiledRoutingTypesDoNotImplementStringer(t *testing.T) {
	stringer := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	owners := []any{
		runtimecontracts.TemplateInstanceField{},
		runtimecontracts.FlowInputResolutionMode(0),
		runtimecontracts.FlowOutputSink(0),
		runtimecontracts.EventConsumerBoundary(0),
		semanticview.ConnectorImportSource{},
		runtimebus.TemplateInstanceLifecycleAction(0),
		events.DeliveryRouteIdentity{},
		events.RoutingSourceKind{},
		events.RoutingSourceAuthority{},
		runtimepinrouting.ConnectRoutePlanResolutionKind{},
		runtimepinrouting.ConnectRoutePlanTargetKind{},
		promotedRoutingMode{},
	}
	for _, owner := range owners {
		typeOf := reflect.TypeOf(owner)
		if typeOf.Implements(stringer) || reflect.PointerTo(typeOf).Implements(stringer) {
			t.Fatalf("%s implements fmt.Stringer; use an explicit codec or diagnostic projection", typeOf)
		}
	}
}

func TestCompiledRoutingStringerGuardRejectsPromotedAndInterfaceConversions(t *testing.T) {
	var promoted any = promotedRoutingMode{FlowInputResolutionMode: runtimecontracts.FlowInputResolutionModeSelect}
	if _, ok := promoted.(fmt.Stringer); ok {
		t.Fatal("promoted routing semantic value unexpectedly satisfies fmt.Stringer")
	}
	var pointer any = &promotedRoutingMode{FlowInputResolutionMode: runtimecontracts.FlowInputResolutionModeSelect}
	if _, ok := pointer.(fmt.Stringer); ok {
		t.Fatal("pointer to promoted routing semantic value unexpectedly satisfies fmt.Stringer")
	}
}

func TestCompiledConnectCompilerInputBoundaryRejectsBehavioralRawConnectAccess(t *testing.T) {
	assertProductionSelectorCallsConfined(t, map[string]struct{}{
		"CompositionConnects": {},
	}, "internal/runtime/core/pinrouting/connect_route_plan.go")
}

func TestCompiledConnectCompilerInputBoundaryRejectsResolvedConnectHelper(t *testing.T) {
	assertProductionIdentifiersConfined(t, map[string]struct{}{
		"resolvedCompositionConnects": {},
	}, "internal/runtime/core/pinrouting/connect_route_plan.go")
}

func TestConnectInterpreterBoundaryRejectsEventGraphMatchOutsideEvaluator(t *testing.T) {
	assertProductionIdentifiersConfined(t, map[string]struct{}{
		"connectSourceEndpointMatches":      {},
		"connectSourceEndpointMatchesEvent": {},
	}, "internal/runtime/core/pinrouting/connect_route_plan.go")
}

func TestCompiledRoutingBoundaryRejectsSemanticStringOperations(t *testing.T) {
	for name, owner := range map[string]reflect.Type{
		"routing source kind":  reflect.TypeOf(events.RoutingSourceKind{}),
		"routing authority":    reflect.TypeOf(events.RoutingSourceAuthority{}),
		"connect target kind":  reflect.TypeOf(runtimepinrouting.ConnectRoutePlanTargetKind{}),
		"connect resolution":   reflect.TypeOf(runtimepinrouting.ConnectRoutePlanResolutionKind{}),
		"delivery route claim": reflect.TypeOf(events.ConnectExecutionClaim{}),
	} {
		if owner.Kind() == reflect.String {
			t.Fatalf("%s regressed to a string-backed semantic authority", name)
		}
	}
}

func TestCompiledConnectDiagnosticProjectionCannotReenterEvaluator(t *testing.T) {
	graphType := reflect.TypeOf(runtimepinrouting.CompiledConnectGraph{})
	for _, methodName := range []string{"MatchingPlans", "PlanMatchesEvent", "MatchingSourceEvent", "IssueMatchesEvent"} {
		method, ok := graphType.MethodByName(methodName)
		if !ok {
			t.Fatalf("compiled graph evaluator %s is missing", methodName)
		}
		for index := 1; index < method.Type.NumIn(); index++ {
			parameter := method.Type.In(index)
			if parameter.Kind() == reflect.String || parameter == reflect.TypeOf(runtimepinrouting.ConnectEndpointRole{}) || parameter == reflect.TypeOf(runtimepinrouting.ConnectEdgeEvidence{}) {
				t.Fatalf("compiled graph evaluator %s accepts diagnostic projection %s", methodName, parameter)
			}
		}
	}
}

func assertProductionIdentifiersConfined(t testing.TB, names map[string]struct{}, allowedFile string) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	allowedFile = filepath.ToSlash(allowedFile)
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, guarded := names[identifier.Name]; guarded && relative != allowedFile {
				t.Errorf("%s uses confined compiled-connect interpreter %s; only %s may own it", relative, identifier.Name, allowedFile)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan production Go files: %v", err)
	}
}

func assertProductionSelectorCallsConfined(t testing.TB, names map[string]struct{}, allowedFile string) {
	t.Helper()
	scanProductionGoFiles(t, func(relative string, parsed *ast.File) {
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, guarded := names[selector.Sel.Name]; guarded && relative != filepath.ToSlash(allowedFile) {
				t.Errorf("%s calls confined compiled-connect input %s; only %s may own it", relative, selector.Sel.Name, allowedFile)
			}
			return true
		})
	})
}

func scanProductionGoFiles(t testing.TB, visit func(string, *ast.File)) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		visit(filepath.ToSlash(relative), parsed)
		return nil
	})
	if err != nil {
		t.Fatalf("scan production Go files: %v", err)
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
