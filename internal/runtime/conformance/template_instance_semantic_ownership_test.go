package conformance

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTemplateInstanceSemanticOwnersRemainTypedAndOpaque(t *testing.T) {
	templateFieldType := reflect.TypeOf(runtimecontracts.TemplateInstanceField{})
	modeType := reflect.TypeOf(runtimecontracts.FlowInputResolutionMode(0))
	actionType := reflect.TypeOf(runtimebus.TemplateInstanceLifecycleAction(0))

	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.FlowSchemaDocument{}), "Instance", templateFieldType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.TemplateInstanceContract{}), "Field", templateFieldType)
	assertSemanticOwnerFieldType(t, reflect.TypeOf(runtimecontracts.FlowInputPinResolution{}), "Mode", modeType)
	assertSemanticOwnerMethodResult(t, reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}), "Field", templateFieldType)
	assertSemanticOwnerMethodResult(t, reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}), "Mode", modeType)
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
		events.RoutingSourceKind(0),
		events.RoutingSourceAuthority(0),
		runtimepinrouting.ConnectRoutePlanFailure(0),
		runtimepinrouting.ConnectRoutePlanEndpoint{},
		runtimepinrouting.ConnectRoutePlan{},
		runtimepinrouting.ConnectRoutePlanInstanceKey{},
		runtimepinrouting.ConnectRoutePlanFanIn{},
		runtimepinrouting.ConnectRoutePlanReplyResolution{},
		runtimepinrouting.ConnectReceiverPinIdentity{},
		runtimepinrouting.ConnectRoutePlanResolutionKind(0),
		runtimepinrouting.ConnectRoutePlanTargetKind(0),
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
	}, map[string]struct{}{
		"internal/runtime/core/pinrouting/connect_route_plan.go": {},
		"internal/runtime/contracts/event_schema_ownership.go":   {},
	})
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
	type hostileRecursiveString struct {
		variant struct{ code string }
	}
	_ = hostileRecursiveString{}.variant
	if !semanticTypeContainsString(reflect.TypeOf(hostileRecursiveString{}), map[reflect.Type]struct{}{}) {
		t.Fatal("semantic string guard accepted recursively nested string storage")
	}
	claimType := reflect.TypeOf(events.ConnectExecutionClaim{})
	recipientKind, ok := claimType.FieldByName("recipientKind")
	if !ok {
		t.Fatal("connect execution claim lost its finite recipient discriminant")
	}
	for name, owner := range map[string]reflect.Type{
		"routing source kind": reflect.TypeOf(events.RoutingSourceKind(0)),
		"routing authority":   reflect.TypeOf(events.RoutingSourceAuthority(0)),
		"delivery recipient":  recipientKind.Type,
		"connect target kind": reflect.TypeOf(runtimepinrouting.ConnectRoutePlanTargetKind(0)),
		"connect resolution":  reflect.TypeOf(runtimepinrouting.ConnectRoutePlanResolutionKind(0)),
		"connect failure":     reflect.TypeOf(runtimepinrouting.ConnectRoutePlanFailure(0)),
		"connect fan-in":      reflect.TypeOf(runtimepinrouting.ConnectFanInAggregation(0)),
		"connect reply role":  reflect.TypeOf(runtimepinrouting.ConnectReplyRole(0)),
	} {
		if semanticTypeContainsString(owner, map[reflect.Type]struct{}{}) {
			t.Fatalf("%s regressed to direct or recursive string-backed semantic authority (%s)", name, owner)
		}
		if owner.Kind() < reflect.Uint || owner.Kind() > reflect.Uint64 {
			t.Fatalf("%s storage = %s, want an immutable unsigned finite discriminant", name, owner.Kind())
		}
	}
}

func semanticTypeContainsString(owner reflect.Type, seen map[reflect.Type]struct{}) bool {
	if owner == nil {
		return false
	}
	if _, visited := seen[owner]; visited {
		return false
	}
	seen[owner] = struct{}{}
	switch owner.Kind() {
	case reflect.String:
		return true
	case reflect.Pointer, reflect.Array, reflect.Slice:
		return semanticTypeContainsString(owner.Elem(), seen)
	case reflect.Map:
		return semanticTypeContainsString(owner.Key(), seen) || semanticTypeContainsString(owner.Elem(), seen)
	case reflect.Struct:
		for index := 0; index < owner.NumField(); index++ {
			if semanticTypeContainsString(owner.Field(index).Type, seen) {
				return true
			}
		}
	}
	return false
}

func TestCompiledRoutingFiniteVariantsAreConstantsNotMutableBindings(t *testing.T) {
	finiteTypes := map[string]struct{}{
		"RoutingSourceKind": {}, "RoutingSourceAuthority": {}, "deliveryRecipientKind": {},
		"ConnectRoutePlanTargetKind": {}, "ConnectRoutePlanResolutionKind": {}, "ConnectRoutePlanFailure": {},
		"ConnectFanInAggregation": {}, "ConnectReplyRole": {},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, relative := range []string{"internal/events/types.go", "internal/runtime/core/pinrouting/connect_route_plan.go"} {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, filepath.Join(repoRoot, relative), nil, 0)
		if err != nil {
			t.Fatalf("parse finite routing owner %s: %v", relative, err)
		}
		if bindings := mutableFiniteRoutingBindings(fset, parsed, finiteTypes); len(bindings) != 0 {
			t.Fatalf("%s exposes mutable finite routing bindings: %v", relative, bindings)
		}
	}

	hostileSet := token.NewFileSet()
	hostile, err := parser.ParseFile(hostileSet, "hostile.go", `package pinrouting
type ConnectRoutePlanFailure uint8
const FixedFailure ConnectRoutePlanFailure = 1
var ArbitraryReceiverName = FixedFailure
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if bindings := mutableFiniteRoutingBindings(hostileSet, hostile, finiteTypes); len(bindings) != 1 || bindings[0] != "ArbitraryReceiverName" {
		t.Fatalf("mutable finite binding guard = %v, want arbitrary exported binding", bindings)
	}
}

func mutableFiniteRoutingBindings(fset *token.FileSet, file *ast.File, finiteTypes map[string]struct{}) []string {
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	config := types.Config{Importer: importer.Default(), Error: func(error) {}}
	_, _ = config.Check("compiledrouting/owner", fset, []*ast.File{file}, info)
	var out []string
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.VAR {
			continue
		}
		for _, specification := range generic.Specs {
			value, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range value.Names {
				object := info.Defs[name]
				if name.IsExported() && finiteRoutingNamedType(object) != "" {
					if _, finite := finiteTypes[finiteRoutingNamedType(object)]; !finite {
						continue
					}
					out = append(out, name.Name)
				}
			}
		}
	}
	return out
}

func finiteRoutingNamedType(object types.Object) string {
	if object == nil {
		return ""
	}
	named, _ := object.Type().(*types.Named)
	if named == nil || named.Obj() == nil {
		return ""
	}
	return named.Obj().Name()
}

func TestCompiledConnectValuesExposeNoMutableSemanticFields(t *testing.T) {
	for _, owner := range []reflect.Type{
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlan{}),
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanEndpoint{}),
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKey{}),
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanFanIn{}),
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanReplyResolution{}),
		reflect.TypeOf(runtimepinrouting.ConnectReceiverPinIdentity{}),
		reflect.TypeOf(runtimepinrouting.ConnectReceiverPinAdmission{}),
		reflect.TypeOf(runtimepinrouting.ConnectReceiverPinCollision{}),
		reflect.TypeOf(runtimepinrouting.SourceEvent{}),
	} {
		for index := 0; index < owner.NumField(); index++ {
			if field := owner.Field(index); field.IsExported() {
				t.Fatalf("%s exposes mutable compiled field %s (%s)", owner, field.Name, field.Type)
			}
		}
	}
}

func TestCompiledRoutingConsumersCannotReconstructCanonicalAuthority(t *testing.T) {
	request := reflect.TypeOf(runtimebus.FlowInstanceRouteMaterializationRequest{})
	if _, exists := request.FieldByName("Template"); exists {
		t.Fatal("route materialization request regained request-local template authority")
	}
	if request.NumField() != 2 {
		t.Fatalf("route materialization request fields = %d, want identity plus activation variables", request.NumField())
	}

	subscriber := reflect.TypeOf(runtimebus.Subscriber{})
	recipient, exists := subscriber.FieldByName("Recipient")
	if !exists || recipient.Type != reflect.TypeOf(events.DeliveryRecipient{}) {
		t.Fatalf("subscriber recipient = %#v, want typed delivery recipient", recipient)
	}
	provenance, exists := subscriber.FieldByName("routeSource")
	if !exists || provenance.IsExported() || provenance.Type.Kind() == reflect.String {
		t.Fatalf("subscriber provenance = %#v, want private non-string discriminant", provenance)
	}
	if _, exists := subscriber.FieldByName("RouteSource"); exists {
		t.Fatal("subscriber regained public route-source string authority")
	}

	deliveryRoute := reflect.TypeOf(events.DeliveryRoute{})
	if field, exists := deliveryRoute.FieldByName("Recipient"); !exists || field.Type != reflect.TypeOf(events.DeliveryRecipient{}) {
		t.Fatalf("delivery route recipient = %#v, want typed delivery recipient", field)
	}
	for _, retired := range []string{"SubscriberType", "SubscriberID"} {
		if _, exists := deliveryRoute.FieldByName(retired); exists {
			t.Fatalf("delivery route regained public %s wire authority", retired)
		}
	}

	forkRecipient := reflect.TypeOf(runfork.RunForkContractFrontierRecipient{})
	if field, exists := forkRecipient.FieldByName("Recipient"); !exists || field.Type != reflect.TypeOf(events.DeliveryRecipient{}) {
		t.Fatalf("selected-fork recipient = %#v, want typed delivery recipient", field)
	}
	for _, retired := range []string{"SubscriberType", "SubscriberID", "RouteSource"} {
		if _, exists := forkRecipient.FieldByName(retired); exists {
			t.Fatalf("selected-fork recipient regained public %s wire authority", retired)
		}
	}

	resolutionInput := reflect.TypeOf(runtimepinrouting.ResolutionInput{})
	routingSource, exists := resolutionInput.FieldByName("RoutingSource")
	if !exists || routingSource.Type != reflect.TypeOf(events.RoutingSource{}) {
		t.Fatalf("pin routing source input = %#v, want admitted RoutingSource", routingSource)
	}

	admit, exists := reflect.TypeOf(&runtimepinrouting.ConnectReceiverPinAdmission{}).MethodByName("Admit")
	if !exists {
		t.Fatal("compiled connect receiver-pin admission owner is missing")
	}
	for index := 1; index < admit.Type.NumIn(); index++ {
		parameter := admit.Type.In(index)
		if parameter.Kind() == reflect.String || strings.Contains(parameter.String(), "routingtopology") {
			t.Fatalf("receiver-pin admission accepts non-canonical topology/readback authority %s", parameter)
		}
	}

	repoRoot := canonicalrouting.RepoRoot(t)
	retiredFunctions := map[string]struct{}{
		"connectReceiverPinCollisionIssues":       {},
		"connectReceiverPinFact":                  {},
		"connectReceiverPinStaticTargetKey":       {},
		"connectReceiverCarrierRouteKeys":         {},
		"connectSubscriberMatchesPlanTarget":      {},
		"routeFlowInputHasLoweredConnectReceiver": {},
		"contractFrontierRouteLookup":             {},
		"eventReferencesMatch":                    {},
		"eventReferencesOverlap":                  {},
		"routeFromEvent":                          {},
	}
	guardedFiles := []string{
		"internal/events/types.go",
		"internal/runtime/core/pinrouting/connect_route_plan.go",
		"internal/runtime/core/pinrouting/pinrouting.go",
		"internal/runtime/core/pinrouting/flow_input_producer.go",
		"internal/runtime/bus/connect_route_plan_dispatch.go",
		"internal/runtime/bus/template_instance_lifecycle.go",
		"internal/runtime/bus/routing_derivation.go",
		"internal/runtime/bus/eventbus.go",
		"internal/runtime/bus/eventbus_routing.go",
		"internal/runtime/bus/delivery_planner.go",
		"internal/runtime/bus/route_plan.go",
		"internal/runtime/runfork/models.go",
		"internal/runtime/runforkadmission/contract_frontier.go",
		"internal/runtime/runforkadmission/selected_route_history.go",
		"internal/runtime/runforkexecution/recipient_planning.go",
		"internal/runtime/manager/selected_contract_route_recovery.go",
		"internal/runtime/routingtopology/topology.go",
	}
	for _, relative := range guardedFiles {
		path := filepath.Join(repoRoot, relative)
		assertFileDeclaresNoFunctions(t, path, retiredFunctions)
		assertFileUsesNoCompositeDelimiter(t, path)
	}
	assertFileImportsNoPath(t, filepath.Join(repoRoot, "internal/runtime/bootverify/workflow_composition_connect_checks.go"), "internal/runtime/routingtopology")
}

func assertFileUsesNoCompositeDelimiter(t testing.TB, path string) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		if strings.Contains(literal.Value, `\x00`) || strings.Contains(literal.Value, `\x1f`) {
			t.Errorf("%s contains prohibited delimiter-backed identity literal %s", path, literal.Value)
		}
		return true
	})
}

func assertFileDeclaresNoFunctions(t testing.TB, path string, prohibited map[string]struct{}) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, blocked := prohibited[function.Name.Name]; blocked {
			t.Errorf("%s redeclares retired authority %s", path, function.Name.Name)
		}
	}
}

func assertFileImportsNoPath(t testing.TB, path, prohibited string) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, imported := range parsed.Imports {
		if strings.Contains(strings.Trim(imported.Path.Value, "\""), prohibited) {
			t.Errorf("%s imports retired behavioral owner %s", path, prohibited)
		}
	}
}

func TestCompiledConnectDiagnosticProjectionCannotReenterEvaluator(t *testing.T) {
	graphType := reflect.TypeOf(runtimepinrouting.CompiledConnectGraph{})
	diagnostics := map[reflect.Type]struct{}{
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanReadback{}):            {},
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanEndpointReadback{}):    {},
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanInstanceKeyReadback{}): {},
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanFanInReadback{}):       {},
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlanReplyReadback{}):       {},
		reflect.TypeOf(runtimepinrouting.ConnectReceiverPinCollision{}):         {},
	}
	for _, methodName := range []string{"MatchingPlans", "PlanMatchesEvent", "MatchingSourceEvent", "IssueMatchesEvent"} {
		method, ok := graphType.MethodByName(methodName)
		if !ok {
			t.Fatalf("compiled graph evaluator %s is missing", methodName)
		}
		for index := 1; index < method.Type.NumIn(); index++ {
			parameter := method.Type.In(index)
			_, diagnostic := diagnostics[parameter]
			if parameter.Kind() == reflect.String || diagnostic || parameter == reflect.TypeOf(runtimepinrouting.ConnectEndpointRole{}) || parameter == reflect.TypeOf(runtimepinrouting.ConnectEdgeEvidence{}) {
				t.Fatalf("compiled graph evaluator %s accepts diagnostic projection %s", methodName, parameter)
			}
		}
	}
}

func TestCompiledConnectEvaluatorsDoNotNormalizeOrConsumeReadback(t *testing.T) {
	guarded := map[string]struct{}{
		"connectSourceEndpointMatches": {},
		"PlanMatchesEvent":             {},
		"MatchingSourceEvent":          {},
		"IssueMatchesEvent":            {},
		"SourceParentRoute":            {},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	path := filepath.Join(repoRoot, "internal/runtime/core/pinrouting/connect_route_plan.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse compiled connect owner: %v", err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		if _, ok := guarded[function.Name.Name]; !ok {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "Normalize", "Trim", "TrimSpace", "Split", "HasPrefix", "HasSuffix", "Readback":
				t.Errorf("compiled connect evaluator %s calls prohibited semantic projection %s", function.Name.Name, selector.Sel.Name)
			}
			return true
		})
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

func assertProductionSelectorCallsConfined(t testing.TB, names, allowedFiles map[string]struct{}) {
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
			_, allowed := allowedFiles[relative]
			if _, guarded := names[selector.Sel.Name]; guarded && !allowed {
				t.Errorf("%s calls confined compiled-connect input %s; allowed owners are %#v", relative, selector.Sel.Name, allowedFiles)
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

func assertSemanticOwnerMethodResult(t *testing.T, owner reflect.Type, methodName string, want reflect.Type) {
	t.Helper()
	method, ok := owner.MethodByName(methodName)
	if !ok {
		t.Fatalf("%s.%s is missing", owner, methodName)
	}
	if method.Type.NumOut() != 1 || method.Type.Out(0) != want {
		t.Fatalf("%s.%s result = %s, want canonical %s", owner, methodName, method.Type.Out(0), want)
	}
}
