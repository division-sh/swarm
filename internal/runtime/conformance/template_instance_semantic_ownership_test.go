package conformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
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
	allowedFunctions := map[string]string{
		"github.com/division-sh/swarm/internal/runtime/authoringview.buildFlows":                                            "public instance-field readback",
		"github.com/division-sh/swarm/internal/runtime/authoringview.inputPinViews":                                         "public resolution-mode readback",
		"github.com/division-sh/swarm/internal/runtime/bootverify.checkTemplateInstanceValidation":                          "verification diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bootverify.flowInputHandlerUsesResolutionMode":                       "typed verification comparison",
		"github.com/division-sh/swarm/internal/runtime/bootverify.validateCanonicalInstanceInputPinResolution":              "verification diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bootverify.validateInputPinResolution":                               "verification diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bus.TemplateInstanceLifecycleDecision.ActivationVariables":           "typed payload projection",
		"github.com/division-sh/swarm/internal/runtime/bus.TemplateInstanceLifecycleDecision.Detail":                        "runtime diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bus.connectRoutePlanFailureDetail":                                   "runtime diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bus.connectRoutePlanInstanceResolutionRemediation":                   "runtime teaching remediation",
		"github.com/division-sh/swarm/internal/runtime/bus.connectRoutePlanResolver.resolveAgentCarrierIdentity":            "runtime diagnostics",
		"github.com/division-sh/swarm/internal/runtime/bus.syntheticDeliveryPayloadProjection":                              "typed delivery projection",
		"github.com/division-sh/swarm/internal/runtime/bus.templateInstanceLifecycleKeyDigest":                              "stable identity encoding",
		"github.com/division-sh/swarm/internal/runtime/bus.templateInstanceLifecycleKeyMap":                                 "typed key projection",
		"github.com/division-sh/swarm/internal/runtime/bus.templateInstanceLifecycleKeyMaterialDetail":                      "runtime diagnostics",
		"github.com/division-sh/swarm/internal/runtime/contracts.TemplateInstanceContract.CanonicalKeyMaterial":             "canonical contract lookup",
		"github.com/division-sh/swarm/internal/runtime/contracts.WorkflowContractBundle.ResolveFlowInputInstanceSourceType": "contract validation diagnostics",
		"github.com/division-sh/swarm/internal/runtime/contracts.validateTemplateInstanceField":                             "contract validation",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.ConnectInstanceKeyDescriptorMatches":                 "typed descriptor comparison",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.EventSourcedInstanceKeyMaterialForConnectRoutePlan":  "typed plan materialization",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.InstanceKeyMaterialForConnectRoutePlan":              "typed plan materialization",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.connectCanonicalResolutionInstanceKey":               "typed plan validation diagnostics",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.connectResolutionInstanceKey":                        "typed plan diagnostics",
		"github.com/division-sh/swarm/internal/runtime/core/pinrouting.deterministicResolutionUUID":                         "stable identity encoding",
		"github.com/division-sh/swarm/internal/runtime/routingtopology.resolutionView":                                      "topology readback",
		"github.com/division-sh/swarm/internal/runtime/semanticview.endpointCensusBuilder.addPinEndpoints":                  "semantic readback",
	}

	findings, err := compilerResolvedTemplateInstanceStringConversions(root)
	if err != nil {
		t.Fatalf("scan semantic string conversions: %v", err)
	}
	seenFunctions := map[string]struct{}{}
	for _, finding := range findings {
		seenFunctions[finding.Function] = struct{}{}
	}
	for function, reason := range allowedFunctions {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("semantic String conversion permission %s has no boundary reason", function)
		}
		if _, ok := seenFunctions[function]; !ok {
			t.Errorf("semantic String conversion permission %s is stale", function)
		}
	}
	for _, finding := range unclassifiedTemplateInstanceStringConversions(findings, allowedFunctions) {
		t.Errorf("%s String conversion in unclassified function %s at %s:%d: %s", finding.Owner, finding.Function, finding.File, finding.Line, finding.Receiver)
	}
}

func TestTemplateInstanceSemanticStringConversionGuardUsesCompilerReceiverTypes(t *testing.T) {
	root := conformanceRepoRoot(t)
	scanner, _, err := newTemplateInstanceSemanticScanner(root)
	if err != nil {
		t.Fatalf("prepare compiler-resolved semantic scanner: %v", err)
	}
	findings, err := scanner.scanSourcePackage(
		"github.com/division-sh/swarm/internal/runtime/conformance/semanticguardhostile",
		map[string]string{"hostile.go": `package semanticguardhostile

import (
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type modeAlias = runtimecontracts.FlowInputResolutionMode

type embeddedMode struct {
	runtimecontracts.FlowInputResolutionMode
}

func fieldResult() runtimecontracts.TemplateInstanceField {
	var value runtimecontracts.TemplateInstanceField
	return value
}

func renderStringer(value fmt.Stringer) string {
	return value.String()
}

func returnedStringer(value modeAlias) fmt.Stringer {
	return value
}

func arbitraryReceiverNames(identity runtimecontracts.TemplateInstanceField, resolution modeAlias, outcome *runtimebus.TemplateInstanceLifecycleAction, promoted embeddedMode) {
	_ = identity.String()
	_ = resolution.String()
	_ = outcome.String()
	_ = fieldResult().String()
	render := resolution.String
	_ = render()
	_ = promoted.String()
	_ = fmt.Sprint(identity)
	_ = fmt.Sprintf("%v", resolution)
	_ = renderStringer(outcome)
	var assigned fmt.Stringer = identity
	assigned = resolution
	_ = assigned.String()
	_ = []fmt.Stringer{promoted}
	_ = fmt.Sprint(any(*outcome))
	_ = returnedStringer(resolution)
	_ = func() fmt.Stringer { return resolution }()
	stringers := make(chan fmt.Stringer, 1)
	stringers <- identity
}
`},
	)
	if err != nil {
		t.Fatalf("scan hostile semantic receivers: %v", err)
	}
	if len(findings) != 16 {
		t.Fatalf("compiler-resolved hostile findings = %#v, want sixteen direct/method-value/promoted/fmt/Stringer conversions", findings)
	}
	owners := map[string]int{}
	for _, finding := range findings {
		owners[finding.Owner]++
	}
	for owner, want := range map[string]int{
		"contracts.TemplateInstanceField":     5,
		"contracts.FlowInputResolutionMode":   8,
		"bus.TemplateInstanceLifecycleAction": 3,
	} {
		if owners[owner] != want {
			t.Fatalf("hostile findings for %s = %d, want %d (%#v)", owner, owners[owner], want, findings)
		}
	}
}

func TestTemplateInstanceSemanticStringConversionGuardIncludesRootRuntimePackage(t *testing.T) {
	root := conformanceRepoRoot(t)
	_, packages, err := newTemplateInstanceSemanticScanner(root)
	if err != nil {
		t.Fatalf("prepare compiler-resolved semantic scanner: %v", err)
	}
	for _, candidate := range packages {
		if candidate.ImportPath == "github.com/division-sh/swarm/internal/runtime" {
			return
		}
	}
	t.Fatal("compiler-resolved semantic scan omitted the root internal/runtime production package")
}

func TestTemplateInstanceSemanticStringConversionGuardAllowsOnlyExactFunctions(t *testing.T) {
	root := conformanceRepoRoot(t)
	scanner, _, err := newTemplateInstanceSemanticScanner(root)
	if err != nil {
		t.Fatalf("prepare compiler-resolved semantic scanner: %v", err)
	}
	const importPath = "github.com/division-sh/swarm/internal/runtime/conformance/semanticguardfunctionhostile"
	findings, err := scanner.scanSourcePackage(importPath, map[string]string{"approved.go": `package semanticguardfunctionhostile

import runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

func approvedBoundary(mode runtimecontracts.FlowInputResolutionMode) string {
	return mode.String()
}

func illegalCoreBranch(mode runtimecontracts.FlowInputResolutionMode) bool {
	return mode.String() == "select"
}
`})
	if err != nil {
		t.Fatalf("scan same-file function boundary: %v", err)
	}
	unauthorized := unclassifiedTemplateInstanceStringConversions(findings, map[string]string{
		importPath + ".approvedBoundary": "test readback boundary",
	})
	if len(unauthorized) != 1 || unauthorized[0].Function != importPath+".illegalCoreBranch" {
		t.Fatalf("same-file unauthorized findings = %#v, want only illegalCoreBranch", unauthorized)
	}
}

type templateInstanceSemanticPackage struct {
	ImportPath string
	Dir        string
	Export     string
	GoFiles    []string
}

type templateInstanceSemanticScanner struct {
	fileSet  *token.FileSet
	importer types.Importer
}

type templateInstanceStringConversion struct {
	Owner    string
	Function string
	File     string
	Line     int
	Receiver string
}

func compilerResolvedTemplateInstanceStringConversions(root string) ([]templateInstanceStringConversion, error) {
	scanner, packages, err := newTemplateInstanceSemanticScanner(root)
	if err != nil {
		return nil, err
	}
	var findings []templateInstanceStringConversion
	for _, candidate := range packages {
		packageFindings, err := scanner.scanPackage(candidate)
		if err != nil {
			return nil, err
		}
		findings = append(findings, packageFindings...)
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		return findings[i].Line < findings[j].Line
	})
	return findings, nil
}

func newTemplateInstanceSemanticScanner(root string) (*templateInstanceSemanticScanner, []templateInstanceSemanticPackage, error) {
	command := exec.Command("go", "list", "-deps", "-export", "-json", "./internal/runtime/...")
	command.Dir = root
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("load runtime package exports: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	exports := map[string]string{}
	var runtimePackages []templateInstanceSemanticPackage
	decoder := json.NewDecoder(bytes.NewReader(output))
	for decoder.More() {
		var candidate templateInstanceSemanticPackage
		if err := decoder.Decode(&candidate); err != nil {
			return nil, nil, fmt.Errorf("decode runtime package export: %w", err)
		}
		if strings.TrimSpace(candidate.Export) != "" {
			exports[candidate.ImportPath] = candidate.Export
		}
		if (candidate.ImportPath == "github.com/division-sh/swarm/internal/runtime" || strings.HasPrefix(candidate.ImportPath, "github.com/division-sh/swarm/internal/runtime/")) && len(candidate.GoFiles) != 0 {
			runtimePackages = append(runtimePackages, candidate)
		}
	}
	sort.Slice(runtimePackages, func(i, j int) bool { return runtimePackages[i].ImportPath < runtimePackages[j].ImportPath })

	fileSet := token.NewFileSet()
	exportImporter := importer.ForCompiler(fileSet, "gc", func(path string) (io.ReadCloser, error) {
		exportPath := strings.TrimSpace(exports[path])
		if exportPath == "" {
			return nil, fmt.Errorf("compiler export for %s is unavailable", path)
		}
		return os.Open(exportPath)
	})
	return &templateInstanceSemanticScanner{fileSet: fileSet, importer: exportImporter}, runtimePackages, nil
}

func (s *templateInstanceSemanticScanner) scanPackage(candidate templateInstanceSemanticPackage) ([]templateInstanceStringConversion, error) {
	sources := make(map[string]string, len(candidate.GoFiles))
	for _, name := range candidate.GoFiles {
		path := filepath.Join(candidate.Dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		sources[path] = string(raw)
	}
	return s.scanSourcePackage(candidate.ImportPath, sources)
}

func (s *templateInstanceSemanticScanner) scanSourcePackage(importPath string, sources map[string]string) ([]templateInstanceStringConversion, error) {
	paths := make([]string, 0, len(sources))
	for path := range sources {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	files := make([]*ast.File, 0, len(paths))
	for _, path := range paths {
		parsed, err := parser.ParseFile(s.fileSet, path, sources[path], 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		files = append(files, parsed)
	}
	info := &types.Info{
		Defs:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
		Types:      map[ast.Expr]types.TypeAndValue{},
		Uses:       map[*ast.Ident]types.Object{},
	}
	config := types.Config{Importer: s.importer}
	if _, err := config.Check(importPath, s.fileSet, files, info); err != nil {
		return nil, fmt.Errorf("type-check %s: %w", importPath, err)
	}

	var findings []templateInstanceStringConversion
	for _, file := range files {
		functions := templateInstanceFunctions(file, importPath, info)
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				if typed.Sel.Name != "String" {
					return true
				}
				owner := templateInstanceSemanticReceiverOwner(info.Selections[typed])
				if owner != "" {
					findings = append(findings, s.templateInstanceStringConversion(functions, typed.Pos(), owner, typed.X))
				}
			case *ast.CallExpr:
				for _, conversion := range templateInstanceSemanticIndirectStringConversions(info, typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			case *ast.ValueSpec:
				for _, conversion := range templateInstanceSemanticValueSpecConversions(info, typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			case *ast.AssignStmt:
				for _, conversion := range templateInstanceSemanticAssignmentConversions(info, typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			case *ast.ReturnStmt:
				for _, conversion := range templateInstanceSemanticReturnConversions(info, templateInstanceFunctionContaining(functions, typed.Pos()), typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			case *ast.CompositeLit:
				for _, conversion := range templateInstanceSemanticCompositeConversions(info, typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			case *ast.SendStmt:
				for _, conversion := range templateInstanceSemanticSendConversions(info, typed) {
					findings = append(findings, s.templateInstanceStringConversion(functions, conversion.expression.Pos(), conversion.owner, conversion.expression))
				}
			}
			return true
		})
	}
	return findings, nil
}

func (s *templateInstanceSemanticScanner) templateInstanceStringConversion(functions []templateInstanceFunction, position token.Pos, owner string, receiver ast.Expr) templateInstanceStringConversion {
	location := s.fileSet.Position(position)
	return templateInstanceStringConversion{
		Owner: owner, Function: templateInstanceFunctionAt(functions, position), File: filepath.ToSlash(location.Filename),
		Line: location.Line, Receiver: renderASTNode(receiver),
	}
}

type templateInstanceSemanticExpression struct {
	owner      string
	expression ast.Expr
}

func templateInstanceSemanticIndirectStringConversions(info *types.Info, call *ast.CallExpr) []templateInstanceSemanticExpression {
	var conversions []templateInstanceSemanticExpression
	if function := templateInstanceCalledFunction(info, call.Fun); templateInstanceFmtStringifies(function) {
		for _, argument := range call.Args {
			conversions = append(conversions, templateInstanceSemanticFormattingInputs(info, argument)...)
		}
	}

	if typeAndValue, ok := info.Types[call.Fun]; ok && typeAndValue.IsType() {
		if len(call.Args) == 1 && templateInstanceStringerInterface(typeAndValue.Type) {
			if owner := templateInstanceSemanticTypeOwner(info.TypeOf(call.Args[0])); owner != "" {
				conversions = append(conversions, templateInstanceSemanticExpression{owner: owner, expression: call.Args[0]})
			}
		}
		return conversions
	}

	signature, ok := info.TypeOf(call.Fun).(*types.Signature)
	if !ok {
		return conversions
	}
	for index, argument := range call.Args {
		target := templateInstanceCallArgumentTarget(signature, index, call.Ellipsis.IsValid())
		if !templateInstanceStringerInterface(target) {
			continue
		}
		if owner := templateInstanceSemanticTypeOwner(info.TypeOf(argument)); owner != "" {
			conversions = append(conversions, templateInstanceSemanticExpression{owner: owner, expression: argument})
		}
	}
	return conversions
}

func templateInstanceSemanticFormattingInputs(info *types.Info, expression ast.Expr) []templateInstanceSemanticExpression {
	if owner := templateInstanceSemanticTypeOwner(info.TypeOf(expression)); owner != "" {
		return []templateInstanceSemanticExpression{{owner: owner, expression: expression}}
	}
	switch typed := expression.(type) {
	case *ast.CallExpr:
		if typeAndValue, ok := info.Types[typed.Fun]; ok && typeAndValue.IsType() {
			var conversions []templateInstanceSemanticExpression
			for _, argument := range typed.Args {
				conversions = append(conversions, templateInstanceSemanticFormattingInputs(info, argument)...)
			}
			return conversions
		}
	case *ast.CompositeLit:
		var conversions []templateInstanceSemanticExpression
		for _, element := range typed.Elts {
			if keyValue, ok := element.(*ast.KeyValueExpr); ok {
				conversions = append(conversions, templateInstanceSemanticFormattingInputs(info, keyValue.Value)...)
				continue
			}
			conversions = append(conversions, templateInstanceSemanticFormattingInputs(info, element.(ast.Expr))...)
		}
		return conversions
	case *ast.ParenExpr:
		return templateInstanceSemanticFormattingInputs(info, typed.X)
	}
	return nil
}

func templateInstanceSemanticValueSpecConversions(info *types.Info, declaration *ast.ValueSpec) []templateInstanceSemanticExpression {
	if len(declaration.Names) != len(declaration.Values) {
		return nil
	}
	var conversions []templateInstanceSemanticExpression
	for index, value := range declaration.Values {
		object := info.Defs[declaration.Names[index]]
		if object != nil {
			conversions = append(conversions, templateInstanceSemanticStringerConversion(info, value, object.Type())...)
		}
	}
	return conversions
}

func templateInstanceSemanticAssignmentConversions(info *types.Info, assignment *ast.AssignStmt) []templateInstanceSemanticExpression {
	if len(assignment.Lhs) != len(assignment.Rhs) {
		return nil
	}
	var conversions []templateInstanceSemanticExpression
	for index, value := range assignment.Rhs {
		conversions = append(conversions, templateInstanceSemanticStringerConversion(info, value, info.TypeOf(assignment.Lhs[index]))...)
	}
	return conversions
}

func templateInstanceSemanticReturnConversions(info *types.Info, function *templateInstanceFunction, statement *ast.ReturnStmt) []templateInstanceSemanticExpression {
	if function == nil || function.results == nil || function.results.Len() != len(statement.Results) {
		return nil
	}
	var conversions []templateInstanceSemanticExpression
	for index, value := range statement.Results {
		conversions = append(conversions, templateInstanceSemanticStringerConversion(info, value, function.results.At(index).Type())...)
	}
	return conversions
}

func templateInstanceSemanticCompositeConversions(info *types.Info, literal *ast.CompositeLit) []templateInstanceSemanticExpression {
	candidate := types.Unalias(info.TypeOf(literal))
	if candidate == nil {
		return nil
	}
	var conversions []templateInstanceSemanticExpression
	switch underlying := candidate.Underlying().(type) {
	case *types.Array:
		for _, element := range literal.Elts {
			if expression, ok := element.(ast.Expr); ok {
				conversions = append(conversions, templateInstanceSemanticStringerConversion(info, expression, underlying.Elem())...)
			}
		}
	case *types.Slice:
		for _, element := range literal.Elts {
			if expression, ok := element.(ast.Expr); ok {
				conversions = append(conversions, templateInstanceSemanticStringerConversion(info, expression, underlying.Elem())...)
			}
		}
	case *types.Map:
		for _, element := range literal.Elts {
			keyValue, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			conversions = append(conversions, templateInstanceSemanticStringerConversion(info, keyValue.Key, underlying.Key())...)
			conversions = append(conversions, templateInstanceSemanticStringerConversion(info, keyValue.Value, underlying.Elem())...)
		}
	case *types.Struct:
		for index, element := range literal.Elts {
			value, target := element.(ast.Expr), types.Type(nil)
			if keyValue, ok := element.(*ast.KeyValueExpr); ok {
				value = keyValue.Value
				if name, ok := keyValue.Key.(*ast.Ident); ok {
					for fieldIndex := 0; fieldIndex < underlying.NumFields(); fieldIndex++ {
						if underlying.Field(fieldIndex).Name() == name.Name {
							target = underlying.Field(fieldIndex).Type()
							break
						}
					}
				}
			} else if index < underlying.NumFields() {
				target = underlying.Field(index).Type()
			}
			if value != nil {
				conversions = append(conversions, templateInstanceSemanticStringerConversion(info, value, target)...)
			}
		}
	}
	return conversions
}

func templateInstanceSemanticStringerConversion(info *types.Info, expression ast.Expr, target types.Type) []templateInstanceSemanticExpression {
	if !templateInstanceStringerInterface(target) {
		return nil
	}
	owner := templateInstanceSemanticTypeOwner(info.TypeOf(expression))
	if owner == "" {
		return nil
	}
	return []templateInstanceSemanticExpression{{owner: owner, expression: expression}}
}

func templateInstanceSemanticSendConversions(info *types.Info, statement *ast.SendStmt) []templateInstanceSemanticExpression {
	candidate := types.Unalias(info.TypeOf(statement.Chan))
	if candidate == nil {
		return nil
	}
	channel, ok := candidate.Underlying().(*types.Chan)
	if !ok {
		return nil
	}
	return templateInstanceSemanticStringerConversion(info, statement.Value, channel.Elem())
}

func templateInstanceCalledFunction(info *types.Info, expression ast.Expr) *types.Func {
	switch typed := expression.(type) {
	case *ast.Ident:
		function, _ := info.Uses[typed].(*types.Func)
		return function
	case *ast.SelectorExpr:
		function, _ := info.Uses[typed.Sel].(*types.Func)
		return function
	case *ast.IndexExpr:
		return templateInstanceCalledFunction(info, typed.X)
	case *ast.IndexListExpr:
		return templateInstanceCalledFunction(info, typed.X)
	case *ast.ParenExpr:
		return templateInstanceCalledFunction(info, typed.X)
	default:
		return nil
	}
}

func templateInstanceFmtStringifies(function *types.Func) bool {
	if function == nil || function.Pkg() == nil || function.Pkg().Path() != "fmt" {
		return false
	}
	switch function.Name() {
	case "Append", "Appendf", "Appendln", "Errorf", "Fprint", "Fprintf", "Fprintln", "Print", "Printf", "Println", "Sprint", "Sprintf", "Sprintln":
		return true
	default:
		return false
	}
}

func templateInstanceCallArgumentTarget(signature *types.Signature, index int, ellipsis bool) types.Type {
	parameters := signature.Params()
	if parameters == nil || parameters.Len() == 0 {
		return nil
	}
	last := parameters.Len() - 1
	if index < last || !signature.Variadic() {
		if index >= parameters.Len() {
			return nil
		}
		return parameters.At(index).Type()
	}
	variadic := parameters.At(last).Type()
	if ellipsis && index == last {
		return variadic
	}
	slice, ok := types.Unalias(variadic).(*types.Slice)
	if !ok {
		return nil
	}
	return slice.Elem()
}

func templateInstanceStringerInterface(candidate types.Type) bool {
	if candidate == nil {
		return false
	}
	underlying := types.Unalias(candidate).Underlying()
	if _, ok := underlying.(*types.Interface); !ok {
		return false
	}
	method, _, _ := types.LookupFieldOrMethod(candidate, true, nil, "String")
	function, ok := method.(*types.Func)
	if !ok {
		return false
	}
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 0 || signature.Results().Len() != 1 {
		return false
	}
	result, ok := signature.Results().At(0).Type().Underlying().(*types.Basic)
	return ok && result.Kind() == types.String
}

type templateInstanceFunction struct {
	start   token.Pos
	end     token.Pos
	id      string
	results *types.Tuple
}

func templateInstanceFunctions(file *ast.File, importPath string, info *types.Info) []templateInstanceFunction {
	var functions []templateInstanceFunction
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		id := importPath + "." + function.Name.Name
		var results *types.Tuple
		if object, ok := info.Defs[function.Name].(*types.Func); ok {
			if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
				id = importPath + "." + templateInstanceReceiverName(signature.Recv().Type()) + "." + function.Name.Name
			}
			if signature, ok := object.Type().(*types.Signature); ok {
				results = signature.Results()
			}
		}
		functions = append(functions, templateInstanceFunction{start: function.Body.Pos(), end: function.Body.End(), id: id, results: results})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.FuncLit)
		if !ok || literal.Body == nil {
			return true
		}
		id := importPath + ".<func literal>"
		if parent := templateInstanceFunctionContaining(functions, literal.Pos()); parent != nil {
			id = parent.id
		}
		var results *types.Tuple
		if signature, ok := info.TypeOf(literal.Type).(*types.Signature); ok {
			results = signature.Results()
		}
		functions = append(functions, templateInstanceFunction{start: literal.Body.Pos(), end: literal.Body.End(), id: id, results: results})
		return true
	})
	return functions
}

func templateInstanceFunctionAt(functions []templateInstanceFunction, position token.Pos) string {
	if function := templateInstanceFunctionContaining(functions, position); function != nil {
		return function.id
	}
	return "<package-scope>"
}

func templateInstanceFunctionContaining(functions []templateInstanceFunction, position token.Pos) *templateInstanceFunction {
	var match *templateInstanceFunction
	for index := range functions {
		if functions[index].start <= position && position <= functions[index].end {
			if match == nil || functions[index].end-functions[index].start < match.end-match.start {
				match = &functions[index]
			}
		}
	}
	return match
}

func templateInstanceReceiverName(receiver types.Type) string {
	receiver = types.Unalias(receiver)
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = types.Unalias(pointer.Elem())
	}
	if named, ok := receiver.(*types.Named); ok {
		return named.Obj().Name()
	}
	return types.TypeString(receiver, func(*types.Package) string { return "" })
}

func templateInstanceSemanticReceiverOwner(selection *types.Selection) string {
	if selection == nil || selection.Obj() == nil || selection.Obj().Name() != "String" {
		return ""
	}
	method, ok := selection.Obj().(*types.Func)
	if !ok {
		return ""
	}
	return templateInstanceSemanticStringMethodOwner(method)
}

func templateInstanceSemanticTypeOwner(candidate types.Type) string {
	if candidate == nil {
		return ""
	}
	method, _, _ := types.LookupFieldOrMethod(candidate, true, nil, "String")
	function, ok := method.(*types.Func)
	if !ok {
		return ""
	}
	return templateInstanceSemanticStringMethodOwner(function)
}

func templateInstanceSemanticStringMethodOwner(method *types.Func) string {
	signature, ok := method.Type().(*types.Signature)
	if !ok || signature.Recv() == nil {
		return ""
	}
	receiver := types.Unalias(signature.Recv().Type())
	for {
		pointer, ok := receiver.(*types.Pointer)
		if !ok {
			break
		}
		receiver = types.Unalias(pointer.Elem())
	}
	named, ok := receiver.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	qualified := named.Obj().Pkg().Path() + "." + named.Obj().Name()
	switch qualified {
	case "github.com/division-sh/swarm/internal/runtime/contracts.TemplateInstanceField":
		return "contracts.TemplateInstanceField"
	case "github.com/division-sh/swarm/internal/runtime/contracts.FlowInputResolutionMode":
		return "contracts.FlowInputResolutionMode"
	case "github.com/division-sh/swarm/internal/runtime/bus.TemplateInstanceLifecycleAction":
		return "bus.TemplateInstanceLifecycleAction"
	default:
		return ""
	}
}

func unclassifiedTemplateInstanceStringConversions(findings []templateInstanceStringConversion, allowedFunctions map[string]string) []templateInstanceStringConversion {
	var unauthorized []templateInstanceStringConversion
	for _, finding := range findings {
		if strings.TrimSpace(allowedFunctions[finding.Function]) == "" {
			unauthorized = append(unauthorized, finding)
		}
	}
	return unauthorized
}

func renderASTNode(node ast.Node) string {
	var out bytes.Buffer
	_ = printer.Fprint(&out, token.NewFileSet(), node)
	return out.String()
}
