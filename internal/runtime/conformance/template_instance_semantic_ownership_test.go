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

func arbitraryReceiverNames(identity runtimecontracts.TemplateInstanceField, resolution modeAlias, outcome *runtimebus.TemplateInstanceLifecycleAction, promoted embeddedMode) {
	_ = identity.String()
	_ = resolution.String()
	_ = outcome.String()
	_ = fieldResult().String()
	render := resolution.String
	_ = render()
	_ = promoted.String()
}
`},
	)
	if err != nil {
		t.Fatalf("scan hostile semantic receivers: %v", err)
	}
	if len(findings) != 6 {
		t.Fatalf("compiler-resolved hostile findings = %#v, want six arbitrary-name/alias/pointer/call-result/method-value/promoted-method conversions", findings)
	}
	owners := map[string]int{}
	for _, finding := range findings {
		owners[finding.Owner]++
	}
	for owner, want := range map[string]int{
		"contracts.TemplateInstanceField":     2,
		"contracts.FlowInputResolutionMode":   3,
		"bus.TemplateInstanceLifecycleAction": 1,
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
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "String" {
				return true
			}
			owner := templateInstanceSemanticReceiverOwner(info.Selections[selector])
			if owner == "" {
				return true
			}
			position := s.fileSet.Position(selector.Pos())
			findings = append(findings, templateInstanceStringConversion{
				Owner: owner, Function: templateInstanceFunctionAt(functions, selector.Pos()), File: filepath.ToSlash(position.Filename),
				Line: position.Line, Receiver: renderASTNode(selector.X),
			})
			return true
		})
	}
	return findings, nil
}

type templateInstanceFunction struct {
	start token.Pos
	end   token.Pos
	id    string
}

func templateInstanceFunctions(file *ast.File, importPath string, info *types.Info) []templateInstanceFunction {
	var functions []templateInstanceFunction
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		id := importPath + "." + function.Name.Name
		if object, ok := info.Defs[function.Name].(*types.Func); ok {
			if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
				id = importPath + "." + templateInstanceReceiverName(signature.Recv().Type()) + "." + function.Name.Name
			}
		}
		functions = append(functions, templateInstanceFunction{start: function.Body.Pos(), end: function.Body.End(), id: id})
	}
	return functions
}

func templateInstanceFunctionAt(functions []templateInstanceFunction, position token.Pos) string {
	for _, function := range functions {
		if function.start <= position && position <= function.end {
			return function.id
		}
	}
	return "<package-scope>"
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
