package runtime_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestRegisteredToolSemanticReconstructionIsAbsent(t *testing.T) {
	forbiddenTypes := map[string]struct{}{
		"RegisteredTool":   {},
		"SourceCapability": {},
	}
	forbiddenFunctions := map[string]struct{}{
		"registeredToolFromContract":   {},
		"executionToolFromContract":    {},
		"newExecutionTool":             {},
		"normalizeImplementationClass": {},
		"toolRequiredPermission":       {},
		"sourceProvenance":             {},
		"BaseSemanticSource":           {},
		"CloneEventCatalogEntry":       {},
		"CloneEventCatalogEntries":     {},
	}
	inspectProductionGo(t, func(path string, file *ast.File) {
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					if _, forbidden := forbiddenTypes[typeSpec.Name.Name]; forbidden {
						t.Errorf("%s declares retired semantic owner %s", path, typeSpec.Name.Name)
					}
				}
			case *ast.FuncDecl:
				if _, forbidden := forbiddenFunctions[typed.Name.Name]; forbidden {
					t.Errorf("%s declares retired semantic reconstruction function %s", path, typed.Name.Name)
				}
			}
		}
	})
}

func TestSchemaToolPlanPackagesRejectFreeSemanticStringsAndConsumerNormalization(t *testing.T) {
	closed := map[string]reflect.Type{
		"ToolInputSchema":               reflect.TypeOf(runtimecontracts.ToolInputSchema{}),
		"ToolSchemaEntry":               reflect.TypeOf(runtimecontracts.ToolSchemaEntry{}),
		"SatisfactionPlan":              reflect.TypeOf(packs.SatisfactionPlan{}),
		"OutboundBindingPlan":           reflect.TypeOf(packs.OutboundBindingPlan{}),
		"PrivateActivityTargetIdentity": reflect.TypeOf(packs.PrivateActivityTargetIdentity{}),
		"ExecutionTool":                 reflect.TypeOf(runtimetools.ExecutionTool{}),
		"ChannelActivityTarget":         reflect.TypeOf(runtimepipeline.ChannelActivityTarget{}),
		"PlanGeneration":                reflect.TypeOf(plangeneration.Generation{}),
		"TriggerCatalogGeneration":      reflect.TypeOf(triggergeneration.Generation{}),
		"SemanticCapabilities":          reflect.TypeOf(semanticview.Capabilities{}),
		"ConnectorGenerationPermission": reflect.TypeOf(semanticview.ConnectorGenerationPermission{}),
		"ConnectorGenerationSurface":    reflect.TypeOf(semanticview.ConnectorGenerationSurface{}),
		"ConnectorImportSource":         reflect.TypeOf(semanticview.ConnectorImportSource{}),
		"ProviderOutputAuthorization":   reflect.TypeOf(runtimeprovideroutput.Authorization{}),
		"PackIdentity":                  reflect.TypeOf(packs.PackIdentity{}),
		"PackSource":                    reflect.TypeOf(packs.PackSource{}),
	}
	for name, typ := range closed {
		for index := 0; index < typ.NumField(); index++ {
			if field := typ.Field(index); field.IsExported() {
				t.Errorf("%s exposes semantic field %s", name, field.Name)
			}
		}
	}

	inspectProductionGo(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok {
				return true
			}
			for _, name := range field.Names {
				if name.Name == "PlanGeneration" && isStringType(field.Type) {
					t.Errorf("%s declares free-string PlanGeneration authority", path)
				}
			}
			return true
		})
	})
}

func TestSemanticOwnershipProducerConsumerLedgerIsClosed(t *testing.T) {
	expectedExecutionProjectionCalls := map[string]int{
		"internal/runtime/tools/platform_builtin_catalog.go": 1,
		"internal/runtime/tools/registry.go":                 3,
	}
	actualExecutionProjectionCalls := map[string]int{}
	requiredTypedFields := map[string]map[string]string{
		"toolSchemaEntryValue": {
			"category":   "ToolCategory",
			"handler":    "ToolHandlerKind",
			"effect":     "ActivityEffectClass",
			"permission": "ToolPermission",
			"ratePolicy": "ToolRatePolicy",
		},
		"compiledChannelOperation": {
			"effect": "ActivityEffectClass",
		},
		"Authorization": {
			"generation": "Generation",
		},
	}
	seenTypedFields := map[string]map[string]bool{}

	inspectProductionGo(t, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			identifier, ok := call.Fun.(*ast.Ident)
			if ok && identifier.Name == "executionToolFromAdmitted" {
				actualExecutionProjectionCalls[path]++
			}
			return true
		})
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					required := requiredTypedFields[typeSpec.Name.Name]
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok || len(required) == 0 {
						continue
					}
					if seenTypedFields[typeSpec.Name.Name] == nil {
						seenTypedFields[typeSpec.Name.Name] = map[string]bool{}
					}
					for _, field := range structType.Fields.List {
						for _, name := range field.Names {
							wantType, tracked := required[name.Name]
							if !tracked {
								continue
							}
							if astTypeName(field.Type) != wantType {
								t.Errorf("%s %s.%s type = %s, want %s", path, typeSpec.Name.Name, name.Name, astTypeName(field.Type), wantType)
							}
							seenTypedFields[typeSpec.Name.Name][name.Name] = true
						}
					}
				}
			case *ast.FuncDecl:
				if typed.Name.Name != "executionToolFromAdmitted" {
					continue
				}
				ast.Inspect(typed.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch calledFunctionName(call.Fun) {
					case "TrimSpace", "Parse", "Normalize", "normalizeImplementationClass", "toolRequiredPermission":
						t.Errorf("%s executionToolFromAdmitted reinterprets admitted semantics through %s", path, calledFunctionName(call.Fun))
					}
					return true
				})
			}
		}
	})

	if !reflect.DeepEqual(actualExecutionProjectionCalls, expectedExecutionProjectionCalls) {
		t.Fatalf("execution-tool projection consumers = %#v, want finite ledger %#v", actualExecutionProjectionCalls, expectedExecutionProjectionCalls)
	}
	for owner, fields := range requiredTypedFields {
		for field := range fields {
			if !seenTypedFields[owner][field] {
				t.Errorf("typed semantic owner %s.%s is missing from the producer/consumer ledger", owner, field)
			}
		}
	}
}

func inspectProductionGo(t *testing.T, inspect func(string, *ast.File)) {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		inspect(filepath.ToSlash(relative), file)
		return nil
	})
	if err != nil {
		t.Fatalf("inspect production Go: %v", err)
	}
}

func isStringType(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "string"
}

func astTypeName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	case *ast.StarExpr:
		return astTypeName(typed.X)
	default:
		return ""
	}
}

func calledFunctionName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}
