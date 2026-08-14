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
		"resolveCatalogTemplateValue":  {},
		"resolveCatalogTemplateString": {},
		"resolveCatalogTemplateToken":  {},
		"channelValueAtPath":           {},
		"setChannelValueAtPath":        {},
		"splitTemplatePath":            {},
		"resolveHTTPTemplate":          {},
		"resolveActivityTemplate":      {},
		"validateChannelSchemaSubset":  {},
		"validateChannelObjectSubset":  {},
		"validateFiniteSourceEnum":     {},
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

func TestSingletonCardinalityAndCoordinatorConsumersStayOnCanonicalOwners(t *testing.T) {
	strictAllowed := map[string]struct{}{
		"internal/runtime/authoringview/view.go":                                   {},
		"internal/runtime/bootverify/workflow_composition_connect_checks.go":       {},
		"internal/runtime/bootverify/workflow_contained_state_operation_checks.go": {},
		"internal/runtime/bootverify/workflow_singleton_coordinator_checks.go":     {},
		"internal/runtime/contracts/workflow_contract_wave1_accessors.go":          {},
		"internal/runtime/core/pinrouting/connect_route_plan.go":                   {},
	}
	inspectProductionGo(t, func(path string, file *ast.File) {
		scopedDemandFunctions := map[string]struct{}{
			"BuildSingletonCoordinatorDemandProjection": {},
			"conditionExpressions":                      {},
			"dataAccumulationExpressions":               {},
			"emitFieldExpressions":                      {},
			"expressionFieldReferences":                 {},
			"wave1AllEntityWriteTargets":                {},
			"wave1ContainedStateOperations":             {},
			"wave1EntityReaderCoverageByFlow":           {},
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				if ident, ok := typed.X.(*ast.Ident); ok && ident.Name == "ref" && typed.Sel.Name == "Mode" {
					t.Errorf("%s reads raw ProjectFlowRef.Mode as behavioral authority; consume the admitted effective mode", path)
				}
			case *ast.CallExpr:
				if calledFunctionName(typed.Fun) == "ResolveFlowSingletonCoordinator" {
					if _, ok := strictAllowed[path]; !ok {
						t.Errorf("%s calls strict singleton coordinator owner outside the finite usage-demand ledger", path)
					}
				}
			}
			return true
		})
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			switch function.Name.Name {
			case "handlerEntityExpressions", "handlerEntityExpressionsForSource":
				t.Errorf("%s declares retired partial executable-reader owner %s", path, function.Name.Name)
			}
			if _, guarded := scopedDemandFunctions[function.Name.Name]; !guarded {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch calledFunctionName(call.Fun) {
				case "NodeEntries", "NodeEventHandlers", "NodeContractSource", "sortedNodeIDs", "nodeFlowID":
					t.Errorf("%s %s reads flattened node aliases; consume ScopedNodeRecords", path, function.Name.Name)
				}
				return true
			})
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
			rangeStatement, ok := node.(*ast.RangeStmt)
			if ok && rangeNormalizesCredentialProjection(rangeStatement) {
				t.Errorf("%s normalizes a credential key after admitted-owner projection", path)
			}
			call, ok := node.(*ast.CallExpr)
			if ok && normalizesCredentialStoreKey(call) {
				t.Errorf("%s normalizes a credential store key after admitted-owner mapping", path)
			}
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

func rangeNormalizesCredentialProjection(statement *ast.RangeStmt) bool {
	call, ok := statement.X.(*ast.CallExpr)
	if !ok || calledFunctionName(call.Fun) != "Credentials" {
		return false
	}
	value, ok := statement.Value.(*ast.Ident)
	if !ok {
		return false
	}
	normalizes := false
	ast.Inspect(statement.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || calledFunctionName(call.Fun) != "TrimSpace" || len(call.Args) != 1 {
			return true
		}
		identifier, ok := call.Args[0].(*ast.Ident)
		if ok && identifier.Name == value.Name {
			normalizes = true
		}
		return true
	})
	return normalizes
}

func normalizesCredentialStoreKey(call *ast.CallExpr) bool {
	if calledFunctionName(call.Fun) != "TrimSpace" || len(call.Args) != 1 {
		return false
	}
	identifier, ok := call.Args[0].(*ast.Ident)
	return ok && identifier.Name == "storeKey"
}

func TestSemanticOwnershipProducerConsumerLedgerIsClosed(t *testing.T) {
	expectedExecutionProjectionCalls := map[string]int{
		"internal/runtime/tools/platform_builtin_catalog.go": 1,
		"internal/runtime/tools/registry.go":                 3,
	}
	actualExecutionProjectionCalls := map[string]int{}
	expectedCompiledResultCalls := map[string]int{
		"internal/packs/channel.go": 1,
	}
	actualCompiledResultCalls := map[string]int{}
	expectedSchemaRelationCalls := map[string]int{
		"internal/packs/channel_relation.go":                1,
		"internal/runtime/contracts/tool_http_execution.go": 1,
	}
	actualSchemaRelationCalls := map[string]int{}
	requiredTypedFields := map[string]map[string]string{
		"toolSchemaEntryValue": {
			"category":          "ToolCategory",
			"handler":           "ToolHandlerKind",
			"effect":            "ActivityEffectClass",
			"permission":        "ToolPermission",
			"ratePolicy":        "ToolRatePolicy",
			"inputSchema":       "ToolInputSchema",
			"outputSchema":      "ToolInputSchema",
			"http":              "ToolHTTPExecution",
			"mcp":               "ToolMCPBinding",
			"responseMapping":   "ToolResponseMapping",
			"responseSuccess":   "ToolResponseSuccessPolicy",
			"credentials":       "[]toolCredentialKey",
			"managedCredential": "ToolManagedCredential",
			"compiledResult":    "ToolCompiledResultProjection",
		},
		"toolInputSchemaValue": {
			"kind":   "ToolSchemaKind",
			"format": "toolSchemaFormat",
		},
		"admittedManagedCredentialValue": {
			"key":          "toolCredentialKey",
			"grantType":    "GrantTypeKind",
			"grantModel":   "GrantModelKind",
			"tokenRequest": "admittedManagedTokenRequest",
		},
		"admittedManagedTokenRequest": {
			"clientAuth": "TokenClientAuthKind",
			"body":       "TokenBodyKind",
		},
		"executionToolValue": {
			"category":           "ToolCategory",
			"handler":            "ToolHandlerKind",
			"requiredPermission": "ToolPermission",
			"inputSchema":        "ToolInputSchema",
			"outputSchema":       "ToolInputSchema",
			"http":               "ToolHTTPExecution",
			"responseMapping":    "ToolResponseMapping",
			"responseSuccess":    "ToolResponseSuccessPolicy",
			"managedCredential":  "ToolManagedCredential",
			"ratePolicy":         "ToolRatePolicy",
			"mcp":                "ToolMCPBinding",
		},
		"compiledChannelOperation": {
			"name":          "channelPlanIdentity",
			"tool":          "channelPlanIdentity",
			"toolSchema":    "ToolSchemaEntry",
			"effect":        "ActivityEffectClass",
			"inputSchema":   "ToolInputSchema",
			"contextSchema": "ToolInputSchema",
			"outputSchema":  "ToolInputSchema",
			"input":         "[]compiledChannelMapping",
			"output":        "[]compiledChannelMapping",
		},
		"SatisfactionPlan": {
			"interfaceRef": "channelPlanIdentity",
			"provider":     "channelPlanIdentity",
			"generation":   "Generation",
		},
		"OutboundBindingPlan": {
			"id": "channelPlanIdentity",
		},
		"PrivateActivityTargetIdentity": {
			"toolID":     "channelPlanIdentity",
			"generation": "Generation",
		},
		"Authorization": {
			"manifestHash": "Hash",
			"generation":   "Generation",
			"packID":       "ID",
			"packVersion":  "Version",
		},
		"compiledHTTPToolSpec": {
			"url":     "toolTemplate",
			"headers": "map[string]toolTemplate",
			"body":    "compiledToolTemplateValue",
		},
		"compiledToolResultField": {
			"target": "toolValuePath",
			"source": "toolValuePath",
		},
		"compiledResultProjectionValue": {
			"fields":       "[]compiledToolResultField",
			"outputSchema": "ToolInputSchema",
		},
		"toolValuePath": {
			"segments": "[]toolPathSegment",
		},
		"CatalogSnapshot": {
			"byProvider": "map[string]catalogEntryValue",
			"byID":       "map[string]catalogEntryValue",
		},
		"catalogEntryValue": {
			"identity": "PackIdentity",
		},
		"InboundAdmissionPlan": {
			"packIdentity": "PackIdentity",
		},
		"PackIdentity": {
			"manifestHash": "Hash",
			"id":           "ID",
			"version":      "Version",
			"packType":     "packIdentityType",
			"source":       "PackSource",
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
			switch calledFunctionName(call.Fun) {
			case "WithCompiledResult":
				actualCompiledResultCalls[path]++
			case "WithToolCompiledResult":
				t.Errorf("%s constructs compiled-result authority outside the channel compiler", path)
			case "ValidateAssignableTo":
				actualSchemaRelationCalls[path]++
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
	if !reflect.DeepEqual(actualCompiledResultCalls, expectedCompiledResultCalls) {
		t.Fatalf("compiled-result producers = %#v, want finite ledger %#v", actualCompiledResultCalls, expectedCompiledResultCalls)
	}
	if !reflect.DeepEqual(actualSchemaRelationCalls, expectedSchemaRelationCalls) {
		t.Fatalf("schema-relation consumers = %#v, want finite ledger %#v", actualSchemaRelationCalls, expectedSchemaRelationCalls)
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
	case *ast.ArrayType:
		return "[]" + astTypeName(typed.Elt)
	case *ast.MapType:
		return "map[" + astTypeName(typed.Key) + "]" + astTypeName(typed.Value)
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
