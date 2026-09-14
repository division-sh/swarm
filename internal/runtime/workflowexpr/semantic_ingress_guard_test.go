package workflowexpr

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// These are semantic execution exits, not a file-level permission to decode
// arbitrary DTOs or reinterpret workflow doubles. Kind-bearing siblings retain
// the independent reader/writer registry in semantic_projection_guard_test.go.
var semanticExecutionExits = map[string][]string{
	"internal/apiv1/operator_event_publish.go::eventPublicationPayload":                              {"FromGo", "ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/cliapp/test_command.go::materializeScenarioSemanticPayload":                            {"Decode", "ProjectSemanticValue"},
	"internal/store/internal/backend/pipelinepersistence/scenario_setup.go::scenarioSetupEntityJSON": {"FromGo", "ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/store/internal/backend/pipelinepersistence/fan_out_owner.go::collectionRangeFromJSONL": {"Decode", "ProjectSemanticValue"},
	"internal/runtime/engine/executor.go::decodeComputeModuleOutput":                                 {"Decode", "ProjectSemanticValue"},
	"internal/runtime/pipeline/activity_engine.go::publishActivityResultWithID":                      {"FromGo", "ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/runtime/pipeline/workflow_gate_decision.go::workflowGateOutcomeEvent":                  {"ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/runtime/pipeline/decision_card_mutation.go::decisionCardDecidedEvent":                  {"ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/runtime/genericschedule/owner.go::fire":                                                {"ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/runtime/pipeline/workflow_gate_decision.go::proposedEffectOutcomeEvent":                {"FromGo", "ProjectSemanticValue", "MarshalPreservingNumberKinds"},
	"internal/runtime/pipeline/workflow_gate_decision.go::handleHumanTaskDecisionCard":               {"FromGo", "ProjectSemanticValue", "MarshalPreservingNumberKinds"},
}

func TestWorkflowSemanticBoundaryGuard(t *testing.T) {
	findings := semanticBoundaryFindings(t, nil)
	for _, finding := range findings {
		t.Error(finding)
	}
}

func TestWorkflowSemanticBoundaryGuardRejectsBypasses(t *testing.T) {
	root := filepath.Clean(filepath.Join(workflowProjectionRuntimeRoot(t), "..", ".."))
	path := filepath.Join(root, "internal/apiv1/operator_event_publish.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(raw), "workflowexpr.ProjectSemanticValue(admitted)", "workflowexpr.ProjectCELValue(admitted.Interface())", 1)
	mutated = strings.Replace(mutated, "canonicaljson.MarshalPreservingNumberKinds(projected)", "canonicaljson.MarshalPreservingNumberKinds(cloned)", 1)
	mutated = strings.Replace(mutated, "projected, err := workflowexpr.ProjectCELValue", "_, err = workflowexpr.ProjectCELValue", 1)
	mutated += `
func hostileApprovedFileBypass(arbitrary interface{ Interface() any }) any { return arbitrary.Interface() }
`
	// A concrete semantic receiver is checked independently of its variable name.
	mutated += `
func hostileSemanticReceiver() any {
  totallyDifferentName, _ := canonicaljson.Decode([]byte("7"))
  return totallyDifferentName.Interface()
}
`
	findings := strings.Join(semanticBoundaryFindings(t, map[string][]byte{path: []byte(mutated)}), "\n")
	for _, want := range []string{"eventPublicationPayload missing ProjectSemanticValue", "eventPublicationPayload writer bypasses projected value", "hostileSemanticReceiver unclassified semantic DTO exit"} {
		if !strings.Contains(findings, want) {
			t.Fatalf("missing hostile finding %q:\n%s", want, findings)
		}
	}
}

func semanticBoundaryFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := filepath.Clean(filepath.Join(workflowProjectionRuntimeRoot(t), "..", ".."))
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports},
		"./internal/apiv1", "./internal/cliapp", "./internal/runtime/workflowexpr", "./internal/runtime/engine", "./internal/runtime/pipeline", "./internal/runtime/genericschedule", "./internal/store/internal/backend/pipelinepersistence")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("semantic boundary guard requires compiler-resolved packages")
	}
	seen := map[string]bool{}
	var findings []string
	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			path, err := filepath.Rel(root, pkg.CompiledGoFiles[i])
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.ToSlash(path)
			guarded := false
			for key := range semanticExecutionExits {
				if strings.HasPrefix(key, path+"::") {
					guarded = true
				}
			}
			if !guarded {
				continue
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				key := path + "::" + fn.Name.Name
				required, selected := semanticExecutionExits[key]
				calls := map[string]bool{}
				projected := map[types.Object]bool{}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					assignment, ok := n.(*ast.AssignStmt)
					if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) == 0 {
						return true
					}
					call, ok := assignment.Rhs[0].(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					obj, ok := pkg.TypesInfo.Uses[sel.Sel].(*types.Func)
					if !ok || obj.Pkg() == nil {
						return true
					}
					if obj.Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/workflowexpr" && obj.Name() == "ProjectSemanticValue" {
						if id, ok := assignment.Lhs[0].(*ast.Ident); ok {
							projected[pkg.TypesInfo.ObjectOf(id)] = true
						}
					}
					return true
				})
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					obj, ok := pkg.TypesInfo.Uses[sel.Sel].(*types.Func)
					if !ok || obj.Pkg() == nil {
						return true
					}
					owner := obj.Pkg().Path()
					if owner == "github.com/division-sh/swarm/internal/runtime/canonicaljson" || owner == "github.com/division-sh/swarm/internal/runtime/workflowexpr" {
						calls[obj.Name()] = true
						if selected && obj.Name() == "MarshalPreservingNumberKinds" {
							var variable types.Object
							if len(call.Args) == 1 {
								if id, ok := call.Args[0].(*ast.Ident); ok {
									variable = pkg.TypesInfo.ObjectOf(id)
								}
							}
							if variable == nil || !projected[variable] {
								findings = append(findings, key+" writer bypasses projected value")
							}
						}
						if selected && (obj.Name() == "DecodeInto" || obj.Name() == "ValueInto") {
							findings = append(findings, key+" generic DTO execution bypass")
						}
					}
					if owner == "github.com/division-sh/swarm/internal/runtime/semanticvalue" && obj.Name() == "Interface" && !semanticDTOExitAllowed(key) {
						findings = append(findings, key+" unclassified semantic DTO exit")
					}
					return true
				})
				if selected {
					seen[key] = true
					for _, symbol := range required {
						if !calls[symbol] {
							findings = append(findings, key+" missing "+symbol)
						}
					}
				}
			}
		}
	}
	for key := range semanticExecutionExits {
		if !seen[key] {
			findings = append(findings, fmt.Sprintf("missing execution boundary %s", key))
		}
	}
	return findings
}

func semanticDTOExitAllowed(function string) bool {
	// Scope-local protocol/evidence DTOs. None can replace a required execution
	// projection above; that is checked separately in its exact enclosing function.
	switch function {
	case "internal/runtime/pipeline/workflow_gate_decision.go::proposedEffectOutcomeEvent",
		"internal/runtime/pipeline/workflow_gate_decision.go::handleHumanTaskDecisionCard",
		"internal/runtime/pipeline/activity_engine.go::prepareActivityHTTPTool",
		"internal/runtime/pipeline/activity_engine.go::buildProposedEffectCard",
		"internal/runtime/pipeline/activity_engine.go::activityManagedCredentialInputValue":
		return true
	default:
		return false
	}
}
