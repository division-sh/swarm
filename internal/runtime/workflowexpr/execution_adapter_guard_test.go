package workflowexpr

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestExecutionAdapterOwners(t *testing.T) {
	if findings := executionAdapterFindings(t, nil); len(findings) != 0 {
		t.Fatal(findings)
	}
}

func TestExecutionAdapterGuardRejectsCompetingInterpretations(t *testing.T) {
	root := workflowProjectionRuntimeRoot(t)
	overlay := map[string][]byte{}
	for relative, changes := range map[string][][2]string{
		"eventbus_logger.go": {
			{"canonicaljson.DecodeInto(payload, &admittedObject)", "canonicaljson.DecodePreservingNumberLexemes(payload, &admittedObject)"},
			{"canonicaljson.MarshalPreservingNumberKinds(decoded)", "canonicaljson.Bytes(decoded)"},
		},
		"engine/fan_out_evaluator.go":        {{"e.bindFrameExpressionSchemas(frame)", ""}},
		"pipeline/activity_engine.go":        {{"raw, err := canonicaljson.MarshalPreservingNumberKinds(payload)", "_, _ = canonicaljson.FromGo(payload)\nraw, err := canonicaljson.MarshalPreservingNumberKinds(payload)"}},
		"workflowexpr/numeric_expression.go": {},
		"../providerconnectors/mock_response_plan.go": {
			{"value, err = canonicaljson.FromGo(response)", "raw, encodeErr := json.Marshal(response)\nif encodeErr != nil { return nil, encodeErr }; value, err = canonicaljson.Decode(raw)"},
			{"return workflowexpr.ProjectSemanticValue(r.value)", "raw, err := canonicaljson.Encode(r.value); if err != nil { return nil, err }; var value any; err = json.Unmarshal(raw, &value); return value, err"},
		},
	} {
		path := filepath.Join(root, relative)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source := string(raw)
		for _, change := range changes {
			next := strings.Replace(source, change[0], change[1], 1)
			if next == source {
				t.Fatalf("missing hostile anchor %q", change[0])
			}
			source = next
		}
		if relative == "engine/fan_out_evaluator.go" {
			source += "\nfunc hostileLocalSchema(arbitrary *executionFrame) { arbitrary.entityType = nil }\n"
			source += "\nfunc hostileConstructor() executionFrame { return executionFrame{} }\n"
		}
		if relative == "workflowexpr/numeric_expression.go" {
			source += "\nfunc hostileNumericPlanner(arbitrary *cel.Env, checked *cel.Ast) (cel.Program, error) { return arbitrary.Program(checked) }\n"
		}
		if relative == "../providerconnectors/mock_response_plan.go" {
			source += "\nfunc hostileMockDecoder(arbitrary AdmittedMockResponse) (any, error) { raw, err := canonicaljson.Encode(arbitrary.value); if err != nil { return nil, err }; var value any; err = json.Unmarshal(raw, &value); return value, err }\n"
		}
		overlay[path] = []byte(source)
	}
	findings := strings.Join(executionAdapterFindings(t, overlay), "\n")
	for _, want := range []string{"missing strict original-byte admission", "erasing execution writer", "PrepareFanOutEvaluation missing shared schema binding", "hostileLocalSchema competing frame schema writer", "hostileConstructor unaccounted frame constructor", "hostileNumericPlanner bypasses checked numeric planning", "activity result writer reinterprets execution kinds as semantic DTOs", "NewMockResponsePlan missing numeric/JSON owner", "Materialize competing mock codec", "hostileMockDecoder competing mock codec"} {
		if !strings.Contains(findings, want) {
			t.Fatalf("missing %q: %s", want, findings)
		}
	}
}

func executionAdapterFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := filepath.Clean(filepath.Join(workflowProjectionRuntimeRoot(t), "..", ".."))
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime", "./internal/runtime/engine", "./internal/runtime/workflowexpr", "./internal/runtime/entityruntime", "./internal/runtime/pipeline", "./internal/store/internal/backend/activityjournal", "./internal/providerconnectors")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("execution adapter guard requires compiler-resolved packages: %v", err)
	}
	const base = "github.com/division-sh/swarm/internal/runtime/"
	const mock = "github.com/division-sh/swarm/internal/providerconnectors"
	var findings []string
	seen := map[string]bool{}
	required := map[string][]string{
		mock + ".NewMockResponsePlan":                                                                {base + "canonicaljson.Decode", base + "canonicaljson.FromGo"},
		"(*" + mock + ".MockResponsePlan).Admit":                                                     {base + "workflowexpr.ProjectSemanticValue"},
		"(" + mock + ".AdmittedMockResponse).Materialize":                                            {base + "workflowexpr.ProjectSemanticValue"},
		base + "workflowexpr.compileValueExpression":                                                 {base + "workflowexpr.validateWorkflowNumericEvidence", base + "workflowexpr.validateWorkflowResultType"},
		base + "workflowexpr.EvalValueResultWithOptions":                                             {base + "workflowexpr.PrepareValueExpression"},
		base + "workflowexpr.PrepareValueExpression":                                                 {base + "workflowexpr.workflowProgram", base + "workflowexpr.compileValueExpression"},
		"(*" + base + "workflowexpr.PreparedValueExpression).Eval":                                   {base + "workflowexpr.ProjectCELValue", base + "workflowexpr.MissingEntityReferences", base + "workflowexpr.normalizeCELResult"},
		"(*" + base + "engine.Executor).PrepareFanOutEvaluation":                                     {"(*" + base + "engine.Executor).bindFrameExpressionSchemas", "(*" + base + "engine.Executor).resolveEmitRoute", base + "workflowexpr.PrepareValueExpression"},
		"(*" + base + "engine.FanOutEvaluation).EvaluateOrdinal":                                     {base + "fanoutobligation.PrepareOrdinalEmission", "(*" + base + "engine.Executor).shapeEmitPayloadWithContext", "(*" + base + "engine.Executor).newEmitIntentWithEnvelope"},
		"(*" + base + "workflowexpr.StructuralPredicateEnv).PredicateProgram":                        {base + "workflowexpr.workflowProgram"},
		base + "workflowexpr.workflowProgram":                                                        {base + "workflowexpr.validateWorkflowNumericEvidence"},
		base + "workflowexpr.projectCELValue":                                                        {base + "canonicaljson.NormalizeRuntimeNumber"},
		base + "entityruntime.normalizeValueForType":                                                 {base + "canonicaljson.NormalizeRuntimeNumber", base + "entityruntime.normalizeJSONFieldValue"},
		base + "entityruntime.normalizeJSONFieldValue":                                               {base + "canonicaljson.CloneRuntimeValue"},
		base + "entityruntime.normalizePartialObjectValue":                                           {base + "canonicaljson.CloneRuntimeValue"},
		"(" + base + "pipeline.pipelineActivityDispatcher).publishActivityResultWithID":              {base + "canonicaljson.MarshalPreservingNumberKinds"},
		"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal.decodePayload": {base + "canonicaljson.DecodeInto", base + "canonicaljson.DecodePreservingNumberLexemes", base + "canonicaljson.CloneRuntimeValue"},
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner := pkg.TypesInfo.Defs[fn.Name].(*types.Func).FullName()
				seen[owner] = true
				admitter := owner == strings.TrimSuffix(base, "/")+".NewRuntimePayloadAdmitter"
				binder := owner == "(*"+base+"engine.Executor).bindFrameExpressionSchemas"
				constructor := owner == "(*"+base+"engine.Executor).newExecutionFrame" || owner == "(*"+base+"engine.Executor).PrepareFanOutEvaluation"
				if admitter || binder || constructor {
					seen[fn.Name.Name] = true
				}
				calls := map[string]bool{}
				mockConsumer := owner == mock+".NewMockResponsePlan"
				if function, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func); ok {
					if signature, ok := function.Type().(*types.Signature); ok {
						for _, fields := range []*types.Tuple{signature.Params(), signature.Results()} {
							for i := 0; i < fields.Len(); i++ {
								typ := strings.TrimPrefix(fields.At(i).Type().String(), "*")
								mockConsumer = mockConsumer || typ == mock+".AdmittedMockResponse" || typ == mock+".MockResponsePlan"
							}
						}
						if receiver := signature.Recv(); receiver != nil {
							typ := strings.TrimPrefix(receiver.Type().String(), "*")
							mockConsumer = mockConsumer || typ == mock+".AdmittedMockResponse" || typ == mock+".MockResponsePlan"
						}
					}
				}
				var strictInput types.Object
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if literal, ok := n.(*ast.CompositeLit); ok {
						if typ := pkg.TypesInfo.TypeOf(literal); typ != nil && typ.String() == base+"engine.executionFrame" {
							if !constructor {
								findings = append(findings, fn.Name.Name+" unaccounted frame constructor")
							}
							for _, element := range literal.Elts {
								if entry, ok := element.(*ast.KeyValueExpr); ok {
									if key, ok := entry.Key.(*ast.Ident); ok && (key.Name == "entityType" || key.Name == "payloadType") {
										findings = append(findings, fn.Name.Name+" competing literal schema writer")
									}
								}
							}
						}
					}
					if assign, ok := n.(*ast.AssignStmt); ok {
						for _, lhs := range assign.Lhs {
							field, ok := lhs.(*ast.SelectorExpr)
							if !ok || binder {
								continue
							}
							selection := pkg.TypesInfo.Selections[field]
							if selection != nil && selection.Recv().String() == "*"+base+"engine.executionFrame" && (field.Sel.Name == "entityType" || field.Sel.Name == "payloadType") {
								findings = append(findings, fn.Name.Name+" competing frame schema writer")
							}
						}
					}
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					var callee *types.Func
					switch fun := call.Fun.(type) {
					case *ast.SelectorExpr:
						callee, _ = pkg.TypesInfo.Uses[fun.Sel].(*types.Func)
					case *ast.Ident:
						callee, _ = pkg.TypesInfo.Uses[fun].(*types.Func)
					}
					if callee == nil || callee.Pkg() == nil {
						return true
					}
					calls[callee.FullName()] = true
					if mockConsumer && callee.Pkg().Path() == "encoding/json" {
						findings = append(findings, fn.Name.Name+" competing mock codec")
					}
					if owner == "("+base+"pipeline.pipelineActivityDispatcher).publishActivityResultWithID" && callee.Pkg().Path() == base+"canonicaljson" &&
						(callee.Name() == "FromGo" || callee.Name() == "Bytes" || callee.Name() == "ValueInto" || callee.Name() == "DecodeInto") {
						findings = append(findings, "activity result writer reinterprets execution kinds as semantic DTOs")
					}
					if pkg.PkgPath == base+"workflowexpr" && callee.Pkg().Path() == "github.com/google/cel-go/cel" &&
						(callee.Name() == "Program" || callee.Name() == "PlanProgram") && owner != base+"workflowexpr.workflowProgram" {
						findings = append(findings, fn.Name.Name+" bypasses checked numeric planning")
					}
					if admitter && callee.Pkg().Path() == base+"canonicaljson" && len(call.Args) > 0 {
						id, isVariable := call.Args[0].(*ast.Ident)
						switch callee.Name() {
						case "DecodeInto":
							if isVariable {
								strictInput = pkg.TypesInfo.ObjectOf(id)
							}
						case "DecodePreservingNumberLexemes":
							if !isVariable || strictInput == nil || strictInput != pkg.TypesInfo.ObjectOf(id) {
								findings = append(findings, "missing strict original-byte admission before execution decode")
							}
						case "Bytes":
							if isVariable {
								findings = append(findings, "erasing execution writer in payload admission")
							}
						}
					}
					return true
				})
				for _, callee := range required[owner] {
					if !calls[callee] {
						findings = append(findings, fn.Name.Name+" missing numeric/JSON owner "+callee)
					}
				}
				if constructor && !calls["(*"+base+"engine.Executor).bindFrameExpressionSchemas"] {
					findings = append(findings, fn.Name.Name+" missing shared schema binding")
				}
				if binder && !calls[base+"semanticview.ResolveEntityStructuralType"] {
					findings = append(findings, "shared binding missing exact entity schema owner")
				}
				if admitter && !calls[base+"canonicaljson.MarshalPreservingNumberKinds"] {
					findings = append(findings, "payload admission missing kind-preserving writer")
				}
			}
		}
	}
	for _, owner := range []string{"NewRuntimePayloadAdmitter", "bindFrameExpressionSchemas", "newExecutionFrame", "PrepareFanOutEvaluation"} {
		if !seen[owner] {
			findings = append(findings, "missing adapter owner "+owner)
		}
	}
	for owner := range required {
		if !seen[owner] {
			findings = append(findings, "missing numeric/JSON owner "+owner)
		}
	}
	return findings
}
