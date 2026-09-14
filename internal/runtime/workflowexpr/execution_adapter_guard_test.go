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
		"engine/fan_out_evaluator.go": {{"e.bindFrameExpressionSchemas(frame)", ""}},
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
		overlay[path] = []byte(source)
	}
	findings := strings.Join(executionAdapterFindings(t, overlay), "\n")
	for _, want := range []string{"missing strict original-byte admission", "erasing execution writer", "EvaluateFanOutOrdinal missing shared schema binding", "hostileLocalSchema competing frame schema writer", "hostileConstructor unaccounted frame constructor"} {
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
	}, "./internal/runtime", "./internal/runtime/engine")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("execution adapter guard requires compiler-resolved packages: %v", err)
	}
	const base = "github.com/division-sh/swarm/internal/runtime/"
	var findings []string
	seen := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner := pkg.TypesInfo.Defs[fn.Name].(*types.Func).FullName()
				admitter := owner == strings.TrimSuffix(base, "/")+".NewRuntimePayloadAdmitter"
				binder := owner == "(*"+base+"engine.Executor).bindFrameExpressionSchemas"
				constructor := owner == "(*"+base+"engine.Executor).newExecutionFrame" || owner == "(*"+base+"engine.Executor).EvaluateFanOutOrdinal"
				if admitter || binder || constructor {
					seen[fn.Name.Name] = true
				}
				calls := map[string]bool{}
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
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					callee, ok := pkg.TypesInfo.Uses[sel.Sel].(*types.Func)
					if !ok || callee.Pkg() == nil {
						return true
					}
					calls[callee.FullName()] = true
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
	for _, owner := range []string{"NewRuntimePayloadAdmitter", "bindFrameExpressionSchemas", "newExecutionFrame", "EvaluateFanOutOrdinal"} {
		if !seen[owner] {
			findings = append(findings, "missing adapter owner "+owner)
		}
	}
	return findings
}
