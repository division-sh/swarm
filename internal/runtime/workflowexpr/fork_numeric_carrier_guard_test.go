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

func TestWorkflowForkNumericCarriersUseCurrentOwner(t *testing.T) {
	if findings := forkNumericCarrierFindings(t, nil); len(findings) != 0 {
		t.Fatal(findings)
	}
}

func TestWorkflowForkNumericCarrierGuardRejectsErasingDecode(t *testing.T) {
	path := filepath.Join(workflowProjectionRuntimeRoot(t), "runfork", "entity_ownership.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const anchor = "\tif event.SourceEventID == \"\" {"
	// Keep the opaque decoder too: mere presence of the canonical call is not proof.
	mutated := strings.Replace(string(raw), anchor, "\tvar arbitrary any\n\t_ = json.Unmarshal(event.Payload, &arbitrary)\n"+anchor, 1)
	if mutated == string(raw) {
		t.Fatal("hostile overlay did not modify the owner")
	}
	findings := forkNumericCarrierFindings(t, map[string][]byte{path: []byte(mutated)})
	if len(findings) != 1 || !strings.Contains(findings[0], "erasing JSON destination any") {
		t.Fatalf("missed erasing decode in otherwise approved owner: %v", findings)
	}
}

func forkNumericCarrierFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := filepath.Clean(filepath.Join(workflowProjectionRuntimeRoot(t), "..", ".."))
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/runfork", "./internal/runtime/runforkexecution", "./internal/store/internal/backend/runforkpersistence")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("fork numeric census requires typechecking: %v", err)
	}
	const base = "github.com/division-sh/swarm/internal/"
	consumers := map[string]bool{
		base + "runtime/runforkexecution.projectSelectedContractSourceEvents":                         false,
		"(" + base + "store/internal/backend/runforkpersistence.runForkSourceStateAdmission).project": false,
	}
	var findings []string
	opaque := false
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				owner := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
				name := owner.FullName()
				canonical := name == base+"runtime/runfork.ProjectSelectedContractSourceEvent"
				_, consumer := consumers[name]
				if !canonical && !consumer {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					target, ok := pkg.TypesInfo.Uses[sel.Sel].(*types.Func)
					if !ok || target.Pkg() == nil {
						return true
					}
					if consumer && target.FullName() == base+"runtime/runfork.ProjectSelectedContractSourceEvent" {
						consumers[name] = true
					}
					if canonical && target.Pkg().Path() == "encoding/json" && target.Name() == "Unmarshal" && len(call.Args) == 2 {
						pointer, ok := pkg.TypesInfo.TypeOf(call.Args[1]).(*types.Pointer)
						if !ok {
							findings = append(findings, name+": invalid JSON destination")
							return true
						}
						typ := pointer.Elem()
						if typ.String() == "map[string]encoding/json.RawMessage" {
							opaque = true
							return true
						}
						if !types.Identical(typ, types.Typ[types.String]) {
							findings = append(findings, fmt.Sprintf("%s: erasing JSON destination %s", name, typ))
						}
					}
					return true
				})
			}
		}
	}
	if !opaque {
		findings = append(findings, "canonical fork activity projection lost opaque payload decoding")
	}
	for name, found := range consumers {
		if !found {
			findings = append(findings, name+": missing exact projection owner")
		}
	}
	return findings
}
