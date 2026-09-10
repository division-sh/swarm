package semanticview_test

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// This guards lifecycle-coordinate construction in the four C1 owner/consumer
// packages, not runtime activation authority or the complete routing system.
func TestCompiledLifecycleEmitterCoordinateOwnership(t *testing.T) {
	if findings := lifecycleCoordinateFindings(t, nil); len(findings) != 0 {
		t.Fatalf("lifecycle endpoint coordinates constructed outside census owner: %q", findings)
	}
}

func TestCompiledLifecycleEmitterCoordinateOwnershipRejectsHostileWrites(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	ownerPath := filepath.Join(root, "internal/runtime/semanticview/lifecycle_endpoints.go")
	owner, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	owner = append(owner, []byte(`
func hostileSameFile(arbitrary *AuthoredEventEndpoint) { arbitrary.StageID = "forged" }
`)...)
	overlay := map[string][]byte{
		ownerPath: owner,
		filepath.Join(root, "internal/runtime/bootverify/lifecycle_coordinate_hostile.go"): []byte(`package bootverify
import sv "github.com/division-sh/swarm/internal/runtime/semanticview"
type endpointAlias = sv.AuthoredEventEndpoint
func hostileAlias() endpointAlias { return endpointAlias{Verdict: "forged", LoopID: "forged"} }
func hostileReceiver(unexpected *endpointAlias) { unexpected.DecisionID = "forged" }
`),
	}
	got := lifecycleCoordinateFindings(t, overlay)
	want := []string{
		"internal/runtime/bootverify/lifecycle_coordinate_hostile.go:hostileAlias:LoopID",
		"internal/runtime/bootverify/lifecycle_coordinate_hostile.go:hostileAlias:Verdict",
		"internal/runtime/bootverify/lifecycle_coordinate_hostile.go:hostileReceiver:DecisionID",
		"internal/runtime/semanticview/lifecycle_endpoints.go:hostileSameFile:StageID",
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("guard findings = %q, want %q", got, want)
	}
}

func lifecycleCoordinateFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := agentNameGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/semanticview", "./internal/runtime/bootverify", "./internal/runtime/routingtopology", "./internal/runtime/authority")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) != 0 {
		t.Fatal("C1 ownership guard could not type-check its packages")
	}
	var findings []string
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatal(err)
			}
			rel = filepath.ToSlash(rel)
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				if rel == "internal/runtime/semanticview/lifecycle_endpoints.go" && agentNameGuardFunctionName(fn) == "(*endpointCensusBuilder).addLifecycleEndpoints" {
					continue
				}
				add := func(field string) {
					switch field {
					case "StageID", "DecisionID", "Verdict", "LoopID":
						findings = append(findings, strings.Join([]string{rel, fn.Name.Name, field}, ":"))
					}
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.AssignStmt:
						for _, lhs := range node.Lhs {
							selector, ok := lhs.(*ast.SelectorExpr)
							if ok && isLifecycleCoordinateEndpoint(pkg.TypesInfo.TypeOf(selector.X)) {
								add(selector.Sel.Name)
							}
						}
					case *ast.CompositeLit:
						if isLifecycleCoordinateEndpoint(pkg.TypesInfo.TypeOf(node)) {
							for _, element := range node.Elts {
								if pair, ok := element.(*ast.KeyValueExpr); ok {
									if key, ok := pair.Key.(*ast.Ident); ok {
										add(key.Name)
									}
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	sort.Strings(findings)
	return findings
}

func isLifecycleCoordinateEndpoint(typ types.Type) bool {
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	named, ok := typ.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/semanticview" && named.Obj().Name() == "AuthoredEventEndpoint"
}
