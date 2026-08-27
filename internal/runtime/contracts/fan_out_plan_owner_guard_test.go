package contracts

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestFanOutRuntimePackagesCannotDependOnRawFanOutSpec(t *testing.T) {
	uses := loadRawFanOutSpecUses(t, nil)
	if len(uses) != 0 {
		t.Fatalf("raw FanOutSpec escaped the contracts compiler into Gate A runtime packages: %s", strings.Join(uses, ", "))
	}
}

func TestFanOutRuntimeOwnerGuardRejectsHostileRawSpecUse(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal", "runtime", "engine", "fan_out_plan_owner_hostile.go")
	overlay := map[string][]byte{path: []byte(`package engine

import runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

func hostileRawFanOutInterpreter(arbitraryReceiver *runtimecontracts.FanOutSpec) string {
    return arbitraryReceiver.ItemsFrom
}
`)}
	uses := loadRawFanOutSpecUses(t, overlay)
	found := false
	for _, use := range uses {
		if strings.Contains(use, "fan_out_plan_owner_hostile.go") {
			found = true
		}
	}
	if !found {
		t.Fatalf("typed owner guard missed hostile raw FanOutSpec interpreter: %v", uses)
	}
}

func loadRawFanOutSpecUses(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := handlerRuleIdentityGuardRepoRoot(t)
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests: false, Overlay: overlay,
	}
	patterns := []string{
		"./internal/runtime/engine", "./internal/runtime/pipeline", "./internal/runtime/bootverify",
		"./internal/runtime/runforkexecution", "./internal/runtime/authoringview",
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatalf("load fan-out runtime owner packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load fan-out runtime owner packages reported type errors")
	}
	var uses []string
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			relative, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				ident, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				object := pkg.TypesInfo.Uses[ident]
				if !isContractsFanOutSpec(object) {
					return true
				}
				position := pkg.Fset.Position(ident.Pos())
				uses = append(uses, filepath.ToSlash(relative)+":"+position.String())
				return true
			})
		}
	}
	return uses
}

func isContractsFanOutSpec(object types.Object) bool {
	if object == nil || object.Name() != "FanOutSpec" || object.Pkg() == nil {
		return false
	}
	return object.Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/contracts"
}
