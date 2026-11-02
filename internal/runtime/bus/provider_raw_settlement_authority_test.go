package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestProviderRawSettlementAuthorityRemainsInboundOnly(t *testing.T) {
	allowedAuthorityFiles := map[string]bool{
		filepath.Clean("inbound_batch.go"):    true,
		filepath.Clean("eventbus_publish.go"): true,
	}
	if err := checkoutsource.WalkDir(filepath.Clean(filepath.Join("..", "..", "..")), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), "providerRawSettlement") && !allowedAuthorityFiles[filepath.Clean(path)] {
			t.Errorf("%s references private provider raw settlement authority outside its inbound/settlement owners", path)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan EventBus production sources: %v", err)
	}

	var callers []string
	if err := checkoutsource.WalkDir(filepath.Clean(filepath.Join("..", "..", "..")), "..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "PrepareInboundDeliveryBatch" {
					callers = append(callers, filepath.Clean(path)+"::"+function.Name.Name)
				}
				return true
			})
		}
		return nil
	}); err != nil {
		t.Fatalf("scan runtime production callers: %v", err)
	}
	sort.Strings(callers)
	want := []string{"../inbound.go::prepareInboundDeliveryBatch", "../private_test_inbound.go::PublishPrivateTestProviderOutput"}
	if len(callers) != len(want) || strings.Join(callers, "\n") != strings.Join(want, "\n") {
		t.Fatalf("PrepareInboundDeliveryBatch production callers = %#v, want exact inbound and private-test issuers %#v", callers, want)
	}
}
