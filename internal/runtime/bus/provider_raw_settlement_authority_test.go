package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProviderRawSettlementAuthorityRemainsInboundOnly(t *testing.T) {
	allowedAuthorityFiles := map[string]bool{
		filepath.Clean("inbound_batch.go"):    true,
		filepath.Clean("eventbus_publish.go"): true,
	}
	if err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
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
	if err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
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
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "PrepareInboundDeliveryBatch" {
				callers = append(callers, filepath.Clean(path))
			}
			return true
		})
		return nil
	}); err != nil {
		t.Fatalf("scan runtime production callers: %v", err)
	}
	if len(callers) != 1 || callers[0] != filepath.Clean("../inbound.go") {
		t.Fatalf("PrepareInboundDeliveryBatch production callers = %#v, want only internal/runtime/inbound.go", callers)
	}
}
