package main

import (
	_ "embed"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-delivery-status-preimages.json
var family145DeliveryStatusBytes []byte

func deliveryStatusPins(t *testing.T) []deliverystatus.Pin {
	t.Helper()
	pins, err := deliverystatus.Decode(family145DeliveryStatusBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145DeliveryStatusCallerInverse(function, current string) string {
	current = family145EventNameCallerInverse(function, current)
	pins, err := deliverystatus.Decode(family145DeliveryStatusBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return deliverystatus.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

type deliveryStatusInventoryResult struct {
	calls, definitions, compositions int
	err                              error
}

func deliveryStatusOwnedInventory(root string, exactSource func(path, function, current string) bool) deliveryStatusInventoryResult {
	var inventory deliveryStatusInventoryResult
	var declarations []struct {
		path string
		fn   *ast.FuncDecl
	}
	walkErr := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		n, d, matching, err := deliveryStatusOwnedFile(filepath.ToSlash(rel), file)
		inventory.calls += n
		inventory.definitions += d
		inventory.err = errors.Join(inventory.err, err)
		for _, fn := range matching {
			declarations = append(declarations, struct {
				path string
				fn   *ast.FuncDecl
			}{filepath.ToSlash(rel), fn})
		}
		return nil
	})
	inventory.err = errors.Join(inventory.err, walkErr)
	// Enumerate every call and alias before composing each complete declaration
	// once. Nothing is retained between scans, including decoded source ASTs.
	for _, decl := range declarations {
		inventory.compositions++
		if !exactSource(decl.path, decl.fn.Name.Name, requestkey.Node(decl.fn)) {
			inventory.err = errors.Join(inventory.err, fmt.Errorf("delivery status caller escaped exact chain: %s %s", decl.path, decl.fn.Name.Name))
		}
	}
	return inventory
}

func deliveryStatusOwnedFile(path string, file *ast.File) (calls, definitions int, matching []*ast.FuncDecl, failure error) {
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if deliverystatus.Helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		hasCall := false
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && deliverystatus.Helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				hasCall = true
			}
			return true
		})
		if hasCall {
			matching = append(matching, fn)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && deliverystatus.Helpers[id.Name] && !allowed[id.Pos()] {
			failure = errors.Join(failure, fmt.Errorf("delivery status helper escaped as alias/initializer: %s %s", path, id.Name))
		}
		return true
	})
	return calls, definitions, matching, failure
}
