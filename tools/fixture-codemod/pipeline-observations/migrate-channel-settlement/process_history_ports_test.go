package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestChannelProcessHistoryKeepsEverySignedQueryUnderItsOwner(t *testing.T) {
	queries, ports := preservedHistoryQueries(), preservedHistoryPorts()
	owners := map[string]string{"channel": "channeldelivery", "effect": "effectpersistence", "operator": "operatorchannel"}
	for index, p := range ports {
		t.Run(p.Name, func(t *testing.T) {
			path := filepath.Join("../../../..", "internal/store/internal/backend", owners[p.Owner], "channel_preserved_history.go")
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			var observed []string
			for _, node := range file.Decls {
				fn, ok := node.(*ast.FuncDecl)
				if !ok || fn.Name.Name != p.Operation {
					continue
				}
				ast.Inspect(fn, func(node ast.Node) bool {
					literal, ok := node.(*ast.BasicLit)
					if ok && literal.Kind == token.STRING {
						query, err := strconv.Unquote(literal.Value)
						if err != nil {
							t.Fatal(err)
						}
						observed = append(observed, query)
					}
					return true
				})
			}
			if err := requireOneSignedHistoryQuery(queries[index], observed); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requireOneSignedHistoryQuery(want string, observed []string) error {
	if len(observed) != 1 || observed[0] != want {
		return fmt.Errorf("history port must retain exactly its complete signed query")
	}
	return nil
}

func TestChannelProcessHistoryRejectsLossyOrCombinedQueries(t *testing.T) {
	queries := preservedHistoryQueries()
	for name, candidate := range map[string][]string{
		"missing":         nil,
		"different-owner": {queries[1]},
		"count-only":      {"SELECT COUNT(*) FROM operator_principals"},
		"extra-limit":     {queries[0] + " LIMIT 1"},
		"combined-cut":    {queries[0], queries[1]},
		"duplicate":       {queries[0], queries[0]},
	} {
		t.Run(name, func(t *testing.T) {
			if err := requireOneSignedHistoryQuery(queries[0], candidate); err == nil {
				t.Fatal("lossy or combined history escaped the signed inventory")
			}
		})
	}
}

func TestChannelProcessHistoryCallsEveryOwnerInOriginalTemporalOrder(t *testing.T) {
	fn := privateProjectionFunction(t, "internal/serveapp/channel_source_lifecycle_supported_surface_test.go", "readChannelPreservedHistory")
	var observed []string
	ast.Inspect(fn, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "reader" {
				observed = append(observed, selector.Sel.Name)
			}
		}
		return true
	})
	var want []string
	for _, p := range preservedHistoryPorts() {
		want = append(want, p.Name)
		method := privateProjectionFunction(t, "internal/store/storetest/channel_process_observation.go", p.Name)
		cuts := 0
		ast.Inspect(method, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "InspectSnapshot" {
				cuts++
			}
			return true
		})
		if cuts != 1 {
			t.Fatalf("%s lost its independent read transaction: %d", p.Name, cuts)
		}
	}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("history grouping or temporal order changed: %v", observed)
	}
}
