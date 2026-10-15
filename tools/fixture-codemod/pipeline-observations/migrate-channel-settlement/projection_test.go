package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

func privateProjectionFunction(t *testing.T, path, name string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("../../../..", path), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatal("missing closed projection:", name)
	return nil
}

func TestChannelSettlementDiagnosticQueriesAndBoundsMatchSignedSources(t *testing.T) {
	for _, d := range diagnosticPorts() {
		pkg := "channeldelivery"
		if d.owner == "effect" {
			pkg = "effectpersistence"
		}
		fn := privateProjectionFunction(t, "internal/store/internal/backend/"+pkg+"/channel_settlement_diagnostics.go", d.operation)
		queries := 0
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "QueryContext" {
				return true
			}
			queries++
			if len(call.Args) < 2 {
				t.Fatal("missing exact diagnostic query")
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok {
				t.Fatal("diagnostic accepts SQL selection")
			}
			query, err := strconv.Unquote(literal.Value)
			if err != nil || query != d.query {
				t.Fatal("diagnostic query/predicate/order/limit changed:", d.name)
			}
			var args []string
			for _, arg := range call.Args[2:] {
				args = append(args, requestkey.Node(arg))
			}
			if strings.Join(args, ",") != d.arguments {
				t.Fatal("diagnostic argument scope changed:", d.name)
			}
			return true
		})
		if queries != 1 {
			t.Fatal("diagnostic query cardinality changed:", d.name, queries)
		}
	}
}

func TestChannelSettlementPhysicalColumnOrderAndIntentScopeRemainExact(t *testing.T) {
	for _, row := range []struct {
		owner, name string
		want        []string
	}{
		{"channeldelivery", "ReadAcknowledgedCardRenderStorageTx", []string{"&result.Input", "&result.Hash"}},
		{"channeldelivery", "ReadIntentSentResponseStorageTx", []string{"&result.DeliveryID", "&result.Reference"}},
		{"channeldelivery", "ReadCardCopyCountsStorageTx", []string{"&result.Total", "&result.Uncertain"}},
		{"channeldelivery", "ReadCardRenderStorageTx", []string{"&result.RenderID", "&result.Input", "&result.Hash", "&result.Snapshot"}},
		{"decisioncard", "ReadChannelDraftCardPhysicalStorageTx", []string{"&result[0]", "&result[1]", "&result[2]", "&result[3]", "&result[4]", "&result[5]", "&result[6]"}},
	} {
		file := "settlement_observation.go"
		if row.owner == "decisioncard" {
			file = "channel_draft_observation.go"
		}
		fn := privateProjectionFunction(t, "internal/store/internal/backend/"+row.owner+"/"+file, row.name)
		assertProjectionScan(t, fn, row.want)
	}
	fn := privateProjectionFunction(t, "internal/store/internal/backend/channeldelivery/settlement_observation.go", "CountTextIntentDispositionStorageTx")
	var queries []string
	ast.Inspect(fn, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(value, "SELECT") {
				queries = append(queries, value)
			}
		}
		return true
	})
	want := []string{"SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='pending'", "SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled' AND disposition=$1"}
	if !reflect.DeepEqual(queries, want) {
		t.Fatal("pending/global/settled disposition scope changed:", queries)
	}
}

func assertProjectionScan(t *testing.T, fn *ast.FuncDecl, want []string) {
	t.Helper()
	scans := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Scan" {
			return true
		}
		scans++
		var actual []string
		for _, arg := range call.Args {
			actual = append(actual, requestkey.Node(arg))
		}
		if !reflect.DeepEqual(actual, want) {
			t.Fatal("physical column order changed:", fn.Name.Name, actual, want)
		}
		return true
	})
	if scans != 1 {
		t.Fatal("physical scan cardinality changed:", fn.Name.Name, scans)
	}
}
