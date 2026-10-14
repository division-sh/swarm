package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"golang.org/x/tools/go/ast/astutil"
)

func TestReadbackFiniteReplayPreservesWorkloadsAndRefusesForeignInputs(t *testing.T) {
	pins, err := selectedreadrole.ReadReadbackPins(filepath.Clean("../../../.."))
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		t.Run(pin.Function, func(t *testing.T) {
			before, after := readRoleProofFunction(t, pin.Before), readRoleProofFunction(t, pin.After)
			normalizeReadbackAssertionRoles(before, after)
			if !reflect.DeepEqual(readRolePreservedCalls(before), readRolePreservedCalls(after)) {
				t.Fatal("readback changed a workload assertion, diagnostic or timing call")
			}
			raw, err := processReadCandidate(pin, []byte("package proof\n"+pin.Before))
			if err != nil {
				t.Fatal(err)
			}
			actual, err := selectedreadrole.Body(raw, pin.Function)
			if err != nil || actual != pin.After {
				t.Fatal("finite readback replay changed its exact output:", err)
			}
			again, err := processReadCandidate(pin, raw)
			if err != nil || string(again) != string(raw) {
				t.Fatal("finite readback replay is not idempotent:", err)
			}
			foreign := strings.TrimSuffix(pin.Before, "}") + "panic(\"foreign\"); }"
			if _, err := processReadCandidate(pin, []byte("package proof\n"+foreign)); err == nil {
				t.Fatal("foreign source passed finite readback preflight")
			}
		})
	}
}

func normalizeReadbackAssertionRoles(before, after *ast.FuncDecl) {
	fullCarrier := false
	for _, field := range before.Type.Params.List {
		fullCarrier = fullCarrier || requestkey.Node(field.Type) == "servedControlProofRuntime"
	}
	if !fullCarrier {
		return
	}
	for _, field := range after.Type.Params.List {
		if requestkey.Node(field.Type) == "servedControlProofRuntime" {
			return
		}
	}
	astutil.Apply(before.Body, func(cursor *astutil.Cursor) bool {
		field, ok := cursor.Node().(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := field.X.(*ast.Ident)
		if ok && owner.Name == "rt" && servedRoleTypes[field.Sel.Name] != "" {
			cursor.Replace(ast.NewIdent(roleParameter(field.Sel.Name)))
		}
		return true
	}, nil)
}

func TestReadbackDiscardedCapabilityCannotHideEvaluationOrForeignCarrier(t *testing.T) {
	dbPackage := types.NewPackage("database/sql", "sql")
	db := types.NewPointer(types.NewNamed(types.NewTypeName(token.NoPos, dbPackage, "DB", nil), types.NewStruct(nil, nil), nil))
	ownerPackage := types.NewPackage("github.com/division-sh/swarm/internal/serveapp", "serveapp")
	owner := types.NewNamed(types.NewTypeName(token.NoPos, ownerPackage, "servedControlProofRuntime", nil), types.NewStruct(nil, nil), nil)
	for _, test := range []struct {
		source string
		want   bool
	}{{"db", true}, {"rt.DB", true}, {"openDB()", false}, {"makeRuntime().DB", false}, {"other.DB", false}, {"items[advance()].DB", false}} {
		t.Run(test.source, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.source)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{expr: {Type: db}}}
			if field, ok := expr.(*ast.SelectorExpr); ok && requestkey.Node(field.X) != "other" {
				info.Types[field.X] = types.TypeAndValue{Type: owner}
			}
			if got := readbackDiscardableDB(info, expr); got != test.want {
				t.Fatalf("discarded evaluation=%s accepted=%t want=%t", test.source, got, test.want)
			}
		})
	}
}
