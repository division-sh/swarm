package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func readRoleProofFunction(t *testing.T, source string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatal("expected one exact declaration:", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		t.Fatal("expected function")
	}
	return fn
}

func TestSelectedReadRoleManifestPreservesAssertionsWaitsAndAllTwentySignatures(t *testing.T) {
	root := filepath.Clean("../../../..")
	pins, err := selectedreadrole.ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	changed, calls := map[string]bool{}, 0
	for _, pin := range pins {
		before, after := readRoleProofFunction(t, pin.Before), readRoleProofFunction(t, pin.After)
		if requestkey.Node(before.Type) != requestkey.Node(after.Type) {
			changed[pin.Function] = true
		}
		if !reflect.DeepEqual(readRolePreservedCalls(before), readRolePreservedCalls(after)) {
			t.Fatalf("business assertion, logging or temporal cut changed: %s", pin.Function)
		}
		ast.Inspect(before, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && readRoleNames[id.Name] {
					calls++
				}
			}
			return true
		})
	}
	if !reflect.DeepEqual(changed, readRoleNames) || calls != 49 {
		t.Fatalf("signature/caller inventory changed: signatures=%d calls=%d", len(changed), calls)
	}
}

func readRolePreservedCalls(fn *ast.FuncDecl) map[string]int {
	out := map[string]int{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := requestkey.Node(call.Fun)
		switch name {
		case "t.Fatalf", "t.Errorf", "t.Logf", "time.Sleep":
			out[requestkey.Node(call)]++
		case "context.WithTimeout":
			out[requestkey.Node(call)]++
		}
		return true
	})
	return out
}

func TestSelectedReadRoleRemovedSQLHasExactExistingDomainOwner(t *testing.T) {
	root := filepath.Clean("../../../..")
	pins, err := selectedreadrole.ReadPins(root)
	if err != nil {
		t.Fatal(err)
	}
	var owners strings.Builder
	for _, path := range []string{
		"internal/store/internal/backend/pipelinepersistence/served_read_roles.go",
		"internal/store/internal/backend/eventrecord/served_read_roles.go",
		"internal/store/internal/backend/delivery/read_projections.go",
		"internal/store/internal/backend/runforkrevision/postgres.go",
		"internal/store/internal/mailboxpersistence/served_read_roles.go",
	} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		owners.Write(raw)
	}
	removed := 0
	for _, pin := range pins {
		fn := readRoleProofFunction(t, pin.Before)
		before := removed
		ast.Inspect(fn, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(value, "SELECT ") && !strings.Contains(pin.After, value) {
				removed++
				if !strings.Contains(owners.String(), value) {
					t.Fatalf("removed physical query has no exact original-owner source: %s/%s", pin.Function, value)
				}
			}
			return true
		})
		if removed != before {
			t.Logf("exact removed SQL literals: %s=%d", pin.Function, removed-before)
		}
	}
	want := 0
	for _, queries := range selectedReadRoleQueries {
		want += len(queries)
	}
	if removed != want {
		t.Fatalf("removed query occurrence inventory=%d, reviewed multiset=%d", removed, want)
	}
}

func TestSelectedReadRoleScalarAndTransportRulesRejectLossOrForeignShapes(t *testing.T) {
	fn := readRoleProofFunction(t, `func proof(){if err:=rt.DB.QueryRow("SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2",run,entity).Scan(&raw);err!=nil{t.Fatal(err)}}`)
	var scan *ast.CallExpr
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Scan" {
				scan = call
			}
		}
		return true
	})
	after, ok := readRoleScalarCall(scan)
	if !ok || !strings.Contains(after, "raw=observed") || !strings.Contains(after, "ReadEntityAccumulatorTextStorage(ctx,rt.ReceiverStateReader,run,entity)") {
		t.Fatal("scalar scope or destination changed:", after)
	}
	for _, source := range []string{
		`func proof(){if err:=rows.Scan(&id);err!=nil{t.Fatal("lost error")}}`,
		`func proof(){if err:=rows.Err();err!=nil{t.Fatal(err);extraAssertion()}}`,
		`func proof(){if err:=rows.Err();err==nil{t.Fatal(err)}}`,
	} {
		gate := readRoleProofFunction(t, source).Body.List[0]
		requireReadRolePanic(t, func() {
			readRoleRowTransport(gate, map[string]readRoleList{"rows": {Port: "ReadRunEventMutationOwnersStorage", Fields: []string{""}}})
		})
	}
	for _, source := range []string{
		"//go:build foreign\n\npackage proof\nvar escape=readForkLoopActivation",
		"package proof\nvar escape=func(){readServedForkDeliveryEvidence(t,rt,run)}",
	} {
		old := changes
		changes = map[string]servedsettlement.Change{"readForkLoopActivation": {}, "readServedForkDeliveryEvidence": {}}
		err := validateNewSource("excluded.go", []byte(source))
		changes = old
		if err == nil {
			t.Fatal("new/excluded or initializer reference escaped closure")
		}
	}
}

func requireReadRolePanic(t *testing.T, run func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("altered transport assertion was discarded")
		}
	}()
	run()
}
