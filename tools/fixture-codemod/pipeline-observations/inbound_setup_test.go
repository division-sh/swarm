package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

func TestInboundRecipesChangeOnlyTheUnusedSeedPool(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "inbound-unused-pool" {
			t.Fatal("upstream-superseded pool recipes must not overwrite current construction owners")
		}
	}
	// Master already retired this cohort. Keep the pure rewrite proof, but do
	// not replay obsolete fixture snapshots over its admitted construction.
	for _, row := range []recipe{
		{Function: "seedPostgresInboundGatewayRuntime", Before: "func seedPostgresInboundGatewayRuntime(t *testing.T, ctx context.Context, db *sql.DB, pg Store, run, entity, flow, slug, provider, secret, agent string) Target { return originalWorkload(ctx, pg, run, entity, flow, slug, provider, secret, agent) }", After: "func seedPostgresInboundGatewayRuntime(t *testing.T, ctx context.Context, pg Store, run, entity, flow, slug, provider, secret, agent string) Target { return originalWorkload(ctx, pg, run, entity, flow, slug, provider, secret, agent) }"},
		{Function: "probe", Before: "func probe() { seedPostgresInboundGatewayRuntime(t, ctx, db, pg, run, entity, flow, slug, provider, secret, agent) }", After: "func probe() { seedPostgresInboundGatewayRuntime(t, ctx, pg, run, entity, flow, slug, provider, secret, agent) }"},
		{Function: "openProviderRawSettlementStore", Before: "func openProviderRawSettlementStore(backend string) (Store, *sql.DB) { if backend == \"sqlite\" { return sqliteOwner, db }; return postgresOwner, db }", After: "func openProviderRawSettlementStore(backend string) Store { if backend == \"sqlite\" { return sqliteOwner }; return postgresOwner }"},
		{Function: "TestInboundGatewayProviderRawSettlementSQLitePostgres", Before: "func TestInboundGatewayProviderRawSettlementSQLitePostgres(t *testing.T) { owner, db := openProviderRawSettlementStore(backend); seedProviderRawSettlementRuntime(t, ctx, owner, db, run, entity, flow, provider, secret, agent) }", After: "func TestInboundGatewayProviderRawSettlementSQLitePostgres(t *testing.T) { owner := openProviderRawSettlementStore(backend); seedProviderRawSettlementRuntime(t, ctx, owner, run, entity, flow, provider, secret, agent) }"},
		{Function: "seedProviderRawSettlementRuntime", Before: "func seedProviderRawSettlementRuntime(t *testing.T, ctx context.Context, selected Store, db *sql.DB, run, entity, flow, provider, secret, agent string) { seedPostgresInboundGatewayRuntime(t, ctx, db, selected, run, entity, flow, slug, provider, secret, agent) }", After: "func seedProviderRawSettlementRuntime(t *testing.T, ctx context.Context, selected Store, run, entity, flow, provider, secret, agent string) { seedPostgresInboundGatewayRuntime(t, ctx, selected, run, entity, flow, slug, provider, secret, agent) }"},
	} {
		assertInboundRecipeChangesOnlyUnusedPool(t, row)
	}
	for _, row := range []struct{ file, function string }{
		{"internal/runtime/inbound_postgres_test.go", "seedPostgresInboundGatewayRuntime"},
		{"internal/runtime/inbound_raw_settlement_store_test.go", "seedProviderRawSettlementRuntime"},
		{"internal/runtime/inbound_raw_settlement_store_test.go", "openProviderRawSettlementStore"},
	} {
		fn := projectionShapeFunction(t, selectedCausalObservationBody(t, row.file, row.function))
		for _, parameter := range fn.Type.Params.List {
			if formattedNativeReadNode(parameter.Type) == "*sql.DB" {
				t.Fatalf("retired seed pool parameter returned: %s", row.function)
			}
		}
		if fn.Type.Results != nil {
			for _, result := range fn.Type.Results.List {
				if formattedNativeReadNode(result.Type) == "*sql.DB" {
					t.Fatalf("retired provider pool result returned: %s", row.function)
				}
			}
		}
	}
}

func assertInboundRecipeChangesOnlyUnusedPool(t *testing.T, row recipe) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, row.File, "package runtime_test\n"+row.Before, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, row.Function)
	if err != nil {
		t.Fatal(err)
	}
	switch row.Function {
	case "seedPostgresInboundGatewayRuntime":
		assertAndRemoveUnusedSeedParameter(t, fn)
	case "seedProviderRawSettlementRuntime":
		removeOnlyInboundSeedCallArgument(t, fn)
		fn.Type.Params.List = append(fn.Type.Params.List[:3], fn.Type.Params.List[4:]...)
	case "openProviderRawSettlementStore":
		removeUnusedProviderPoolReturn(t, fn)
	case "TestInboundGatewayProviderRawSettlementSQLitePostgres":
		removeUnusedProviderPoolPropagation(t, fn)
	default:
		removeOnlyInboundSeedCallArgument(t, fn)
	}
	var expected bytes.Buffer
	if err := format.Node(&expected, token.NewFileSet(), fn); err != nil {
		t.Fatal(err)
	}
	actual, err := canonicalFunction(normalizeInboundReceiptWitnessCalls(row.After))
	if err != nil || actual != expected.String() {
		t.Fatalf("recipe changed other fixture/workload/assertion statements: %v", err)
	}
}

func removeUnusedProviderPoolReturn(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	if fn.Type.Results.NumFields() != 2 {
		t.Fatal("provider fixture's original result changed")
	}
	fn.Type.Results.List = fn.Type.Results.List[:1]
	fn.Type.Results.Opening, fn.Type.Results.Closing = token.NoPos, token.NoPos
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if result, ok := node.(*ast.ReturnStmt); ok {
			if len(result.Results) != 2 {
				t.Fatal("provider fixture's original returns changed")
			}
			result.Results = result.Results[:1]
			count++
		}
		return true
	})
	if count != 2 {
		t.Fatalf("provider fixture has %d returns, want two", count)
	}
}

func removeUnusedProviderPoolPropagation(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "seedProviderRawSettlementRuntime" {
				if len(call.Args) != 10 {
					t.Fatal("provider seed arity changed")
				}
				call.Args = append(call.Args[:3], call.Args[4:]...)
			}
		}
		if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Rhs) == 1 {
			if call, ok := assignment.Rhs[0].(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "openProviderRawSettlementStore" {
					if len(assignment.Lhs) != 2 {
						t.Fatal("provider fixture binding changed")
					}
					assignment.Lhs = assignment.Lhs[:1]
				}
			}
		}
		return true
	})
}

func assertAndRemoveUnusedSeedParameter(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	if fn.Type.Params.NumFields() != 11 {
		t.Fatal("seed's original pool arity changed")
	}
	parameter := fn.Type.Params.List[2]
	if len(parameter.Names) != 1 || parameter.Names[0].Name != "db" {
		t.Fatal("seed's original pool argument changed")
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && identifier.Name == "db" {
			t.Fatal("seed pool is live; its removal is not authorized")
		}
		return true
	})
	fn.Type.Params.List = append(fn.Type.Params.List[:2], fn.Type.Params.List[3:]...)
}

func removeOnlyInboundSeedCallArgument(t *testing.T, fn *ast.FuncDecl) {
	t.Helper()
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, ok := call.Fun.(*ast.Ident)
		if !ok || callee.Name != "seedPostgresInboundGatewayRuntime" {
			return true
		}
		if len(call.Args) != 11 {
			t.Fatal("seed call's original arity changed")
		}
		argument, ok := call.Args[2].(*ast.Ident)
		if !ok || argument.Name != "db" {
			t.Fatal("cannot discard an effectful or different pool expression")
		}
		call.Args = append(call.Args[:2], call.Args[3:]...)
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("exactly one seed call expected, got %d", count)
	}
}
