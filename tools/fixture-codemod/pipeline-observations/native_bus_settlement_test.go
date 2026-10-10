package main

import (
	"encoding/json"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

func normalizedNativePostCommitRoot(t *testing.T, source string) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	var initializer *ast.CompositeLit
	var body []ast.Stmt
	for _, stmt := range fn.Body.List {
		assign, ok := stmt.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			body = append(body, stmt)
			continue
		}
		if name, ok := assign.Lhs[0].(*ast.Ident); ok && name.Name == "interceptor" && assign.Tok == token.DEFINE {
			pointer, ok := assign.Rhs[0].(*ast.UnaryExpr)
			if !ok || pointer.Op != token.AND {
				t.Fatal("native observer collaborator is not the exact pointer initializer")
			}
			initializer, ok = pointer.X.(*ast.CompositeLit)
			if !ok {
				t.Fatal("native observer lost its complete interceptor initializer")
			}
			continue
		}
		if formattedNativeReadNode(assign.Lhs[0]) == "interceptor.probe" {
			if formattedNativeReadNode(assign.Rhs[0]) != "storetest.CollectTransactions(t, pg, storetest.TransactionProbeOptions{})" {
				t.Fatal("observer was not installed on the original selected owner")
			}
			continue
		}
		body = append(body, stmt)
	}
	fn.Body.List = body
	if initializer != nil {
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok || len(literal.Elts) != 1 || formattedNativeReadNode(literal.Type) != "[]runtimebus.EventInterceptor" {
				return true
			}
			if name, ok := literal.Elts[0].(*ast.Ident); ok && name.Name == "interceptor" {
				literal.Elts[0] = initializer
			}
			return true
		})
	}
	return formattedNativeReadNode(fn)
}

func normalizedNativePostCommitMethod(source string) string {
	for _, message := range []string{
		"post-commit interceptor ran with sql tx still in context",
		"post-commit error interceptor ran with sql tx still in context",
		"deferred event interceptor ran with sql tx still in context",
	} {
		before := "if tx, ok := runtimepipelinefixture.SQLTx(ctx); ok && tx != nil {\n\t\ti.t.Fatal(\"" + message + "\")\n\t}"
		source = strings.Replace(source, before, "requireNativePublicationSettledBeforeIntercept(i.t, i.probe)", 1)
	}
	return source
}

const nativeSettlementObserverShape = `func requireNativePublicationSettledBeforeIntercept(t *testing.T,probe *storetest.TransactionCollector) {
t.Helper()
if probe == nil { t.Fatal("post-commit proof requires its original selected transaction observer") }
if counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 { t.Fatalf("interceptor reached before native publication settled: %+v",counts) }
}`

func TestNativeBusSettlementPreservesVisibilityAndObserverPosition(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-postcommit-settlement" {
			continue
		}
		matched++
		if row.Function == "Intercept" {
			if normalizedNativePostCommitMethod(row.Before) != row.After {
				t.Fatal("actual deferred/error interception, visible event or return outcome changed")
			}
			continue
		}
		before := strings.Replace(row.Before, "_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)", 1)
		if normalizedNativePostCommitRoot(t, before) != normalizedNativePostCommitRoot(t, row.After) {
			t.Fatal("actual deferred publication changed beyond native constructor/probe setup")
		}
	}
	if matched != 3 {
		t.Fatalf("settlement snapshots=%d, want3 plus three prior snapshot updates", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "requireNativePublicationSettledBeforeIntercept")
	want, err := canonicalFunction(nativeSettlementObserverShape)
	got, parseErr := canonicalFunction(actual)
	if err != nil || parseErr != nil || want != got {
		t.Fatal("missing observer, unobserved native commit or unfinished original work must refuse")
	}
	for _, pair := range [][2]string{
		{"if probe == nil", "if false"},
		{"counts.Total.WriteCommits == 0", "false"},
		{"counts.Active != 0", "false"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		changed, mutantErr := canonicalFunction(mutant)
		if mutant == actual || (mutantErr == nil && changed == want) {
			t.Fatalf("weakened native settlement observer admitted: %v", pair)
		}
	}
}
