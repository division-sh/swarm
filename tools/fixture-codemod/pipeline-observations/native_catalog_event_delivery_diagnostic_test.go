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
)

const catalogEventDeliveryDiagnosticShape = `func dumpEventDeliveries(t testing.TB,h *runtimeHarness,eventID string) string {
t.Helper()
reader,err := h.catalogOperatorEventLister()
if err != nil { return "observation_error:"+err.Error() }
rows,err := storetest.ReadEventDeliveryDiagnosticRows(testAuthorActivityContext(context.Background()),reader,eventID)
if err != nil { return "observation_error:"+err.Error() }
var out []string
for _,row := range rows { out=append(out,row.SubscriberType+"/"+row.SubscriberID+" status="+row.Status+" reason="+row.Reason+" route="+row.FlowInstance+"/"+row.EntityID) }
if len(out)==0 { return "<none>" }
return strings.Join(out,"; ")
}`

func catalogEventDeliveryDiagnosticPreserved(source string) bool {
	want, err := canonicalFunction(catalogEventDeliveryDiagnosticShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogEventDeliveryDiagnosticKeepsExactFieldsScopeAndRefusals(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-catalog-event-delivery-diagnostic")
	if !catalogEventDeliveryDiagnosticPreserved(row.After) {
		t.Fatal("diagnostic changed original owner, context, complete row order, fields or refusals")
	}
	for _, pair := range [][2]string{
		{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
		{"context.Background()", "h.ctx"}, {"reader, eventID", "reader, otherEventID"},
		{"if err != nil", "if false"}, {"row.Reason", "row.Status"},
		{"row.FlowInstance", "row.EntityID"}, {"row.EntityID", "row.SubscriberID"},
		{"for _, row := range rows", "for _, row := range rows[:1]"},
		{"strings.Join(out, \"; \")", "strings.Join(out, \", \")"},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || catalogEventDeliveryDiagnosticPreserved(mutant) {
			t.Fatalf("weakened diagnostic admitted: %v", pair)
		}
	}
}

func eventDeliveryDiagnosticSQL(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "query.go", "package witness\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(strings.TrimSpace(value), "SELECT ") {
			queries = append(queries, strings.Join(strings.Fields(value), ""))
		}
		return true
	})
	return queries
}

func TestNativeCatalogEventDeliveryDiagnosticKeepsBothOriginalPhysicalQueries(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-catalog-event-delivery-diagnostic")
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "delivery", "read_projections.go")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, body, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "ReadEventDeliveryDiagnosticRows")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	want, got := eventDeliveryDiagnosticSQL(t, row.Before), eventDeliveryDiagnosticSQL(t, source)
	if len(want) != 2 || !reflect.DeepEqual(want, got) {
		t.Fatalf("physical scope, NULL defaults, target extraction or total order changed: %v != %v", got, want)
	}
	for _, required := range []string{
		"rows.Scan(&row.SubscriberType, &row.SubscriberID, &row.Status, &row.Reason, &row.FlowInstance, &row.EntityID)",
		"q.QueryContext(ctx, query, eventID)", "if !postgres", "rows.Err()", "rows.Close()",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("delivery-owned row mapping or failure boundary missing: %s", required)
		}
	}
}
