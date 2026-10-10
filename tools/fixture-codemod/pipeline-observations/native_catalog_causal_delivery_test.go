package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func causalDeliveryConsumerShape(function string) string {
	switch function {
	case "catalogDeliveryAttemptCounts":
		return `func catalogDeliveryAttemptCounts(t *testing.T, h *runtimeHarness, runID string) (deliveries, attempts int64) {
t.Helper()
reader, err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
deliveries, attempts, err = storetest.ReadNonLogRunDeliveryClaimTotals(context.Background(), reader, runID)
if err != nil { t.Fatal(err) }
return deliveries, attempts
}`
	case "scatterGatherDeliveryProgress":
		return `func scatterGatherDeliveryProgress(ctx context.Context, h *runtimeHarness, eventID string) ([]scatterGatherDeliveryCount, error) {
reader, err := h.catalogOperatorEventLister()
if err != nil { return nil, err }
rows, err := storetest.ReadCausalDeliveryStatusCounts(ctx, reader, catalogRuntimeRunID, eventID)
if err != nil { return nil, err }
var result []scatterGatherDeliveryCount
for _, row := range rows { result = append(result, scatterGatherDeliveryCount{class: row.Class, subscriber: row.Subscriber, status: row.Status, reason: row.Reason, count: row.Count}) }
return result, nil
}`
	case "logScatterGatherAttempts":
		return `func logScatterGatherAttempts(t testing.TB, h *runtimeHarness, deliveryID string) {
t.Helper()
ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
defer cancel()
reader, err := h.catalogOperatorEventLister()
if err != nil { t.Logf("failed attempt readback for %s: %v", deliveryID, err); return }
rows, err := storetest.ReadDeliveryAttemptDiagnosticRows(ctx, reader, deliveryID)
if err != nil { t.Logf("failed attempt readback for %s: %v", deliveryID, err); return }
for _, row := range rows { t.Logf("delivery %s attempt %d: closure=%s outcome=%s reason=%s failure=%s", deliveryID, row.Version, row.Closure, row.Outcome, row.Reason, row.Failure) }
}`
	default:
		return ""
	}
}

func causalDeliveryConsumerPreserved(row recipe) bool {
	want, err := canonicalFunction(causalDeliveryConsumerShape(row.Function))
	got, parseErr := canonicalFunction(row.After)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogCausalDeliveryRecipesPreserveDiagnosticsAndRefusals(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-causal-delivery-observation" {
			continue
		}
		count++
		if !causalDeliveryConsumerPreserved(row) {
			t.Fatalf("%s changed exact context, native read, diagnostic data or refusals", row.Function)
		}
		for _, pair := range [][2]string{
			{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
			{"if err != nil", "if false"}, {"context.Background(), reader, runID", "context.Background(), reader, foreignRunID"},
			{"reader, catalogRuntimeRunID, eventID", "reader, otherRunID, eventID"},
			{"reason: row.Reason", "reason: row.Status"}, {"count: row.Count", "count: 1"},
			{"WithoutCancel(h.ctx)", "WithoutCancel(context.Background())"}, {"time.Second", "time.Minute"},
			{"row.Failure)", "row.Reason)"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After != row.After && causalDeliveryConsumerPreserved(mutant) {
				t.Fatalf("weakened exact observation accepted: %v", pair)
			}
		}
	}
	if count != 3 {
		t.Fatalf("causal delivery recipes=%d, want all three consumers", count)
	}
}

func causalDeliveryQueryLiteral(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package witness\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		trimmed := strings.TrimSpace(text)
		if strings.HasPrefix(trimmed, "SELECT ") || strings.HasPrefix(trimmed, "WITH RECURSIVE ") {
			queries = append(queries, strings.Join(strings.Fields(text), ""))
		}
		return true
	})
	if len(queries) != 1 {
		t.Fatalf("fixed query count=%d, want one", len(queries))
	}
	return queries[0]
}

func TestNativeCatalogCausalDeliveryOwnerKeepsAllOriginalPhysicalPredicates(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "delivery", "read_projections.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	owners := map[string]string{"catalogDeliveryAttemptCounts": "ReadNonLogRunDeliveryClaimTotals", "scatterGatherDeliveryProgress": "ReadCausalDeliveryStatusCounts", "logScatterGatherAttempts": "ReadDeliveryAttemptDiagnosticRows"}
	for _, row := range rows {
		if row.Family != "native-catalog-causal-delivery-observation" {
			continue
		}
		fn, err := uniqueFunction(file, owners[row.Function])
		if err != nil {
			t.Fatal(err)
		}
		source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		if causalDeliveryQueryLiteral(t, source) != causalDeliveryQueryLiteral(t, row.Before) {
			t.Fatalf("%s changed causal UNION, run isolation, group/order, runtime-log exclusion, claim sum or attempt history", fn.Name.Name)
		}
	}
}
