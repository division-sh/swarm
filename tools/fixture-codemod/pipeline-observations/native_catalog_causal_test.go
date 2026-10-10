package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativeCatalogCausalRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-catalog-causal-observation" {
			found = append(found, row)
		}
	}
	if len(found) != 12 {
		t.Fatalf("causal observation recipes=%d, want all 12 consumers", len(found))
	}
	return found
}

func undoNativeCatalogReaderProjection(t *testing.T, row recipe) string {
	t.Helper()
	fn := projectionShapeFunction(t, row.After)
	bind := "reader, err := h.catalogOperatorEventLister()"
	refusal := formattedNativeReadNode(projectionShapeStatement(t, "if err != nil { t.Fatal(err) }"))
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var statements []ast.Stmt
		for i := 0; i < len(block.List); i++ {
			if formattedNativeReadNode(block.List[i]) == bind {
				if i+1 >= len(block.List) || formattedNativeReadNode(block.List[i+1]) != refusal {
					t.Fatal("native reader projection dropped its exact refusal")
				}
				i++
				continue
			}
			statements = append(statements, block.List[i])
		}
		block.List = statements
		return true
	})
	source := formattedNativeReadNode(fn)
	source = strings.ReplaceAll(source, "selected catalogOperatorEventLister", "db *sql.DB")
	source = strings.ReplaceAll(source, "(t, selected,", "(t, db,")
	source = strings.ReplaceAll(source, "(t, reader,", "(t, h.db,")
	source = strings.ReplaceAll(source, "catalogFlowInstanceForCausalFlow(h.workflow,", "catalogFlowInstanceForCausalFlow(h.db, h.workflow,")
	if row.Function == "assertCatalogRuntimeOutcome" {
		const manifestCall = "assertAgentReceived(t, h, h.startedAt, expected.Expected.AgentReceived)"
		if strings.Count(source, manifestCall) != 1 {
			t.Fatal("native subscriber manifest caller changed harness, boundary or expectations")
		}
		source = strings.Replace(source, manifestCall, "assertAgentReceived(t, h.db, h.startedAt, expected.Expected.AgentReceived)", 1)
	}
	if row.Function == "TestCatalogCausalEntityIDs_FollowsSourceEventIDChain" {
		source = strings.Replace(source, "catalogCausalEntityIDs(t, pg,", "catalogCausalEntityIDs(t, db,", 1)
	}
	if row.Function == "assertCausalEvents" {
		source = strings.Replace(source, "if h == nil {", "if h == nil || h.db == nil {", 1)
		source = strings.Replace(source, "runtime harness is required for causal_events assertions", "runtime harness database is required for causal_events assertions", 1)
	}
	return source
}

func TestNativeCatalogCausalCallerRecipesPreserveEveryAssertion(t *testing.T) {
	for _, row := range nativeCatalogCausalRecipes(t) {
		if row.Function == "catalogEventsSince" || row.Function == "hasExpectedEmittedEvents" {
			continue
		}
		before, err := canonicalFunction(row.Before)
		actual, parseErr := canonicalFunction(undoNativeCatalogReaderProjection(t, row))
		if err != nil || parseErr != nil || actual != before {
			t.Fatalf("%s changed assertions, workload or source identity beyond native reader propagation: %v / %v", row.Function, err, parseErr)
		}
	}
}

const nativeCatalogEventsSinceShape = `func catalogEventsSince(t testing.TB, selected catalogOperatorEventLister, since time.Time) []catalogStoredEvent {
t.Helper()
if selected == nil { return nil }
rows, err := storetest.ReadCausalEventStorageSince(testAuthorActivityContext(context.Background()), selected, since)
if err != nil { t.Fatalf("query causal events: %v", err) }
out := []catalogStoredEvent{}
for _, row := range rows {
row.ID = strings.TrimSpace(row.ID)
row.Name = strings.TrimSpace(row.Name)
row.SourceEventID = strings.TrimSpace(row.SourceEventID)
row.PayloadEntityID = strings.TrimSpace(row.PayloadEntityID)
if row.ID != "" { out = append(out, row) }
}
return out
}`

func TestNativeCatalogCausalReaderRecipeRetainsExactProjection(t *testing.T) {
	for _, row := range nativeCatalogCausalRecipes(t) {
		if row.Function != "catalogEventsSince" {
			continue
		}
		expected, err := canonicalFunction(nativeCatalogEventsSinceShape)
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("causal read context, result normalization or empty behavior changed: %v / %v", err, parseErr)
		}
	}
}

func nativeCatalogWaitAlgorithm(t *testing.T, source string, old bool) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	var tail []ast.Stmt
	started := false
	for _, stmt := range fn.Body.List {
		started = started || strings.HasPrefix(formattedNativeReadNode(stmt), "counts := make(")
		if !started {
			continue
		}
		if old && strings.HasPrefix(formattedNativeReadNode(stmt), "if err := rows.Err();") {
			continue
		}
		if old {
			if loop, ok := stmt.(*ast.ForStmt); ok && formattedNativeReadNode(loop.Cond) == "rows.Next()" {
				if len(loop.Body.List) < 2 || formattedNativeReadNode(loop.Body.List[0]) != "var eventID, eventName, payloadEntityID string" {
					t.Fatal("unreviewed original wait row shape")
				}
				replacement := projectionShapeStatement(t, "for _, row := range rows {}").(*ast.RangeStmt)
				replacement.Body.List = append([]ast.Stmt{projectionShapeStatement(t, "eventID, eventName, payloadEntityID := row.ID, row.Name, row.PayloadEntityID")}, loop.Body.List[2:]...)
				stmt = replacement
			}
		}
		tail = append(tail, stmt)
	}
	if !started {
		t.Fatal("wait assertion algorithm is missing")
	}
	fn.Body.List = tail
	return formattedNativeReadNode(fn)
}

func TestNativeCatalogCausalWaitRecipePreservesMultiplicityScopeAndCancellation(t *testing.T) {
	for _, row := range nativeCatalogCausalRecipes(t) {
		if row.Function != "hasExpectedEmittedEvents" {
			continue
		}
		if nativeCatalogWaitAlgorithm(t, row.Before, true) != nativeCatalogWaitAlgorithm(t, row.After, false) {
			t.Fatal("emission wait lost causal scope, normalization, multiplicity or result assertions")
		}
		fn := projectionShapeFunction(t, row.After)
		want := projectionShapeFunction(t, `func (h *runtimeHarness) hasExpectedEmittedEvents(ctx context.Context, entityID string, want []string, flowPrefix string, source semanticview.Source) bool {
h.t.Helper()
reader, err := h.catalogOperatorEventLister()
if err != nil { h.t.Fatal(err) }
relevantEventIDs := catalogCausalEventIDs(h.t, reader, h.startedAt, h.publishedIDs)
relevantEntityIDs := catalogCausalEntityIDs(h.t, reader, h.startedAt, h.publishedIDs, entityID)
rows, err := storetest.ReadCausalEventStorageSince(ctx, reader, h.startedAt)
if err != nil {
if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) { return false }
h.t.Fatalf("query emitted events for wait: %v", err)
}
}`)
		if len(fn.Body.List) < len(want.Body.List) {
			t.Fatal("native wait read/refusal prelude is missing")
		}
		for i, statement := range want.Body.List {
			if formattedNativeReadNode(fn.Body.List[i]) != formattedNativeReadNode(statement) {
				t.Fatal("native wait read replaced original owner, context, boundary or cancellation refusal")
			}
		}
	}
}

func TestNativeCatalogCausalOraclesRejectLostScopeMultiplicityAndRefusals(t *testing.T) {
	for _, probe := range []struct {
		function, before, after string
	}{
		{"catalogCausalEventIDs", "out[row.SourceEventID]", "out[row.ID]"},
		{"catalogCausalEntityIDs", "row.PayloadEntityID", "row.SourceEventID"},
		{"assertEmittedEvents", "dedup := !hasDuplicateStrings(want)", "dedup := true"},
		{"assertCausalEvents", "row.SourceEventID", "row.ID"},
	} {
		for _, row := range nativeCatalogCausalRecipes(t) {
			if row.Function != probe.function {
				continue
			}
			changed := row
			changed.After = strings.Replace(row.After, probe.before, probe.after, 1)
			if changed.After == row.After {
				t.Fatalf("negative control did not change %s", probe.function)
			}
			before, err := canonicalFunction(row.Before)
			actual, parseErr := canonicalFunction(undoNativeCatalogReaderProjection(t, changed))
			if err == nil && parseErr == nil && before == actual {
				t.Fatalf("lost causal scope/reference/count accepted: %s", probe.function)
			}
		}
	}
	for _, row := range nativeCatalogCausalRecipes(t) {
		if row.Function != "hasExpectedEmittedEvents" {
			continue
		}
		before := nativeCatalogWaitAlgorithm(t, row.Before, true)
		for _, pair := range [][2]string{
			{"counts[eventName]--", "counts[eventName] = 0"},
			{"!causalEvent && !causalEntity", "!causalEvent || !causalEntity"},
			{"remaining > 0", "remaining > 1"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After || nativeCatalogWaitAlgorithm(t, changed, false) == before {
				t.Fatalf("changed wait count/scope accepted: %v", pair)
			}
		}
	}
}

func TestNativeCatalogCausalOwnerKeepsExactReadAndRowErrorContract(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "eventrecord", "causal_observation.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "ReadCausalObservationSince")
	if err != nil {
		t.Fatal(err)
	}
	want := `func ReadCausalObservationSince(ctx context.Context, tx *sql.Tx, postgres bool, since time.Time) ([]CausalObservationRow, error) {
query := ` + "`SELECT event_id::text,event_name,COALESCE(source_event_id::text,''),\nCOALESCE(NULLIF(payload->>'entity_id',''),COALESCE(entity_id::text,''))\nFROM events WHERE created_at >= $1 ORDER BY created_at ASC,event_id ASC`" + `
if !postgres {
query = ` + "`SELECT event_id,event_name,COALESCE(source_event_id,''),\nCOALESCE(NULLIF(json_extract(payload,'$.entity_id'),''),COALESCE(entity_id,''))\nFROM events WHERE created_at >= ? ORDER BY created_at ASC,event_id ASC`" + `
}
rows, err := tx.QueryContext(ctx, query, since)
if err != nil { return nil, err }
defer rows.Close()
out := []CausalObservationRow{}
for rows.Next() {
var row CausalObservationRow
if err := rows.Scan(&row.ID, &row.Name, &row.SourceEventID, &row.PayloadEntityID); err != nil { return nil, err }
out = append(out, row)
}
if err := rows.Err(); err != nil { return nil, err }
return out, nil
}`
	expected, err := canonicalFunction(want)
	actual, parseErr := canonicalFunction(formattedNativeReadNode(fn))
	if err != nil || parseErr != nil || actual != expected {
		t.Fatalf("causal SQL, time/order, exact read or row error contract changed: %v / %v", err, parseErr)
	}
	for _, pair := range [][2]string{
		{"created_at >=", "created_at >"},
		{"ORDER BY created_at ASC,event_id ASC", "ORDER BY event_id ASC"},
		{"payload->>'entity_id'", "payload->>'other_id'"},
		{"rows.Err()", "ignoredRowsError()"},
		{"tx.QueryContext(ctx, query, since)", "foreignDB.QueryContext(ctx, query, since)"},
	} {
		changed := strings.Replace(want, pair[0], pair[1], 1)
		actual, err := canonicalFunction(changed)
		if changed == want || (err == nil && actual == expected) {
			t.Fatalf("lost SQL scope/order/row errors or native transaction accepted: %v", pair)
		}
	}
}
