package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeFanOutDiagnosticShape = `func logScatterGatherProgress(t testing.TB, h *runtimeHarness, eventID string) {
t.Helper()
ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
defer cancel()
reader, err := h.catalogOperatorEventLister()
if err != nil { t.Logf("failed fan-out observation: %v", err); return }
rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, reader, catalogRuntimeRunID, eventID)
if err != nil { t.Logf("failed fan-out observation: %v", err); return }
for _, row := range rows { t.Logf("fan-out at failed deadline: status=%s cursor=%d/%d owner=%q generation=%d lease=%v", row.Status, row.Cursor, row.Cardinality, row.Owner, row.Generation, row.LeaseExpiry) }
facts, err := storetest.ReadSourceRouteSettlementStorage(ctx, reader, catalogRuntimeRunID, eventID)
if err != nil { t.Logf("failed fan-out event size read: %v", err) } else { t.Logf("fan-out persisted settlement inputs: events=%d distinct=%d bytes=%d", facts.Events, facts.DistinctLedgers, facts.Bytes) }
logScatterGatherDeliveryProgress(t, ctx, h, eventID)
}`

func fanoutDiagnosticConsumerPreserved(source string) bool {
	want, err := canonicalFunction(nativeFanOutDiagnosticShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogFanOutDiagnosticKeepsFullContextFieldsAndLateCut(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-fanout-diagnostic-observation" {
			continue
		}
		count++
		if !fanoutDiagnosticConsumerPreserved(row.After) {
			t.Fatal("fan-out diagnostic changed native owner, context/deadline, late read or fields")
		}
		for _, pair := range [][2]string{
			{"if err != nil", "if false"}, {"reader, catalogRuntimeRunID, eventID", "reader, otherRunID, eventID"},
			{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
			{"time.Second", "time.Minute"}, {"row.Cursor, row.Cardinality", "row.Cardinality, row.Cursor"},
			{"row.LeaseExpiry", "nil"}, {"facts.Bytes", "facts.Events"}, {"facts.DistinctLedgers", "facts.Events"},
			{"logScatterGatherDeliveryProgress(t, ctx, h, eventID)", "logScatterGatherDeliveryProgress(t, context.Background(), h, eventID)"},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After || fanoutDiagnosticConsumerPreserved(mutant) {
				t.Fatalf("lost fan-out diagnostic contract accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("fan-out diagnostic recipes=%d, want one complete consumer", count)
	}
}

func TestNativeCatalogFanOutDiagnosticOwnersPreserveOriginalSQLAndRendering(t *testing.T) {
	for _, owner := range []struct{ path, function, sql string }{
		{"pipelinepersistence/owner_operations.go", "ReadSourceFanOutIntentDiagnosticRows", `SELECT status,cardinality,cursor,COALESCE(claim_owner,''),claim_generation,CAST(lease_expires_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2`},
		{"eventrecord/causal_observation.go", "ReadSourceRouteSettlementStorage", `SELECT COUNT(*),COUNT(DISTINCT CAST(route_settlement AS TEXT)),COALESCE(SUM(LENGTH(CAST(route_settlement AS TEXT))),0) FROM events WHERE run_id=$1 AND source_event_id=$2`},
	} {
		path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", owner.path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueFunction(file, owner.function)
		if err != nil {
			t.Fatal(err)
		}
		source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		if causalDeliveryQueryLiteral(t, source) != strings.Join(strings.Fields(owner.sql), "") {
			t.Fatalf("%s changed exact physical SQL", fn.Name.Name)
		}
		if owner.function == "ReadSourceFanOutIntentDiagnosticRows" && (!strings.Contains(source, "append([]byte(nil), value...)") || !strings.Contains(source, "if err := rows.Close(); err != nil")) {
			t.Fatal("native lease bytes or final row-close refusal lost")
		}
	}
}
