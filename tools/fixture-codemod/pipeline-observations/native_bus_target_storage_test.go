package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func replaceBusTargetObservationCut(t *testing.T, source, from, until, replacement string) string {
	t.Helper()
	start := strings.Index(source, from)
	if start < 0 {
		t.Fatalf("missing exact target cut: %s", from)
	}
	end := strings.Index(source[start:], until)
	if end < 0 {
		t.Fatalf("missing target cut assertion: %s", until)
	}
	return source[:start] + replacement + source[start+end:]
}

func normalizedBusTargetStorage(t *testing.T, row recipe) string {
	t.Helper()
	before, err := canonicalFunction(row.Before)
	if err != nil {
		t.Fatal(err)
	}
	selected, label := "pg", "query dead_letters: %v"
	if row.Function == "TestEventBusPublishSQLiteRecordsTargetFailureDeadLetter" {
		selected, label = "sqliteStore", "query sqlite dead_letters: %v"
	} else {
		before = strings.Replace(before, "_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)", 1)
	}
	cut := fmt.Sprintf(`storage,err := storetest.ReadTargetFailureDeadLetterStorage(ctx,%s,eventID)
if err != nil { t.Fatalf(%q,err) }
reason,targetContext := storage.Reason,storage.TargetContext`, selected, label)
	before = replaceBusTargetObservationCut(t, before, "\tvar reason, targetContext string", "\tif reason !=", busFailureReceiptFragment(t, cut)+"\n")
	if selected == "sqliteStore" {
		cut := `pipelineReceipts,err := storetest.CountPipelineEventReceiptStorage(ctx,sqliteStore,eventID)
if err != nil { t.Fatalf("query sqlite pipeline receipt: %v",err) }`
		before = replaceBusTargetObservationCut(t, before, "\tvar pipelineReceipts int", "\tif pipelineReceipts !=", busFailureReceiptFragment(t, cut)+"\n")
	}
	value, err := canonicalFunction(before)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestNativeBusTargetStoragePreservesActualRoutingAndPhysicalQueries(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original []string
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-failure-storage" {
			continue
		}
		matched++
		after, err := canonicalFunction(row.After)
		if err != nil || normalizedBusTargetStorage(t, row) != after {
			t.Fatal("target preflight, sibling subscription, actual publish or failure assertions changed")
		}
		for _, query := range eventDeliveryDiagnosticSQL(t, row.Before) {
			if strings.Contains(query, "FROMdead_letters") {
				original = append(original, query)
			}
		}
		for _, pair := range [][2]string{
			{", eventID)", ", otherEvent)"},
			{"storage.Reason", "\"target_unreachable_terminated\""},
			{"storage.TargetContext", "\"missing-flow\""},
		} {
			mutant := strings.Replace(after, pair[0], pair[1], 1)
			if mutant == after || mutant == normalizedBusTargetStorage(t, row) {
				t.Fatalf("weakened target observation admitted: %v", pair)
			}
		}
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/dead_letter_owner.go", "ReadTargetFailureDeadLetterStorage")
	if matched != 2 || len(original) != 2 || !slices.Equal(original, eventDeliveryDiagnosticSQL(t, owner)) {
		t.Fatal("target dialect queries changed projection, default, event/class/handler predicates or multiplicity")
	}
}
