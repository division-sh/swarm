package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func family145NumericTimerInspectionProofError(before, after string) error {
	if strings.Contains(after, "*sql.DB") || strings.Contains(after, "sql.Open(") || strings.Contains(after, "AdmitPostgresRuntimeStore(") || strings.Contains(after, "AdmitSQLiteRuntimeStore(") || strings.Contains(after, "db.Close()") {
		return fmt.Errorf("numeric timer child still owns supplied-pool construction or independent close")
	}
	normalized := strings.Replace(after, "\tvar reader pipeline.WorkflowTimerActivationPersistence", "\tvar db *sql.DB\n\tvar reader pipeline.WorkflowTimerActivationPersistence", 1)
	for _, cut := range [][2]string{
		{"selected, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, loaded.Config.Store.SQLite.Path)\n\t\treader = selected", "db, err = sql.Open(\"sqlite\", loaded.Config.Store.SQLite.Path)\n\t\tif err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\treader = storetest.AdmitSQLiteRuntimeStore(t, db)"},
		{"selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)\n\t\treader = selected", "db, err = sql.Open(\"postgres\", dsn)\n\t\tif err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\treader = storetest.AdmitPostgresRuntimeStore(t, db)"},
	} {
		if strings.Count(normalized, cut[0]) != 1 || strings.Count(before, cut[1]) != 1 {
			return fmt.Errorf("numeric child changed exact configured location, context or native owner")
		}
		normalized = strings.Replace(normalized, cut[0], cut[1], 1)
	}
	close := "\tdefer func() {\n\t\tif err := db.Close(); err != nil {\n\t\t\tt.Error(err)\n\t\t}\n\t}()\n"
	normalized = strings.Replace(normalized, "\tactivations, err := reader.ListWorkflowTimerActivations", close+"\tactivations, err := reader.ListWorkflowTimerActivations", 1)
	if normalized != before {
		return fmt.Errorf("numeric child changed requests, read scope, all100 identities, timer clocks, deadline, output or rejection assertions")
	}
	return nil
}

func TestFamily145NumericTimerPostJoinChildKeepsExactWholeBodyAndNativeLocationOwner(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		if row.File != "internal/serveapp/numeric_timer_inspection_test.go" || row.Function != "TestOwnedNumericTimerInspection" {
			continue
		}
		seen++
		if err := family145NumericTimerInspectionProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{
			"storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, loaded.Config.Store.SQLite.Path)",
			"storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)",
			"reader.ListWorkflowTimerActivations(ctx, request.RunID, \"\", false)",
			"len(request.Entities) != 100", "activation.FireAt.Sub(activation.CreatedAt) != after",
			"request.Entities[activation.EntityID] != activation.Route.InstancePath",
			"os.WriteFile(request.Output, body, 0600)",
		} {
			if strings.Count(row.After, cut) != 1 || family145NumericTimerInspectionProofError(row.Before, strings.Replace(row.After, cut, "foreignNumericInspection()", 1)) == nil {
				t.Fatalf("lost exact location/owner/read/identity/clock/output passed: %s", cut)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("numeric inspection whole-child inventory=%d/1", seen)
	}
}
