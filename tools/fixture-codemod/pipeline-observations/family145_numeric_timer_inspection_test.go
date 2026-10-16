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
