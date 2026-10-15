package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func family145GateCompletionConstructorProofError(before, after string) error {
	const reader = ", ReceiverStateReader: requireReceiverProofStateReader(t, projected)"
	if strings.Count(after, reader) != 1 {
		return fmt.Errorf("gate completion does not bind its reader to the captured selected owner")
	}
	const capture = "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\tdb, selectedPG, selectedSQLite = selectedRuntimeStoreForTest(t, p)\n\t})"
	const sqliteCapture = "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\tdb, selectedPG, selectedSQLite = selectedRuntimeStoreForTest(t, p)\n\t\t})"
	const native = "buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {\n\t\t\treturn storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})\n\t\t}"
	const sqlite = "opts.ConfigPath = writeStoreBackendRuntimeConfig(t, \"sqlite\", filepath.Join(t.TempDir(), \"gate-completion.sqlite\"))"
	const ownerAssertion = "\n\t\tif projected == nil || backend == \"sqlite\" && projected != selectedSQLite || backend != \"sqlite\" && projected != selectedPG {\n\t\t\tt.Fatalf(\"gate completion fixture lost actual current owner: projected=%p postgres=%p sqlite=%p\", projected, selectedPG, selectedSQLite)\n\t\t}"
	begin, end := strings.Index(before, "buildStoresForServe = func("), strings.Index(before, "\n\t\tt.Cleanup(func() {")
	if begin < 0 || end < begin || strings.Count(after, native) != 1 || strings.Count(after, capture) != 1 || strings.Index(after, capture) > strings.Index(after, "if backend ==") {
		return fmt.Errorf("gate completion constructor does not capture the actual two-backend production projection")
	}
	if strings.Count(after, ownerAssertion) != 1 || strings.Count(after, "\n\tvar projected any") != 1 || strings.Count(after, "\n\t\tprojected = p.deps.EventStore") != 1 {
		return fmt.Errorf("gate completion did not prove the exact current selected owner at every readiness/restart")
	}
	normalized := strings.Replace(strings.Replace(after, reader, "", 1), native, before[begin:end], 1)
	normalized = strings.Replace(normalized, "\n\t"+capture, "", 1)
	normalized = strings.Replace(normalized, sqlite, sqlite+"\n\t\t"+sqliteCapture, 1)
	normalized = strings.Replace(normalized, "\n\tvar projected any", "", 1)
	normalized = strings.Replace(normalized, "\n\t\tprojected = p.deps.EventStore", "", 1)
	normalized = strings.Replace(normalized, ownerAssertion, "", 1)
	if normalized != before {
		return fmt.Errorf("gate completion construction changed source/options/DSN/readiness/start/restart/consumer bindings or cleanup")
	}
	if strings.Contains(after, "store.NewPostgresStore(") || strings.Contains(after, "BootstrapPostgresRuntimeStore(") || strings.Contains(after, "DatabaseForTest(") || strings.Contains(after, "openSelectedPostgresOwner(") {
		return fmt.Errorf("gate completion retained a separate facade, raw getter or duplicate admission")
	}
	return nil
}

func TestFamily145GateCompletionConstructorUsesOneActualOwnerAndPreservesEveryConsumer(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		if row.Function != "gateCompletionHarness" {
			continue
		}
		seen++
		if err := family145GateCompletionConstructorProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{
			"storeselected.OpenRuntime(ctx,", "Selection: selection", "PostgresDSN: dsn", "SessionLockTTL: runtimeSessionLockTTL(cfg)",
			"db, selectedPG, selectedSQLite = selectedRuntimeStoreForTest(t, p)", "deliveryReader = p.deps.DeliveryStore.SummarizeRun",
			"p.waitForReadyLine()", "Postgres: selectedPG", "SQLite: selectedSQLite", "ReadRunDeliveries: deliveryReader", "t.Cleanup(cleanup)", "buildStoresForServe = original",
			"projected = p.deps.EventStore", "projected == nil", "projected != selectedSQLite", "projected != selectedPG", "t.Fatalf(\"gate completion fixture lost actual current owner:",
			"ReceiverStateReader: requireReceiverProofStateReader(t, projected)",
		} {
			if family145GateCompletionConstructorProofError(row.Before, strings.Replace(row.After, cut, "foreignCompletionOwner()", 1)) == nil {
				t.Fatalf("gate completion owner/source/consumer/context/lifetime mutation passed: %s", cut)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("whole gate completion constructor inventory=%d/1", seen)
	}
}
