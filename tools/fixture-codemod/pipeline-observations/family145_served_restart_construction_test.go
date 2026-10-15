package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var family145ServedRestartConstructors = map[string]string{
	"lifecycleRestartHarness":                      "",
	"TestReceiverCompositionRestartBothStores":     "receiver.sqlite",
	"TestServedMailboxCursorRetainedRestartParity": "mailbox.sqlite",
	"TestAudit2277StoppedRunReadinessRestart":      "audit2277.sqlite",
}

func family145ServedRestartConstructionInverse(before, after string) (string, error) {
	begin := strings.Index(before, "buildStoresForServe = func(")
	if begin < 0 {
		return "", fmt.Errorf("missing reviewed selected construction")
	}
	indent := before[strings.LastIndex(before[:begin], "\n")+1 : begin]
	end := strings.Index(before[begin:], "\n"+indent+"t.Cleanup(func() {")
	native := "buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {\n" + indent + "\treturn storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})\n" + indent + "}"
	if end < 0 || strings.Count(after, native) != 1 {
		return "", fmt.Errorf("native serving construction lost context/selection/DSN/TTL")
	}
	return strings.Replace(after, native, before[begin:begin+end], 1), nil
}

func family145ServedRestartConstructionProofError(function, before, after string) error {
	after = family145WriterStageCallerInverse(function, after)
	if function == "lifecycleRestartHarness" {
		var err error
		after, err = family145ServedSnapshotCaptureInverse(after, 1)
		if err != nil {
			return err
		}
	}
	sqliteFile, known := family145ServedRestartConstructors[function]
	if !known {
		return fmt.Errorf("unreviewed serving constructor: %s", function)
	}
	for _, old := range []string{"store.NewPostgresStore(", "BootstrapPostgresRuntimeStore(", "DatabaseForTest(", "openSelectedPostgresOwner("} {
		if strings.Contains(after, old) {
			return fmt.Errorf("serving callback retains independent construction: %s", old)
		}
	}
	normalized, err := family145ServedRestartConstructionInverse(before, after)
	if err != nil {
		return err
	}
	if sqliteFile != "" {
		sqliteCapture := "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\t\t\tdb, _, _ = selectedRuntimeStoreForTest(t, p)\n\t\t\t\t})"
		if function == "TestServedMailboxCursorRetainedRestartParity" {
			if strings.Contains(after, "selectedRuntimeStoreForTest(") || strings.Contains(after, "*sql.DB") {
				return fmt.Errorf("public cursor journey retains unused raw observation")
			}
			normalized = strings.Replace(normalized, "\n\t\t\tvar deliveryReader", "\n\t\t\tvar db *sql.DB\n\t\t\tvar deliveryReader", 1)
		} else {
			common := "\n\t\t\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\t\tdb, _, _ = selectedRuntimeStoreForTest(t, p)\n\t\t\t})"
			if strings.Count(after, common) != 1 || strings.Index(after, common) > strings.Index(after, "if backend ==") {
				return fmt.Errorf("serving restart does not capture actual owner on both backends")
			}
			normalized = strings.Replace(normalized, common, "", 1)
		}
		config := "opts.ConfigPath = writeStoreBackendRuntimeConfig(t, \"sqlite\", filepath.Join(t.TempDir(), \"" + sqliteFile + "\"))"
		normalized = strings.Replace(normalized, config, config+"\n\t\t\t\t"+sqliteCapture, 1)
	}
	if normalized != before {
		return fmt.Errorf("serving rewrite changed source/options/assertions/faults/receipts/identity/restart or joined cleanup: %s", function)
	}
	return nil
}

func family145ServedRestartAssertions(t *testing.T, function, before, after string) {
	t.Helper()
	assertions, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		t.Fatal(err)
	}
	current, err := family145DescriptorBusinessAssertions(after)
	if err != nil || strings.Join(assertions, "\n") != strings.Join(current, "\n") {
		t.Fatalf("serving behavioral assertions changed: %s/%v", function, err)
	}
	for _, assertion := range assertions {
		_, call, ok := strings.Cut(assertion, " => ")
		if !ok || !strings.Contains(after, call) || family145ServedRestartConstructionProofError(function, before, strings.Replace(after, call, "weakenedServingAssertion()", 1)) == nil {
			t.Fatalf("serving assertion mutation passed: %s/%s", function, assertion)
		}
	}
}

func TestFamily145WholeServedRestartConstructionRetainsAllTemporalCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if _, known := family145ServedRestartConstructors[row.Function]; !known {
			continue
		}
		if seen[row.Function] {
			t.Fatalf("duplicate constructor: %s", row.Function)
		}
		seen[row.Function] = true
		if err := family145ServedRestartConstructionProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"storeselected.OpenRuntime(ctx,", "Selection: selection", "PostgresDSN: dsn", "SessionLockTTL: runtimeSessionLockTTL(cfg)", "deliveryReader = p.deps.DeliveryStore.SummarizeRun", "t.Cleanup(cleanup)", "buildStoresForServe = original", "waitForReadyLine()"} {
			if !strings.Contains(row.After, cut) || family145ServedRestartConstructionProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignServingOwner()", 1)) == nil {
				t.Fatalf("serving source/lifetime/capture mutation passed: %s/%s", row.Function, cut)
			}
		}
		family145ServedRestartAssertions(t, row.Function, row.Before, row.After)
	}
	if len(seen) != len(family145ServedRestartConstructors) {
		t.Fatalf("whole serving constructor inventory=%d/%d", len(seen), len(family145ServedRestartConstructors))
	}
}
