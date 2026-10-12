package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func family145BudgetConstructionProofError(function, before, after string) error {
	if strings.Contains(after, "AdmitPostgresRuntimeStore(") || strings.Count(after, "StartPostgresRuntimeStoreWithReopen(t, dsn)") != 1 {
		return fmt.Errorf("budget recovery retained a supplied-pool constructor or foreign native location")
	}
	indent := "\t\t\t\t"
	if function == "TestCompletionBudgetRecoveryProjectionParity" {
		indent = "\t\t"
	}
	native := indent + "native, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)\n"
	normalized := strings.Replace(after, native, "", 1)
	switch function {
	case "TestCompletionBudgetRecoveryProjectionParity":
		normalized = strings.Replace(normalized, "dsn, db, cleanup := testutil.StartPostgres(t)", "_, db, cleanup := testutil.StartPostgres(t)", 1)
		normalized = strings.Replace(normalized, "return native, db, true", "return storetest.AdmitPostgresRuntimeStore(t, db), db, true", 1)
	case "TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores":
		normalized = strings.Replace(normalized, "dsn, opened, cleanup := testutil.StartPostgres(t)", "_, opened, cleanup := testutil.StartPostgres(t)", 1)
		normalized = strings.Replace(normalized, "selected, db = native, opened", "selected, db = storetest.AdmitPostgresRuntimeStore(t, opened), opened", 1)
	default:
		return fmt.Errorf("unknown budget construction occurrence: %s", function)
	}
	if normalized != before {
		return fmt.Errorf("budget construction changed source/stage/spend/scope/terminal/refusal/cleanup assertions: %s", function)
	}
	return nil
}

func TestFamily145BudgetRecoveryNativeConstructionPreservesAllRetentionAndProjectionCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Function != "TestCompletionBudgetRecoveryProjectionParity" && row.Function != "TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores" {
			continue
		}
		seen[row.Function] = true
		if err := family145BudgetConstructionProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"StartPostgresRuntimeStoreWithReopen(t, dsn)", "t.Cleanup(cleanup)", "t.Fatalf", "storetest.RequireRun"} {
			if !strings.Contains(row.After, cut) || family145BudgetConstructionProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignBudgetScope()", 1)) == nil {
				t.Fatalf("budget native identity/retention/assertion/cleanup cut survived mutation: %s/%s", row.Function, cut)
			}
		}
		actual := family145ConversationForkOwnerSource(t, "../"+filepath.ToSlash(strings.TrimPrefix(row.File, "internal/")), row.Function)
		if actual != row.After {
			t.Fatalf("budget constructor escaped its finite source: %s", row.Function)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("budget construction cohort=%d/2", len(seen))
	}
}
