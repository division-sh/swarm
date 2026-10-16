package main

import (
	"fmt"
	"strings"
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
