package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeRecoveryUnitFixtureDoesNotPretendToConsumeRawPersistence(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched, transferred := 0, 0
	for _, row := range rows {
		if row.Family == "native-runtime-recovery-log" && strings.HasPrefix(row.Function, "TestRuntimeStart_") {
			transferred++
			continue // The native log oracle now covers the complete recovery consumer.
		}
		if row.Family != "native-recovery-unit-authority" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "func startupRecoveryWorkflowPersistence(db *sql.DB, timers", "func startupRecoveryWorkflowPersistence(timers", 1)
		before = strings.ReplaceAll(before, "startupRecoveryWorkflowPersistence(db, ", "startupRecoveryWorkflowPersistence(")
		before = strings.Replace(before, "startupRecoveryWorkflowPersistence(nil, deps.TimerObligationReader)", "startupRecoveryWorkflowPersistence(deps.TimerObligationReader)", 1)
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || afterErr != nil || sourceErr != nil || got != want || actual != want {
			t.Fatalf("%s changed beyond unused unit SQL argument removal", row.Function)
		}
		for _, cut := range []string{"timers: timers", "deps.TimerObligationReader", "runtimeLogPersistenceStub{db: db}", "startupRecoveryWorkflowPersistence(scheduleStore)", "t.Fatal(", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedRecoveryCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("unit dependency/log/decision assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 2 || transferred != 7 {
		t.Fatalf("unit/native recovery authority recipes=%d/%d,want2/7", matched, transferred)
	}
}
