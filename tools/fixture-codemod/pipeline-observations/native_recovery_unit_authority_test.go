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
		current, currentWant := row.After, before
		if row.Function == "completeRuntimeRecoveryTestDeps" {
			old := "deps.DeliveryStore = newRuntimeShutdownDeliveryStore(t)"
			if strings.Count(currentWant, old) != 1 || row.Successor == "" {
				t.Fatal("reviewed recovery delivery-owner successor is missing or ambiguous")
			}
			currentWant = strings.Replace(currentWant, old, "deps.DeliveryStore = &managedNativeRecoveryDeliveryStore{}", 1)
			current = row.Successor
		}
		currentPin, pinErr := canonicalFunction(current)
		currentExpected, currentErr := canonicalFunction(currentWant)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || afterErr != nil || pinErr != nil || currentErr != nil || sourceErr != nil || got != want || currentPin != currentExpected || actual != currentExpected {
			t.Fatalf("%s changed beyond unused unit SQL argument removal and the exact reviewed delivery-owner successor", row.Function)
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
		if current != row.After {
			mutant := strings.Replace(current, "&managedNativeRecoveryDeliveryStore{}", "newRuntimeShutdownDeliveryStore(t)", 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutant == current || mutantErr == nil && changed == currentExpected {
				t.Fatal("retired delivery fixture admitted as the current recovery owner")
			}
		}
	}
	if matched != 2 || transferred != 7 {
		t.Fatalf("unit/native recovery authority recipes=%d/%d,want2/7", matched, transferred)
	}
}
