package main

import (
	"encoding/json"
	"go/ast"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func runtimeLogProtectedAssertions(t *testing.T, source string) []string {
	t.Helper()
	var assertions []string
	ast.Inspect(projectionShapeFunction(t, source).Body, func(node ast.Node) bool {
		check, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		text := formattedNativeReadNode(check)
		if strings.Contains(text, "logger.Log(") || strings.Contains(text, "row.") || strings.Contains(text, "got := strings.TrimSpace(") || strings.Contains(text, "gotHash != sourceFact.BundleHash()") {
			assertions = append(assertions, text)
		}
		return true
	})
	return assertions
}

func runtimeRecoveryNativeSource(source, name string) string {
	if !strings.HasPrefix(name, "TestRuntimeStart_") {
		return source
	}
	source = strings.ReplaceAll(source, "startupRecoveryWorkflowPersistence(db, ", "startupRecoveryWorkflowPersistence(")
	source = strings.Replace(source, "func "+name+"(t *testing.T)", "func Verify"+strings.TrimPrefix(name, "Test")+"ForTest(t *testing.T, open RuntimeLogNativeOpenerForTest)", 1)
	source = strings.Replace(source, "\tctx := testAuthorActivityContext(context.Background())\n\t_, db, cleanup := testutil.StartPostgres(t)\n\tdefer cleanup()\n\tmodule := loadRuntimeOwnershipWorkflowModule(t)", "\tmodule := loadRuntimeOwnershipWorkflowModule(t)\n\tfixture := nativeRuntimeRecoveryLogFixtureForTest(t, open, module.SemanticSource())\n\tctx := fixture.Context", 1)
	source = strings.ReplaceAll(source, "runtimeLogPersistenceStub{db: db}", "fixture.Persistence")
	source = strings.ReplaceAll(source, "latestStartupRecoveryDecisionLog(t, db)", "latestStartupRecoveryDecisionLog(t, fixture, ctx)")
	source = strings.ReplaceAll(source, "startupRecoveryFanOutSessionForTest(t, db)", "startupRecoveryFanOutSessionForTest(t, fixture.FanOutCapacity)")
	source = strings.Replace(source, "Options: RuntimeOptions{", "Options: RuntimeOptions{\nSourceArtifactFact: nativeRuntimeLogSourceFactForTest(t, fixture),", 1)
	if name == "TestRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatal" {
		source = strings.Replace(source, "\t\t}})", "\t\t}}, startupRecoveryFanOutSessionForTest(t, fixture.FanOutCapacity))", 1)
	}
	return source
}

func TestNativeRuntimeLogRecipesPreserveLoggingAndRecoveryContracts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	logs, recovery := 0, 0
	for _, row := range rows {
		switch row.Family {
		case "native-runtime-log-persistence":
			logs++
			if strings.Contains(row.Function, "RejectsDeletedPersistedSourceArtifactFact") {
				for _, cut := range []string{"fixture.Runs.CreateRun", "ErrSourceArtifactUnavailable", "counts != before", "counts.Events != 0", "assertRunRowExists(t, fixture, ctx, runID, false)"} {
					if !strings.Contains(row.After, cut) {
						t.Fatalf("approved source-creation refusal lost %s", cut)
					}
				}
			} else if want, got := runtimeLogProtectedAssertions(t, row.Before), runtimeLogProtectedAssertions(t, row.After); len(want) == 0 || !slices.Equal(want, got) {
				t.Fatalf("native logger changed payload, run/source, spoof or lineage assertions: %s", row.Function)
			}
		case "native-runtime-recovery-log":
			if !strings.HasPrefix(row.Function, "TestRuntimeStart_") {
				continue
			}
			recovery++
			want, err := canonicalFunction(runtimeRecoveryNativeSource(row.Before, row.Function))
			got, afterErr := canonicalFunction(row.After)
			if err != nil || afterErr != nil || want != got {
				t.Fatalf("recovery workload, gate, detail or shutdown assertion changed: %s", row.Function)
			}
		default:
			continue
		}
		actual := selectedCausalObservationBody(t, row.File, projectionShapeFunction(t, row.After).Name.Name)
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		want, err := canonicalFunction(current)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got {
			t.Fatalf("native logger consumer diverged from finite recipe: %s", row.Function)
		}
	}
	if logs != 7 || recovery != 7 {
		t.Fatalf("native log/recovery consumers=%d/%d want7/7", logs, recovery)
	}
}

func TestNativeRuntimeLogOracleRejectsWeakenedPayloadLineageAndRecovery(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family != "native-runtime-log-persistence" || strings.Contains(row.Function, "RejectsDeleted") {
			continue
		}
		mutant := strings.Replace(row.After, "logger.Log(ctx,", "logger.Log(context.Background(),", 1)
		if mutant == row.After || slices.Equal(runtimeLogProtectedAssertions(t, row.Before), runtimeLogProtectedAssertions(t, mutant)) {
			t.Fatalf("lost exact native logging scope accepted: %s", row.Function)
		}
	}
	for _, row := range rows {
		if row.Family != "native-runtime-recovery-log" || !strings.Contains(row.After, "decision_outcome") {
			continue
		}
		want, err := canonicalFunction(runtimeRecoveryNativeSource(row.Before, row.Function))
		mutant, mutantErr := canonicalFunction(strings.Replace(row.After, "decision_outcome", "ignored_outcome", 1))
		if err != nil || mutantErr != nil || want == mutant {
			t.Fatal("lost recovery decision assertion accepted")
		}
	}
}

func TestNativeRuntimeLogMixedStubAndTransactionStoryAreRetired(t *testing.T) {
	for _, path := range []string{"internal/runtime/diagnostics_test.go", "internal/runtime/runtime_recovery_diagnostics_test.go", "internal/runtime/runtime_recovery_fan_out_fixture_test.go", "internal/runtime/runtime_log_native_fixture_test.go", "internal/runtime/runtime_log_native_external_test.go"} {
		source, err := os.ReadFile(filepath.Join("..", "..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		for _, retired := range []string{"runtimeLogPersistenceStub", "runRuntimeLogStoryForTest", "ensureRuntimeLogRunRowInStoryForTest", "ensureRuntimeLogRunRowForTest", "syncRuntimeLogRunCountsForTest", "*sql.DB", "*sql.Tx", "eventfixture.RunMutation", "testutil.StartPostgres", "db.Stats()"} {
			if strings.Contains(string(source), retired) {
				t.Fatalf("native log family regained raw/fake protocol: %s/%s", path, retired)
			}
		}
	}
}
