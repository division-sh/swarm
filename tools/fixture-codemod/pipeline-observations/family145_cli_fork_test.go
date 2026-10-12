package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func family145CLIForkRow(file, function string) bool {
	if file == "internal/serveapp/run_fork_runtime_test.go" {
		if function == "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter" {
			return false
		}
		return strings.HasPrefix(function, "TestRunForkRuntimeOwnerHarness_") || strings.HasPrefix(function, "seedRunForkCLI") || function == "stageRunForkCLIForkPoint" || function == "registerRunForkCLIContractCatalog"
	}
	return file == "internal/serveapp/run_fork_constructor_fixture_test.go" || (file == "internal/serveapp/main_runtime_test.go" && function == "seedRunForkSelectedExecutionSourceEvent")
}

func family145CLIForkProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("CLI fork lost original business assertions: %s", function)
	}
	if err := family145DescriptorReadinessEscape([]byte("package proof\n" + after)); err != nil {
		return err
	}
	if err := family145CLIForkCallInputsError(function, before, after); err != nil {
		return err
	}
	return family145CLIForkEventCutsError(function, before, after)
}

func family145CLIForkCallInputsError(function, before, after string) error {
	for _, name := range []string{"runForkRuntimeOwnerHarness", "RequireRun", "CommitDeliveryObligationsForPersistedEvent", "seedRunForkCLIConstruction", "seedRunForkCLIActivationSourceWithoutRevision", "seedRunForkCLIActivationSource", "stageRunForkCLIForkPoint", "seedRunForkCLISelectedExecutionSource", "seedRunForkSelectedExecutionSourceEvent", "registerRunForkCLIContractCatalog"} {
		calls := sourceOutcomePreparationCalls(before, name)
		for i := range calls {
			calls[i] = strings.ReplaceAll(calls[i], ", db,", ", pg,")
			calls[i] = strings.ReplaceAll(calls[i], "storetest.NewPostgresStoreForTest(db)", "pg")
			calls[i] = strings.ReplaceAll(calls[i], "storetest.AdmitPostgresRuntimeStore(t, db)", "pg")
			if function == "seedRunForkSelectedExecutionSourceEvent" {
				calls[i] = strings.ReplaceAll(calls[i], ", pg,", ", selected,")
			}
			if function == "seedRunForkCLIActivationSourceWithoutRevision" {
				calls[i] = strings.ReplaceAll(calls[i], ", selected,", ", pg,")
			}
		}
		if !reflect.DeepEqual(calls, sourceOutcomePreparationCalls(after, name)) {
			return fmt.Errorf("CLI fork changed %s source/route/capture/CLI inputs", name)
		}
	}
	return nil
}

func family145CLIForkEventCutsError(function, before, after string) error {
	for _, pair := range []struct{ before, after string }{
		{"InsertExistingRunRootEventRecord", "InsertNativeExistingRunRootEvent"},
		{"captureRunForkCLIRevision", "CaptureSelectedRunForkRevision"},
	} {
		want := sourceOutcomePreparationCalls(before, pair.before)
		for i := range want {
			want[i] = strings.ReplaceAll(want[i], ", db, authoractivityfixture.DialectPostgres,", ", pg,")
			want[i] = strings.ReplaceAll(want[i], ", db,", ", pg,")
			want[i] = strings.ReplaceAll(want[i], pair.before, pair.after)
			if pair.before == "captureRunForkCLIRevision" {
				want[i] = "storetest." + strings.Replace(want[i], "(t,", "(t, context.Background(),", 1)
			}
		}
		got := sourceOutcomePreparationCalls(after, pair.after)
		// The source helper already owns one exact background context.
		if function == "seedRunForkSelectedExecutionSourceEvent" {
			for i := range want {
				want[i] = strings.ReplaceAll(want[i], "pg", "selected")
				want[i] = strings.ReplaceAll(want[i], "context.Background()", "ctx")
			}
		}
		if !reflect.DeepEqual(want, got) {
			return fmt.Errorf("CLI fork changed event-only or revision temporal cuts: %s/%v/%v", function, want, got)
		}
	}
	return nil
}

func TestFamily145CLIForkChainKeepsEveryOriginalBusinessAssertionAndEventCaptureInput(t *testing.T) {
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var count int
	for _, row := range rows {
		if !family145CLIForkRow(row.File, row.Function) || row.Removed {
			continue
		}
		count++
		if err := family145CLIForkProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"runForkRuntimeOwnerHarness", "InsertNativeExistingRunRootEvent", "CaptureSelectedRunForkRevision", "seedRunForkCLIConstruction"} {
			calls := sourceOutcomePreparationCalls(row.After, name)
			for _, call := range calls {
				if family145CLIForkProofError(row.Function, row.Before, strings.Replace(row.After, call, "foreignCut()", 1)) == nil {
					t.Fatalf("missing %s passed", call)
				}
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145CLIForkProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original %s assertion passed", row.Function)
			}
		}
	}
	if count != 19 {
		t.Fatalf("CLI whole-chain transition inventory: %d, want 19", count)
	}
}

func TestFamily145CLIForkHasNoRawCarrierOrIndependentCaptureAndKeepsOriginalConstruction(t *testing.T) {
	for _, file := range []string{"run_fork_runtime_test.go", "run_fork_constructor_fixture_test.go"} {
		data, err := os.ReadFile("../../../internal/serveapp/" + file)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || !family145CLIForkRow("internal/serveapp/"+file, fn.Name.Name) {
				continue
			}
			if err := family145DescriptorReadinessEscape([]byte("package proof\n" + family145NodeString(fn))); err != nil {
				t.Fatal(err)
			}
		}
		for _, escape := range []string{"captureRunForkCLIRevision", "NewPostgresStoreForTest", "testutil.StartPostgres(t)"} {
			if strings.Contains(string(data), escape) {
				t.Fatalf("CLI chain retained %s", escape)
			}
		}
	}
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var retired, construction bool
	for _, row := range rows {
		if row.Function == "captureRunForkCLIRevision" {
			retired = row.Removed
		}
		if row.Function == "seedRunForkCLIConstruction" {
			construction = true
			want := strings.Replace(row.Before, "db *sql.DB", "selected runForkCLISelectedStore", 1)
			want = strings.Replace(want, "\n\tselected := storetest.AdmitPostgresRuntimeStore(t, db)", "", 1)
			if want != row.After {
				t.Fatal("CLI construction changed genuine source, authority, route topology, clock or cleanup")
			}
		}
	}
	if !retired || !construction {
		t.Fatal("CLI chain retirement/construction inventory incomplete")
	}
}

func TestFamily145CLIForkEverySQLObservationMatchesItsClosedPhysicalOwner(t *testing.T) {
	owners := map[string][]struct{ path, function string }{
		"TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteThroughCanonicalOwnerJSON": {
			{"internal/backend/runforkpersistence/cli_storage_observation.go", "CountSelectedForkExactExecutionStorageTx"},
			{"internal/backend/eventrecord/run_name_cardinality.go", "CountRunEventOrSourceEventStorageTx"},
			{"internal/backend/eventrecord/cli_diagnostic_storage.go", "CountPostgresForkTypedRuntimeDiagnosticTx"},
			{"internal/backend/eventrecord/cli_diagnostic_storage.go", "ReadPostgresGlobalRuntimeDiagnosticStorageTx"},
			{"internal/backend/runforkpersistence/runtime_outcome_observation.go", "CountSelectedForkSourceEventLineageStorageTx"},
		},
		"TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteReportsSourceAdvancedBranchJSON": {
			{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
		},
		"TestRunForkRuntimeOwnerHarness_MaterializeOnlyUsesCanonicalStoreOwnerJSON": {
			{"internal/backend/pipelinepersistence/cli_history_fixture.go", "ReadCLIMaterializedEntityStateTx"},
			{"internal/backend/runforkpersistence/cli_storage_observation.go", "ReadSelectedForkBindingModeStorageTx"},
		},
		"TestRunForkRuntimeOwnerHarness_ActivateUsesCanonicalStoreOwnerJSON": {
			{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
			{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
		},
		"TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingRejectsDeliveryReplayWithoutPersistedRouteRecovery": {
			{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
			{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
			{"internal/backend/runforkpersistence/cli_storage_observation.go", "CountSelectedForkDeliveryReplayStorageTx"},
			{"internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunEventsForTest"},
			{"internal/backend/delivery/read_projections.go", "CountRunDeliveryStorageTx"},
		},
	}
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var count int
	for _, row := range rows {
		ports, ok := owners[row.Function]
		if !ok {
			continue
		}
		count++
		queries := family145ConnectorSQL(t, "package proof\n"+row.Before)
		if len(queries) != len(ports) {
			t.Fatalf("CLI SQL inventory changed: %s/%d/%d", row.Function, len(queries), len(ports))
		}
		for i, port := range ports {
			owned := family145ConversationForkOwnerSource(t, port.path, port.function)
			physical := family145ConnectorSQL(t, "package proof\n"+owned)
			// For validated nonempty UUIDs, NULL-coalesced text equality and UUID
			// equality select the same union, including duplicates and SQL NULL.
			want := strings.ReplaceAll(queries[i], "COALESCE(source_event_id::text,'')=$2::text", "source_event_id=$2")
			if len(physical) != 1 || want != physical[0] {
				t.Fatalf("CLI physical fields/predicates/ordering/NULL changed: %s/%s\n%s\n%v", row.Function, port.function, want, physical)
			}
		}
	}
	if count != len(owners) {
		t.Fatal("CLI physical observation inventory incomplete")
	}
}
