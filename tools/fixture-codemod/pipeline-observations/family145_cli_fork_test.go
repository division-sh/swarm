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
