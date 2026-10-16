package main

import (
	"encoding/json"
	"strings"
	"testing"
)

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
