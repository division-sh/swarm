package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFamily145SelectedCleanupRetainsPhysicalPredicatesIndependentCutsAndNoPartialEvidence(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original []string
	for _, row := range rows {
		if row.Function == "assertSelectedContractExecutionCleanup" {
			original = family145ConnectorSQL(t, "package proof\n"+row.Before)
		}
	}
	var physical []string
	for _, entry := range [][2]string{
		{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
		{"internal/backend/runforkpersistence/selected_refusal_observation.go", "CountSelectedForkRunsForSourceTx"},
		{"internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunEventsForTest"},
		{"internal/backend/delivery/read_projections.go", "CountSelectedForkDeliveryStorageTx"},
		{"internal/backend/pipelinepersistence/selected_cleanup_fixture.go", "CountGlobalPipelineReceiptStorageTx"},
		{"internal/backend/pipelinepersistence/workflow_projection_observation.go", "CountWorkflowFieldRowsForRun"},
		{"internal/backend/mutationlog/selected_cleanup_observation.go", "CountRunMutationStorageTx"},
		{"internal/backend/runforkpersistence/selected_cleanup_fixture.go", "CountSelectedForkBindingStorageTx"},
		{"internal/backend/runforkpersistence/selected_cleanup_fixture.go", "CountSelectedForkExecutionStorageTx"},
		{"internal/backend/runforkpersistence/selected_cleanup_fixture.go", "CountSelectedForkRouteRecoveryStorageTx"},
		{"internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadRunStatusStorageTx"},
	} {
		owner := family145ConversationForkOwnerSource(t, entry[0], entry[1])
		physical = append(physical, family145ConnectorSQL(t, "package proof\n"+owner)...)
	}
	if len(original) != 11 || !reflect.DeepEqual(original, physical) {
		t.Fatalf("cleanup changed original SQL order/scope/status/cardinality: old=%v new=%v", original, physical)
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_selected_cleanup_fixture.go", "ReadPostgresSelectedForkCleanupStorageForTest")
	if err := family145SelectedCleanupAssemblyProofError(bridge); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{"&out.Deliveries", "&out.Events"}, {"&out.GlobalReceipts", "&out.Lineage"},
		{"&out.Mutations", "&out.State"}, {"&out.Bindings", "&out.Lineage"},
		{"return SelectedForkCleanupStorageCounts{}, fmt.Errorf", "return out, fmt.Errorf"},
		{"value, err := step.read(ctx, tx)", "value, err := 0, error(nil)"},
	} {
		if family145SelectedCleanupAssemblyProofError(strings.ReplaceAll(bridge, pair[0], pair[1])) == nil {
			t.Fatalf("cleanup accepted wrong field, synthetic zero or partial failure evidence: %v", pair)
		}
	}
}

func family145SelectedCleanupAssemblyProofError(bridge string) error {
	last := -1
	for _, cut := range []string{
		"validatePostgresPublicationIdentities(selected, run)", "CountPhysicalRunEventsForTest(ctx, selected, run)",
		"delivery.CountSelectedForkDeliveryStorageTx(ctx, tx, run)", "pipelinepersistence.CountGlobalPipelineReceiptStorageTx",
		"pipelinepersistence.CountWorkflowFieldRowsForRun(ctx, tx, run)", "mutationlog.CountRunMutationStorageTx(ctx, tx, run)",
		"runforkpersistence.CountSelectedForkBindingStorageTx(ctx, tx, run)", "runforkpersistence.CountSelectedForkExecutionStorageTx(ctx, tx, run)",
		"runforkpersistence.CountSelectedForkRouteRecoveryStorageTx(ctx, tx, run)", "for _, step := range steps", "readServedDeliveryObservation(ctx, selected,",
		"value, err := step.read(ctx, tx)", "*step.count = value", "return SelectedForkCleanupStorageCounts{}, fmt.Errorf(\"count %s: %w\", step.label, err)",
	} {
		position := strings.Index(bridge, cut)
		if position <= last {
			return fmt.Errorf("cleanup assembly lost exact owner, independent cut or failure discard: %s", cut)
		}
		last = position
	}
	if !strings.Contains(bridge, "out.Events, err = CountPhysicalRunEventsForTest(ctx, selected, run)") {
		return fmt.Errorf("cleanup event field no longer receives its exact independent read")
	}
	for _, pair := range [][2]string{
		{"delivery.CountSelectedForkDeliveryStorageTx(ctx, tx, run)", "&out.Deliveries"},
		{"pipelinepersistence.CountGlobalPipelineReceiptStorageTx", "&out.GlobalReceipts"},
		{"pipelinepersistence.CountWorkflowFieldRowsForRun(ctx, tx, run)", "&out.State"},
		{"mutationlog.CountRunMutationStorageTx(ctx, tx, run)", "&out.Mutations"},
		{"runforkpersistence.CountSelectedForkBindingStorageTx(ctx, tx, run)", "&out.Bindings"},
		{"runforkpersistence.CountSelectedForkExecutionStorageTx(ctx, tx, run)", "&out.Lineage"},
		{"runforkpersistence.CountSelectedForkRouteRecoveryStorageTx(ctx, tx, run)", "&out.RouteRecoveries"},
	} {
		read, field := strings.Index(bridge, pair[0]), strings.Index(bridge, pair[1])
		if read < 0 || field < read || strings.Contains(bridge[read:field], "&out.") {
			return fmt.Errorf("cleanup field is not bound to its exact physical read: %v", pair)
		}
	}
	return nil
}
