package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var family145SelectedCleanupCalls = map[string][]string{
	"TestExecuteSelectedContractRunForkRejectsSameEventReplayScopeWriteSkew": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.SetSelectedReplayScopeCollisionFault(ctx, pg, true)",
		"t.Cleanup(func() {",
		"storetest.SetSelectedReplayScopeCollisionFault(context.Background(), pg, false)",
		"assertSelectedContractExecutionCleanup(t, pg, sourceRunID, result.Materialization.ForkRunID)", "time.Unix(1700002320, 0).UTC()",
	},
	"TestExecuteSelectedContractRunForkCleansUpBeforeActivationOnPublishFailure": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.SetSelectedExecutionLineageInsertFault(ctx, pg, true)",
		"t.Cleanup(func() {",
		"storetest.SetSelectedExecutionLineageInsertFault(context.Background(), pg, false)",
		"assertSelectedContractExecutionCleanup(t, pg, sourceRunID, result.Materialization.ForkRunID)", "time.Unix(1700002335, 0).UTC()",
	},
	"assertSelectedContractExecutionCleanup": {
		"ctx := context.Background()", "storetest.ReadRunStatusStorage(ctx, selected, sourceRunID)",
		"storetest.CountSelectedForkRunsForSource(ctx, selected, sourceRunID)",
		"storetest.ReadPostgresSelectedForkCleanupStorage(ctx, selected, forkRunID)",
		"storetest.ReadRunStatusStorage(ctx, selected, forkRunID)",
		"forkEvents, forkDeliveries, forkReceipts, forkState = counts.Events, counts.Deliveries, counts.GlobalReceipts, counts.State",
		"forkMutations, bindingRows, lineageRows, routeRecoveryRows = counts.Mutations, counts.Bindings, counts.Lineage, counts.RouteRecoveries",
	},
}

func family145SelectedCleanupProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("selected cleanup changed original activation, retention, error or cardinality assertion: %s", function)
	}
	for _, cut := range family145SelectedCleanupCalls[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("selected cleanup lost exact owner, context, fields or temporal cut: %s", cut)
		}
	}
	if function == "assertSelectedContractExecutionCleanup" {
		if strings.Count(after, `if forkRunID != ""`) != 2 || strings.Index(after, "ReadRunStatusStorage(ctx, selected, sourceRunID)") > strings.Index(after, "CountSelectedForkRunsForSource(") ||
			strings.Index(after, "ReadPostgresSelectedForkCleanupStorage(") > strings.Index(after, "ReadRunStatusStorage(ctx, selected, forkRunID)") {
			return fmt.Errorf("cleanup changed independent source/fork read order or absent-fork guard")
		}
	} else {
		for _, call := range []string{"executeLiveSelectedContractRunFork", "selectedContractTestProcessCapability", "LoadRunForkSelectedContractSource"} {
			if !reflect.DeepEqual(sourceOutcomePreparationCalls(before, call), sourceOutcomePreparationCalls(after, call)) {
				return fmt.Errorf("selected cleanup changed actual execution/source/authority input: %s", call)
			}
		}
		if strings.Index(after, "t.Cleanup(") > strings.Index(after, "Fault(ctx, pg, true)") ||
			strings.Index(after, "Fault(ctx, pg, true)") > strings.Index(after, "executeLiveSelectedContractRunFork(") {
			return fmt.Errorf("selected cleanup moved fault or registered cleanup after installation")
		}
		seed := strings.Index(after, "seedSelectedConstructedRootHistory(")
		fault := strings.Index(after, "Fault(ctx, pg, true)")
		if seed < 0 || fault < 0 || (strings.Contains(function, "WriteSkew") && seed > fault) || (strings.Contains(function, "PublishFailure") && seed < fault) {
			return fmt.Errorf("selected cleanup changed fault-versus-source-construction cut")
		}
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func TestFamily145SelectedCleanupClosesBothCallersAndKeepsRealFaultExecution(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		cuts, ok := family145SelectedCleanupCalls[row.Function]
		if !ok {
			continue
		}
		if found[row.Function] {
			t.Fatalf("duplicate selected cleanup transition: %s", row.Function)
		}
		found[row.Function] = true
		if err := family145SelectedCleanupProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145SelectedCleanupProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("selected cleanup lost owner/clock/scope/field accepted: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			hostile, err := family145BusAssertionCounterexample(row.After, strings.SplitN(assertion, " => ", 2)[0])
			if err != nil {
				t.Fatal(err)
			}
			if family145SelectedCleanupProofError(row.Function, row.Before, hostile) == nil {
				t.Fatalf("selected cleanup lost original assertion: %s", assertion)
			}
		}
		if row.Function != "assertSelectedContractExecutionCleanup" {
			owner, function := "pipelinepersistence/selected_cleanup_fixture.go", "SetSelectedReplayScopeCollisionFaultTx"
			if strings.Contains(row.Function, "PublishFailure") {
				owner, function = "runforkpersistence/selected_cleanup_fixture.go", "SetSelectedExecutionLineageInsertFaultTx"
			}
			physical := family145ConversationForkOwnerSource(t, "internal/backend/"+owner, function)
			if family145SelectedCleanupDDL(t, row.Before) != family145SelectedCleanupDDL(t, physical) {
				t.Fatalf("selected fault changed exact original trigger/function/body: %s", row.Function)
			}
		}
	}
	if len(found) != len(family145SelectedCleanupCalls) {
		t.Fatalf("selected cleanup whole helper/caller closure=%v", found)
	}
}

func family145SelectedCleanupDDL(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "ddl-proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var ddl string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil || !strings.HasPrefix(strings.TrimSpace(text), "CREATE OR REPLACE FUNCTION") {
			return true
		}
		if ddl != "" {
			t.Fatal("selected fault contains more than one CREATE cut")
		}
		quoted := false
		var normalized []rune
		for _, c := range text {
			if c == '\'' {
				quoted = !quoted
			}
			if quoted || (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
				normalized = append(normalized, c)
			}
		}
		ddl = string(normalized)
		return true
	})
	if ddl == "" {
		t.Fatal("selected fault lost its exact original CREATE cut")
	}
	return ddl
}

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
