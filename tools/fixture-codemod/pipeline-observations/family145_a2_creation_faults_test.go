package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145A2CreationFaultCuts = map[string][]string{
	"a2ActivationJoinScheduleFault": {
		"if err := storetest.SetA2ActivationJoinScheduleFailure(ctx, selected.events, true); err != nil {",
		"return func() {",
		"if err := storetest.SetA2ActivationJoinScheduleFailure(ctx, selected.events, false); err != nil {",
	},
	"TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores": {
		"drop := a2ActivationJoinScheduleFault(t, ctx, selected)",
		"failed, err := bus.CommitFlowInstanceActivation(ctx, plan)",
		`if err == nil || !strings.Contains(err.Error(), "a2_activation_join_sql_fault") || failed.Acknowledged {`,
		"drop()",
		"committed, err := bus.CommitFlowInstanceActivation(ctx, plan)",
	},
	"TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores": {
		"before := load()",
		"if err := storetest.SetA2JoinCloseScheduleFailure(ctx, selected.events, true); err != nil {",
		`failed := publish("b", "failed", "dead_letter")`,
		"failureJSON, err := storetest.ReadGateEventFailure(ctx, selected.events, failed.ID())",
		"if err := storetest.SetA2JoinCloseScheduleFailure(ctx, selected.events, false); err != nil {",
		"if err := schedules.Stop(ctx); err != nil {",
	},
	"TestA2AccumulatorPublicationFailureRollsBackOnBothStores": {
		"before := proof.evidence(t)",
		"if err := storetest.SetA2AccumulatorPublicationFailure(context.Background(), proof.selected.events, true); err != nil {",
		`publication := proof.publish(t, "item.keyed", ` + "`" + `{"id":"refused-at-commit","marker":"must-not-persist"}` + "`" + `)`,
		"forward, _, outcome, executionErr := proof.pc.InterceptDeliveryRoute(proof.ctx, delivery, publication.route)",
		"proof.restart()",
		"if err := storetest.SetA2AccumulatorPublicationFailure(context.Background(), proof.selected.events, false); err != nil {",
	},
}

func family145A2CreationFaultProofError(key, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(family145NormalizeGateCountAssertions(key, after))
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("A2 creation fault changed original rollback, receipt, identity or replay assertions: %s", key)
	}
	function := strings.SplitN(key, "|", 2)[1]
	cursor := -1
	for _, cut := range family145A2CreationFaultCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor {
			return fmt.Errorf("A2 fault changed exact native owner, context or installation/removal cut: %s", cut)
		}
		cursor = at
	}
	return nil
}

func assertFamily145A2FaultPhysicalStatements(t *testing.T, function, original string) {
	t.Helper()
	var file, method, bridgeFile string
	switch function {
	case "a2ActivationJoinScheduleFault":
		file, method, bridgeFile = "genericschedule/test_a2_join_schedule_faults.go", "SetA2ActivationJoinScheduleFailureForTest", "test_workflow_scheduler_observation.go"
	case "TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores":
		file, method, bridgeFile = "genericschedule/test_a2_join_schedule_faults.go", "SetA2JoinCloseScheduleFailureForTest", "test_workflow_scheduler_observation.go"
	case "TestA2AccumulatorPublicationFailureRollsBackOnBothStores":
		file, method, bridgeFile = "eventrecord/a2_accumulator_publication_fault.go", "SetA2AccumulatorPublicationFailureTx", "test_event_support.go"
	default:
		return
	}
	physical := family145ConversationForkOwnerSource(t, "internal/backend/"+file, method)
	for _, keyword := range []string{"CREATE", "DROP"} {
		want := family145GateRecoveryStatements(t, "package proof\n"+original, keyword)
		got := family145GateRecoveryStatements(t, "package proof\n"+physical, keyword)
		if len(want) == 0 || !reflect.DeepEqual(want, got) {
			t.Fatalf("%s changed exact trigger/table/error/condition/action: %v/%v", method, want, got)
		}
	}
	bridgeMethod := strings.TrimSuffix(method, "Tx")
	if !strings.HasSuffix(bridgeMethod, "ForTest") {
		bridgeMethod += "ForTest"
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/"+bridgeFile, bridgeMethod)
	coordinator := "return runWorkflowProjectionFault(ctx, selected,"
	if bridgeFile == "test_event_support.go" {
		coordinator = "return runUnrevisionedEventFixtureTransactionForTest(ctx, selected,"
	}
	if strings.Count(bridge, coordinator) != 1 || strings.Count(bridge, method+"(ctx, tx, postgres, enabled)") != 1 {
		t.Fatalf("%s bypassed original selected coordinator or changed enabled/dialect/context", method)
	}
}
