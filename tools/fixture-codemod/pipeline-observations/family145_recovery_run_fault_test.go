package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145RecoveryRunFaultCuts = []string{
	"storetest.StartPostgresRuntimeStore(t)", "registerDifferentTestAuthorActivityCatalog(t, pg,",
	"requireCanonicalRuntimeLogSurface(t, ctx, pg)", "conformanceRuntimeLogPayloadAdmitter",
	"conformanceTestRuntimeOccurrence(t, authorActivityTestSourceArtifactFact.BundleHash())", "newScopedTestEventBus(t, pg,",
	"runtimebustest.SubscribeForRun(t, bus, replayRunID, replayRecipient)", "acknowledgeConformancePipelineEvent(t, ctx, pg.PipelineObligations(), replayParentID)",
	"acknowledgeConformancePipelineEvent(t, ctx, pg.PipelineObligations(), skipParentID)",
	"storetest.ClearEventRunForRecovery(ctx, pg, droppedEventID)", "bus.DeliveryAuthority()", "pg.ActivateDeliveryAuthority(ctx, authority)",
	"runtimedeliverycontinuation.New(pg, pg, authority, workOwner, bus, nil)", "bus.SetDeliveryContinuationOwner(continuations)",
	"runtimepipeline.NewRecoveryManagerWith(bus)", "continuations.Start(ctx)", "context.WithTimeout(context.Background(), 5*time.Second)",
	"continuations.Retire(retireCtx)", "pg.ClaimDelivery(ctx, authority, delivery.Event(), replayRoute)",
	"worklifetime.NewEventDeliveryCarrierGuard(delivery)", "carrier.Consume(nil)",
	"pg.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())",
	"sync.OnceValues(func() (worklifetime.DeliveryContinuationResolution, error)",
	"return carrier.Complete(nil)", "t.Errorf(\"join continued local delivery ownership: %v\", err)",
	"time.After(5 * time.Second)", "pg.ListOperatorRuntimeLogs(ctx,",
}

func family145RecoveryRunFaultProofError(before, after string) error {
	if err := family145RecoveryRunFaultAssertionInventory(before, after); err != nil {
		return err
	}
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("recovery run fault changed an original business assertion or its order")
	}
	for _, name := range []string{"PersistedProjection", "RuntimeDiagnostic", "CommitSemanticEvent", "CommitSemanticEventWithRoutes", "ClaimDelivery", "Consume", "Complete", "SettleSuccess"} {
		if !reflect.DeepEqual(sourceOutcomePreparationCalls(before, name), sourceOutcomePreparationCalls(after, name)) {
			return fmt.Errorf("recovery fault changed an original event/claim/settlement/ownership input: %s", name)
		}
	}
	for _, cut := range family145RecoveryRunFaultCuts {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("recovery fault lost native owner or original lifetime/fault cut: %s", cut)
		}
	}
	if strings.Count(after, "rm.Recover(ctx)") != 2 || strings.Count(after, "return carrier.Complete(nil)") != 1 || strings.Count(after, "completeCarrier()") != 2 ||
		strings.Index(after, "storetest.ClearEventRunForRecovery(") > strings.Index(after, "pg.ActivateDeliveryAuthority(") ||
		strings.Index(after, "continuations.Retire(") > strings.Index(after, "continuations.Start(") ||
		strings.Index(after, "worklifetime.NewEventDeliveryCarrierGuard(delivery)") > strings.Index(after, "if delivery.ID() != replayChildID") ||
		strings.Index(after, "join continued local delivery ownership") > strings.Index(after, "if delivery.ID() != replayChildID") ||
		strings.Index(after, "join continued local delivery ownership") > strings.Index(after, "pg.ClaimDelivery(") ||
		strings.Index(after, "join continued local delivery ownership") > strings.Index(after, "carrier.Consume(") ||
		strings.LastIndex(after, "completeCarrier()") < strings.Index(after, "pg.SettleSuccess(") {
		return fmt.Errorf("recovery fault changed registration, normal completion, repeat-recovery or corruption order")
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func family145RecoveryRunFaultAssertionInventory(before, after string) error {
	old, err := runSetupAssertions(before)
	if err != nil {
		return err
	}
	current, err := runSetupAssertions(after)
	if err != nil {
		return err
	}
	want := map[string]int{}
	for _, assertion := range old {
		want[assertion]++
	}
	want[`t.Errorf("join continued local delivery ownership: %v", err)`]++
	for _, assertion := range current {
		want[assertion]--
	}
	for assertion, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("recovery fault lost an original assertion or added an unreviewed one: %s", assertion)
		}
	}
	return nil
}

func TestFamily145RecoveryRunFaultRetainsRealReplayNoRedispatchAndJoinedOwnership(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if row.Function != "TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader" {
			continue
		}
		found++
		if err := family145RecoveryRunFaultProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		cleanup := "defer func() {\n\t\t\tif _, err := completeCarrier(); err != nil {\n\t\t\t\tt.Errorf(\"join continued local delivery ownership: %v\", err)\n\t\t\t}\n\t\t}()"
		for _, cut := range []string{
			"t.Fatalf(\"continued local delivery id = %q, want %q\", delivery.ID(), replayChildID)\n\t\t}",
			"t.Fatalf(\"continued local delivery disposition = %q, want acquired\", claimResult.Disposition)\n\t\t}",
		} {
			if !strings.Contains(row.After, cleanup) || !strings.Contains(row.After, cut) {
				t.Fatal("receipt-to-claim cleanup counterexample no longer matches the reviewed source")
			}
			late := strings.Replace(row.After, cleanup, "", 1)
			late = strings.Replace(late, cut, cut+"\n\t\t"+cleanup, 1)
			if family145RecoveryRunFaultProofError(row.Before, late) == nil {
				t.Fatalf("carrier cleanup after identity/claim accepted: %s", cut)
			}
		}
		for _, cut := range family145RecoveryRunFaultCuts {
			if family145RecoveryRunFaultProofError(row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost original recovery fault/owner/lifetime cut accepted: %s", cut)
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
			if family145RecoveryRunFaultProofError(row.Before, hostile) == nil {
				t.Fatalf("lost original replay/skip/drop/no-redispatch assertion accepted: %s", assertion)
			}
		}
	}
	if found != 1 {
		t.Fatalf("recovery fault whole caller inventory=%d want1", found)
	}
}

func TestFamily145RecoveryRunFaultKeepsExactPhysicalWriteAndOriginalWriterCoordinator(t *testing.T) {
	owner := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/fixture_chain_depth.go", "ClearEventRunForRecoveryTx")
	for _, cut := range []string{
		"UPDATE events SET run_id = NULL WHERE event_id = $1::uuid", "UPDATE events SET run_id = NULL WHERE event_id = ?",
		"tx.ExecContext(ctx, query, eventID)", "result.RowsAffected()", "if changed != 1", "return err",
	} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("named recovery fault changed exact write/binding/absence/error contract: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ClearEventRunForRecoveryForTest")
	for _, cut := range []string{"validateStandaloneStorageEvent(selected, eventID)", "runUnrevisionedEventFixtureTransactionForTest(ctx, selected,", "eventrecord.ClearEventRunForRecoveryTx(ctx, tx, postgres, eventID)"} {
		if !strings.Contains(bridge, cut) {
			t.Fatalf("recovery fault lost exact unrevisioned selected writer: %s", cut)
		}
	}
	queue := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/recovery_run_fault_queue_test.go", "RecoveryRunFaultQueuedWritersForTest")
	if !strings.Contains(queue, "return selected.backend.QueuedWritersForTest()") {
		t.Fatal("admission proof stopped observing the original selected SQLite coordinator")
	}
}
