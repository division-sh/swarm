package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145GateScalarReadCuts = map[string][]string{
	"a2_join_commit_ack_loss_external_test.go|TestA2JoinCloseLostOwnerResultRecoversOnBothStores": {
		"continuationDiffs, err := storetest.CountRunEventMutationStorage(ctx, selected.events, runID, completionID)",
	},
	"a2_membership_snapshot_admission_external_test.go|TestA2MembershipSnapshotValidationOnBothStores": {
		"schedules, err := storetest.CountRunEventTimerStorage(f.ctx, selected.events, runID, wantEvent)",
	},
	"prospective_publication_external_test.go|TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores": {
		"initialStorage, err := storetest.ReadWorkflowRunFieldsRevisionStorage(context.Background(), selected.events, runID)",
		"initialFields, initialRevision := initialStorage.Fields, initialStorage.Revision",
		"storage, err := storetest.ReadWorkflowRunFieldsRevisionStorage(context.Background(), selected.events, runID)",
		"fields, fieldRevision := storage.Fields, storage.Revision",
		"headerRevision, err := storetest.ReadWorkflowRunHeaderRevisionStorage(context.Background(), selected.events, runID)",
		"if name == \"publication_failure\" {\n\t\t\t\t\tif err := storetest.InstallProspectivePublicationCreationFailure(context.Background(), selected.events); err != nil {",
		"states, err := storetest.CountWorkflowRunFieldRows(context.Background(), selected.events, runID)",
		"published, err := storetest.CountRunEventNameStorage(context.Background(), selected.events, runID, \"work.ready\")",
		"fields, err := storetest.ReadWorkflowRunFieldsStorage(context.Background(), selected.events, runID)",
		"companions, err := storetest.CountRunFlowInstanceStorage(context.Background(), selected.events, runID)",
	},
	"a2_bound_reply_binding_external_test.go|testA2BoundReplyJourney": {
		"replies, err := storetest.CountGateSourceNamedEvents(ctx, selected.events, source.ResolveFlowEventReference(\"provider\", \"provider.replied\"), requestID)",
		"replyID, err := storetest.ReadTemplateSourceNamedEventIdentity(ctx, selected.events, source.ResolveFlowEventReference(\"provider\", \"provider.replied\"), requestID)",
		"ordinaryID, err := storetest.ReadTemplateSourceNamedEventIdentity(ctx, selected.events, source.ResolveFlowEventReference(\"provider\", \"provider.notified\"), ordinaryTrigger.ID())",
	},
	"a2_count_join_execution_external_test.go|TestA2CountJoinRealExecutionAndRestartOnBothStores": {
		"eventsBefore, err := storetest.CountPhysicalRunEvents(ctx, selected.events, runID)",
		"eventsAfter, err := storetest.CountPhysicalRunEvents(ctx, selected.events, runID)",
	},
	"a2_join_deadline_execution_external_test.go|TestA2JoinDeadlineExecutionRetainsEntryAndPartialContextOnBothStores": {
		"publications, err := storetest.CountTemplateEventIdentity(ctx, selected.events, late.ID())",
	},
	"a2_accumulator_canonical_evidence_external_test.go|TestA2AccumulatorSchemaAdmittedCanonicalEvidenceOnBothStores": {
		"publications, err := storetest.CountTemplateEventIdentity(proof.ctx, proof.selected.events, event.ID())",
		"deliveries, err := storetest.CountTemplateEventDeliveries(proof.ctx, proof.selected.events, event.ID())",
	},
	"a2_accumulator_canonical_evidence_external_test.go|a2CanonicalAccumulatorEvidence": {
		"publications, err := storetest.CountRunEventNameStorage(proof.ctx, proof.selected.events, proof.runID, \"numeric.completed\")",
		"evidence.Publications = publications",
	},
	"a2_accumulator_persistence_external_test.go|evidence": {
		"publications, err := storetest.CountRunEventNameStorage(p.ctx, p.selected.events, p.runID, \"item.recorded\")",
		"evidence.Publications = publications",
	},
	"a2_accumulator_persistence_external_test.go|TestA2AccumulatorKeyedExecutionPersistenceOnBothStores": {
		"committed, err := storetest.CountTemplateEventIdentity(proof.ctx, proof.selected.events, event.ID())",
	},
	"a2_fieldless_reply_preservation_external_test.go|TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores": {
		"publications, err := storetest.CountRunEventNameStorage(ctx, selected.events, runID, name)",
	},
	"a2_map_fan_out_execution_external_test.go|assertProgress": {
		"eventsCount, err := storetest.CountRunEventNameStorage(p.ctx, p.selected.events, p.runID, \"item.ready\")",
	},
	"a2_publication_binding_external_test.go|testA2StageEntryPublicationBinding": {
		"routes, err := storetest.CountTemplateEventDeliveries(ctx, selected.events, outputID)",
		"outputID, err := storetest.ReadTemplateSourceNamedEventIdentity(ctx, selected.events, \"item.completed\", triggerID)",
	},
	"pause_eligibility_test.go|TestPausedHandedNodeParksUntilContinueBothStores": {
		"before, err := storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
		"after, err := storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
		"after, err = storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
	},
}

func TestFamily145GateCausalCountsRetainExactPhysicalPredicatesAndNativeReaders(t *testing.T) {
	for _, tc := range []struct{ physicalFile, physical, bridgeFile, bridge, ids, args string }{
		{"mutationlog/selected_cleanup_observation.go", "CountRunEventMutationStorageTx", "test_scenario_setup_observation.go", "CountRunEventMutationStorageForTest", "runID, eventID", "runID, eventID"},
		{"genericschedule/test_workflow_scheduler_observation.go", "CountRunEventTimerStorageTx", "test_workflow_scheduler_observation.go", "CountRunEventTimerStorageForTest", "runID", "runID, eventType"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/"+tc.physicalFile, tc.physical)
		queries := family145ConnectorSQL(t, "package proof\n"+physical)
		want := "SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2"
		if tc.physical == "CountRunEventTimerStorageTx" {
			want = "SELECT COUNT(*) FROM timers WHERE run_id=$1 AND fire_event=$2"
		}
		expected := family145ConnectorSQL(t, "package proof\nconst query = `"+want+"`")
		if len(queries) != 1 || queries[0] != expected[0] || strings.Count(physical, ", "+tc.args+").Scan(&count)") != 1 {
			t.Fatalf("%s changed original physical run/event conjunction, arguments or destination", tc.physical)
		}
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/"+tc.bridgeFile, tc.bridge)
		if strings.Count(bridge, "return readFanOutSemanticStorage(ctx, selected, []string{"+tc.ids+"},") != 1 || strings.Count(bridge, tc.physical+"(ctx, tx, "+tc.args+")") != 1 {
			t.Fatalf("%s escaped original selected reader or changed argument/context order", tc.bridge)
		}
	}
}

func TestFamily145ProspectiveProjectionSQLCorrespondsToAllOriginalCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original []string
	for _, row := range rows {
		if row.Function == "TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores" {
			original = family145ConnectorSQL(t, "package proof\n"+row.Before)
		}
	}
	if len(original) != 7 {
		t.Fatalf("prospective original read inventory=%d, want 7", len(original))
	}
	for _, function := range []string{"ReadWorkflowRunFieldsRevisionStorageTx", "ReadWorkflowRunHeaderRevisionStorageTx", "ReadWorkflowRunFieldsStorageTx", "CountWorkflowFieldRowsForRun"} {
		source := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_projection_observation.go", function)
		queries := family145ConnectorSQL(t, "package proof\n"+source)
		if len(queries) != 1 {
			t.Fatalf("physical %s read inventory=%d", function, len(queries))
		}
		found := false
		for _, query := range original {
			if strings.EqualFold(query, queries[0]) {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s changed original predicate, columns, casts or multiplicity: %s", function, queries[0])
		}
	}
}

func TestFamily145ProspectivePublicationFaultPreservesDDLPhaseAndClosedOwner(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original string
	for _, row := range rows {
		if row.Function == "TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores" {
			original = row.Before
		}
	}
	before := family145GateRecoveryStatements(t, "package proof\n"+original, "CREATE")
	physical := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/prospective_publication_fixture.go", "InstallProspectivePublicationCreationFailureTx")
	after := family145GateRecoveryStatements(t, "package proof\n"+physical, "CREATE")
	if len(before) != 3 || !reflect.DeepEqual(before, after) {
		t.Fatalf("prospective publication fault changed event/table/action/when/error/trigger or installation order: %v/%v", before, after)
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "InstallProspectivePublicationCreationFailureForTest")
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "_, postgres := selected.(*PostgresStore)", "return runUnrevisionedEventFixtureTransactionForTest(ctx, selected,", "return eventrecord.InstallProspectivePublicationCreationFailureTx(ctx, tx, postgres)"} {
		if strings.Count(bridge, cut) != 1 {
			t.Fatalf("publication fault escaped original selected event owner: %s", cut)
		}
	}
	consumer := family145ConversationForkOwnerSource(t, "../runtime/pipeline/prospective_publication_external_test.go", "TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores")
	for _, escaped := range []string{"selected.db", "selected.postgres", "BeginTx", "CREATE TRIGGER", "CREATE FUNCTION"} {
		if strings.Contains(consumer, escaped) {
			t.Fatalf("prospective publication retained raw authority or dialect interpretation: %s", escaped)
		}
	}
}

func TestFamily145GateSourceIdentitiesReuseOnlyOriginalNameAndSourcePredicates(t *testing.T) {
	physical := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadTemplateSourceNamedEventIdentityTx")
	if strings.Count(physical, "tx.QueryRowContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1 AND source_event_id=$2`, name, source).Scan(&out)") != 1 {
		t.Fatal("source identity changed its original physical projection, predicates or name/source argument order")
	}
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		if row.Function != "testA2BoundReplyJourney" && row.Function != "testA2StageEntryPublicationBinding" {
			continue
		}
		// Stored event UUID/text identity scanned into string is preserved by
		// the existing text projection; the two conjunctions commute, not widen.
		want := 1
		if row.Function == "testA2BoundReplyJourney" {
			want = 2
		}
		if strings.Count(row.Before, "SELECT event_id FROM events WHERE source_event_id=$1 AND event_name=$2") != want {
			t.Fatalf("original source identity inventory changed: %s", row.Function)
		}
		if err := family145GateScalarReadProofError(family145GateScalarReadKey(row.File, row.Function), row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("source identity consumer inventory=%d, want 2", seen)
	}
}

func family145GateScalarReadKey(file, function string) string {
	return strings.TrimPrefix(file, "internal/runtime/pipeline/") + "|" + function
}

func family145GateScalarReadProofError(key, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(family145NormalizeGateCountAssertions(key, after))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("gate scalar read lost original assertion: %s", key)
	}
	cursor := -1
	for _, cut := range family145GateScalarReadCuts[key] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor {
			return fmt.Errorf("gate scalar read lost exact owner/context/event/run/field or cut order: %s", cut)
		}
		cursor = at
	}
	return nil
}
