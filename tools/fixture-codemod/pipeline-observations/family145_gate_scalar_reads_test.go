package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145GateScalarReadCuts = map[string][]string{
	"prospective_publication_external_test.go|TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores": {
		"initialStorage, err := storetest.ReadWorkflowRunFieldsRevisionStorage(context.Background(), selected.events, runID)",
		"initialFields, initialRevision := initialStorage.Fields, initialStorage.Revision",
		"storage, err := storetest.ReadWorkflowRunFieldsRevisionStorage(context.Background(), selected.events, runID)",
		"fields, fieldRevision := storage.Fields, storage.Revision",
		"headerRevision, err := storetest.ReadWorkflowRunHeaderRevisionStorage(context.Background(), selected.events, runID)",
		"states, err := storetest.CountWorkflowRunFieldRows(context.Background(), selected.events, runID)",
		"published, err := storetest.CountRunEventNameStorage(context.Background(), selected.events, runID, \"work.ready\")",
		"fields, err := storetest.ReadWorkflowRunFieldsStorage(context.Background(), selected.events, runID)",
		"companions, err := storetest.CountRunFlowInstanceStorage(context.Background(), selected.events, runID)",
	},
	"a2_bound_reply_binding_external_test.go|testA2BoundReplyJourney": {
		"replies, err := storetest.CountGateSourceNamedEvents(ctx, selected.events, source.ResolveFlowEventReference(\"provider\", \"provider.replied\"), requestID)",
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
	},
	"pause_eligibility_test.go|TestPausedHandedNodeParksUntilContinueBothStores": {
		"before, err := storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
		"after, err := storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
		"after, err = storetest.CountRunEventNameStorage(context.Background(), s.events, runID, \"selected\")",
	},
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

func family145GateScalarReadKey(file, function string) string {
	return strings.TrimPrefix(file, "internal/runtime/pipeline/") + "|" + function
}

func family145GateScalarReadProofError(key, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
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

func TestFamily145GateScalarReadsKeepAllContextScopeDestinationsAndBusinessAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		key := family145GateScalarReadKey(row.File, row.Function)
		cuts, ok := family145GateScalarReadCuts[key]
		if !ok {
			continue
		}
		if found[key] {
			t.Fatalf("ambiguous scalar read consumer %s", key)
		}
		found[key] = true
		if err := family145GateScalarReadProofError(key, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145GateScalarReadProofError(key, row.Before, strings.Replace(row.After, cut, "foreignCut()", 1)) == nil {
				t.Fatalf("missing bound scalar read passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145GateScalarReadProofError(key, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original refusal/cardinality/cancellation assertion passed: %s", key)
			}
		}
	}
	if len(found) != len(family145GateScalarReadCuts) {
		t.Fatalf("scalar read consumer inventory: %d/%d", len(found), len(family145GateScalarReadCuts))
	}
}
