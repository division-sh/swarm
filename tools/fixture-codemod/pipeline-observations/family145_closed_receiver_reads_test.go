package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145ClosedReceiverCuts = map[string][]string{
	"assertClosedReceiverAuthorityEvidence": {
		"selected any", "deadline := time.Now().Add(10 * time.Second)",
		"closedReceiverEvidenceReady(t, ctx, selected, runID)", "closedReceiverEvidenceDiagnostic(t, ctx, selected, runID)",
		"case <-ctx.Done():", "case <-time.After(20 * time.Millisecond):",
		"storetest.ReadClosedReceiverAuthorityStorage(ctx, selected)",
		"map[string]bool{\"downstream-agent\": false, \"upstream-agent\": false}",
		"agentID, authorityKind, state, surfaceActor, executionKind := row.AgentID, row.AuthorityKind, row.State, row.SurfaceActor, row.ExecutionKind",
		"storetest.ReadClosedReceiverEmissionStorage(ctx, selected, runID, \"task.completed\")",
		"completedID, completedBy, completedType, completedSource := completed.EventID, completed.ProducedBy, completed.ProducedType, completed.SourceEventID",
		"storetest.ReadClosedReceiverEmissionStorage(ctx, selected, runID, \"task.finalized\")",
		"finalizedID, finalizedBy, finalizedType, finalizedSource := finalized.EventID, finalized.ProducedBy, finalized.ProducedType, finalized.SourceEventID",
		"identitytest.RootNode(t, \"bridge-node\").Key()", "storetest.CountClosedReceiverPipelineSuccessStorage(ctx, selected, runID)",
		"if receipts < 3", "_ = finalizedID",
	},
	"closedReceiverEvidenceDiagnostic": {
		"selected any", "storetest.ReadClosedReceiverDeliveryStorage(ctx, selected, runID)",
		"eventName, flowInstance, entityID, targetRoute := row.EventName, row.FlowInstance, row.EntityID, row.TargetRoute",
		"subscriberType, subscriberID, status, reason, failure := row.SubscriberType, row.SubscriberID, row.Status, row.Reason, row.Failure",
		"fmt.Sprintf(\"%s[%s/%s target=%s]:%s/%s:%s:%s:%s\", eventName, flowInstance, entityID, targetRoute, subscriberType.String, subscriberID.String, status.String, reason.String, failure.String)",
		"storetest.ReadClosedReceiverReceiptStorage(ctx, selected, runID)",
		"eventName, subscriberType, subscriberID, outcome, reason, failure := row.EventName, row.SubscriberType, row.SubscriberID, row.Outcome, row.Reason, row.Failure",
		"fmt.Sprintf(\"receipt:%s:%s/%s:%s:%s:%s\", eventName, subscriberType, subscriberID, outcome, reason, failure)",
		"storetest.ReadClosedReceiverAgentStorage(ctx, selected)",
		"agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, generation, runMode, status := row.AgentID, row.NameOwner, row.NameSource, row.RoutePresence, row.Scope, row.InstanceID, row.FlowInstance, row.EntityID, row.Phase, row.Generation, row.RunMode, row.Status",
		"fmt.Sprintf(\"agent:%s:%s/%s/%s/%s/%s/%s/%s:%s/%d/%s/%s\", agentID, nameOwner, nameSource, routePresence, scope, instanceID, flowInstance, entityID, phase, generation, runMode, status)",
		"storetest.ReadClosedReceiverAgentRouteStorage(ctx, selected, runID)",
		"agentID, owner, source, presence, scope, instanceID, instancePath := row.AgentID, row.Owner, row.Source, row.Presence, row.Scope, row.InstanceID, row.InstancePath",
		"fmt.Sprintf(\"agent-route:%s:%s/%s/%s/%s/%s/%s\", agentID, owner, source, presence, scope, instanceID, instancePath)",
		"storetest.ReadClosedReceiverRuntimeLogStorage(ctx, selected, runID)", "facts = append(facts, \"runtime-log:\"+payload)",
	},
	"closedReceiverEvidenceReady": {
		"selected any", "identitytest.RootNode(t, \"bridge-node\").Key()",
		"storetest.ReadClosedReceiverReadinessStorage(ctx, selected, runID, bridgeNodeID)",
		"actors, eventsCount, deliveries := observed.Actors, observed.Events, observed.Deliveries",
		"return actors == 2 && eventsCount == 2 && deliveries == 3",
	},
}

func family145ClosedReceiverProofError(function, before, after string) error {
	if function == "TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain" {
		return family145ClosedReceiverConstructorProofError(before, after)
	}
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	normalized := strings.Replace(after, "closedReceiverEvidenceDiagnostic(t, ctx, selected, runID)", "closedReceiverEvidenceDiagnostic(t, ctx, db, backend, runID)", 1)
	normalized = strings.Replace(normalized, "closedReceiverEvidenceReady(t, ctx, selected, runID)", "closedReceiverEvidenceReady(t, ctx, db, backend, runID)", 1)
	got, err := family145DescriptorBusinessAssertions(normalized)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("closed receiver lost original authority/lineage/receipt/refusal assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145ClosedReceiverCuts[function] {
		at := strings.Index(after, cut)
		if at <= cursor {
			return fmt.Errorf("closed receiver lost an independent query/field/window: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryContext(") || strings.Contains(after, "QueryRowContext(") || strings.Contains(after, "*sql.DB") || strings.Contains(after, "backend ==") {
		return fmt.Errorf("closed receiver retains raw/dialect authority")
	}
	return nil
}

func family145ClosedReceiverConstructorProofError(before, after string) error {
	const native = "var selected closedReceiverE2EStore\n\t\t\tif backend == \"postgres\" {\n\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)\n\t\t\t} else {\n\t\t\t\tselected = storetest.StartSQLiteRuntimeStore(t)\n\t\t\t}"
	begin, end := strings.Index(before, "var ("), strings.Index(before, "\n\t\t\trunID :=")
	if begin < 0 || end < begin || strings.Count(after, native) != 1 || strings.Count(after, "requireClosedReceiverDiagnosticReadback(t, ctx, selected, runID)") != 1 {
		return fmt.Errorf("receiver construction is not the exact two native owners")
	}
	normalized := strings.Replace(after, native, before[begin:end], 1)
	normalized = strings.Replace(normalized, "\n\t\t\trequireClosedReceiverDiagnosticReadback(t, ctx, selected, runID)", "", 1)
	normalized = strings.Replace(normalized, "assertClosedReceiverAuthorityEvidence(t, ctx, selected, backend, runID, root.ID())", "assertClosedReceiverAuthorityEvidence(t, ctx, db, backend, runID, root.ID())", 1)
	const workflow = "workflowPersistence := runtimepipeline.NewWorkflowPersistence(selected)"
	const duplicate = "\n\t\t\tif backend == \"sqlite\" {\n\t\t\t\tworkflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)\n\t\t\t}"
	normalized = strings.Replace(normalized, workflow, workflow+duplicate, 1)
	if normalized != before {
		return fmt.Errorf("receiver construction changed source/admission/configuration/publication/cleanup or behavioral evidence")
	}
	return nil
}

func TestFamily145ClosedReceiverWholeChainPreservesExactNativeConstructionAndAllFields(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		if row.File != "internal/runtime/eventbus_receiver_authority_e2e_test.go" {
			continue
		}
		seen++
		if err := family145ClosedReceiverProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		cuts := family145ClosedReceiverCuts[row.Function]
		if row.Function == "TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain" {
			cuts = []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)", "processOwner := worklifetime.NewProcess()", "closeExternalRuntimeTestGeneration(rt, processOwner, capability)", "root.ID()", "requireClosedReceiverDiagnosticReadback(t, ctx, selected, runID)"}
		}
		for _, cut := range cuts {
			if family145ClosedReceiverProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignReceiverCut()", 1)) == nil {
				t.Fatalf("receiver construction/field/identity/window mutation passed: %s", cut)
			}
		}
	}
	if seen != 4 {
		t.Fatalf("receiver whole-family inventory=%d/4", seen)
	}
}

func family145ClosedReceiverOwnedQueries(t *testing.T) map[string]bool {
	t.Helper()
	owners := map[string][]string{
		"internal/backend/effectpersistence/closed_receiver_observation.go":   {"ReadClosedReceiverAuthorityStorageTx"},
		"internal/backend/agentpersistence/closed_receiver_observation.go":    {"ReadClosedReceiverAgentStorageTx"},
		"internal/backend/eventrecord/closed_receiver_observation.go":         {"ReadClosedReceiverEmissionStorageTx", "ReadClosedReceiverRuntimeLogStorageTx"},
		"internal/backend/pipelinepersistence/closed_receiver_observation.go": {"CountClosedReceiverPipelineSuccessStorageTx", "ReadClosedReceiverReceiptStorageTx"},
		"internal/backend/delivery/read_projections.go":                       {"ReadClosedReceiverReadinessStorageTx", "ReadClosedReceiverDeliveryStorageTx", "ReadClosedReceiverAgentRouteStorageTx"},
	}
	owned := map[string]bool{}
	for path, functions := range owners {
		for _, function := range functions {
			source := family145ConversationForkOwnerSource(t, path, function)
			for _, query := range family145A2ClaimLiteralQueries(t, source) {
				if owned[query] {
					t.Fatalf("duplicate receiver SQL owner: %s", query)
				}
				owned[query] = true
			}
			if function == "ReadClosedReceiverDeliveryStorageTx" || function == "ReadClosedReceiverReceiptStorageTx" {
				if !strings.Contains(source, "strings.Replace(query, \"e.run_id = ?\", \"e.run_id = $1::uuid\", 1)") {
					t.Fatal("receiver dialect changed the exact run placeholder substitution")
				}
			}
		}
	}
	return owned
}

func TestFamily145ClosedReceiverEntireDialectQueryInventoryHasExactClosedOwners(t *testing.T) {
	owned := family145ClosedReceiverOwnedQueries(t)
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	consumed := map[string]bool{}
	for _, row := range rows {
		if row.File != "internal/runtime/eventbus_receiver_authority_e2e_test.go" {
			continue
		}
		for _, query := range family145A2ClaimLiteralQueries(t, row.Before) {
			// The original SQLite emission query was assembled from these
			// fixed fragments; the new owner spells the same statement whole.
			if strings.HasSuffix(query, "COALESCE(source_event_id, '') FROM events WHERE run_id =") {
				query += " ? AND event_name = ?"
			}
			if !owned[query] {
				t.Fatalf("receiver physical query lost scope/columns/NULL/order: %s", query)
			}
			consumed[query] = true
		}
	}
	if len(consumed) != len(owned) || len(owned) != 14 {
		t.Fatalf("receiver dialect-query inventory consumed=%d owned=%d/14", len(consumed), len(owned))
	}
}

func TestFamily145ClosedReceiverPositiveDiagnosticProofCannotLoseASection(t *testing.T) {
	source := family145ConversationForkOwnerSource(t, "../runtime/eventbus_receiver_authority_e2e_test.go", "requireClosedReceiverDiagnosticReadback")
	cuts := []string{
		"closedReceiverEvidenceDiagnostic(t, ctx, selected, runID)",
		"\"task.assigned[\"", "\"task.completed[\"", "\"task.finalized[\"",
		"\"receipt:task.assigned:platform/pipeline:success:\"", "\"receipt:task.completed:platform/pipeline:success:\"", "\"receipt:task.finalized:platform/pipeline:success:\"",
		"\"agent:upstream-agent:\"", "\"agent:downstream-agent:\"", "\"agent-route:upstream-agent:\"", "\"agent-route:downstream-agent:\"",
		"if !strings.Contains(evidence, fragment)", "t.Fatalf(\"closed receiver diagnostic lost persisted %q: %s\", fragment, evidence)",
	}
	validate := func(source string) bool {
		for _, cut := range cuts {
			if strings.Count(source, cut) != 1 {
				return false
			}
		}
		return true
	}
	if !validate(source) {
		t.Fatal("receiver diagnostic positive proof lost a section, source, context or refusal")
	}
	for _, cut := range cuts {
		if validate(strings.Replace(source, cut, "foreignReceiverEvidence()", 1)) {
			t.Fatalf("weakened diagnostic execution proof accepted: %s", cut)
		}
	}
}
