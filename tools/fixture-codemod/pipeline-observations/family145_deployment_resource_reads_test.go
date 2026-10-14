package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145DeploymentReadCuts = map[string][]string{
	"waitDeploymentForkRowEvent": {
		"selected any", "deadline := time.Now().Add(10 * time.Second)",
		"storetest.ReadFirstRootReadyEventStorage(ctx, selected, runID)", "if err != sql.ErrNoRows", "time.Sleep(20 * time.Millisecond)",
		"storetest.ReadDeploymentIntentProgressStorage(ctx, selected, runID)", "intents, cursor = progress.Intents, progress.Cursor",
		"storetest.CountNestedRunFanOutOutcomes(ctx, selected, runID)", "storetest.CountPhysicalRunEvents(ctx, selected, runID)",
		"storetest.CountRunDeliveryStorage(ctx, selected, runID)", "storetest.CountRunFlowInstanceStorage(ctx, selected, runID)",
		"storetest.ReadDeploymentIntentMetadataStorage(ctx, selected, runID)",
		"status, intentBundle, origin = metadata.Status, metadata.BundleHash, metadata.Origin",
		"sourceKind, feedID, blocked, claimant, retry = metadata.SourceKind, metadata.FeedID, metadata.Blocked, metadata.Claimant, metadata.Retry",
		"wake()", "time.Sleep(2 * time.Second)", "storetest.CountRunNamedEventStorage(ctx, selected, runID, \"root.ready\")",
		"storetest.ReadDeploymentRunStatusStorage(ctx, selected, runID)", "runStatus, bundleHash := run.Status, run.BundleHash",
		"storetest.CountAdmittedUnboundBundleGrantsStorage(ctx, selected, bundleHash)",
	},
	"selectedDeploymentForkOperationSnapshot": {
		"storetest.ReadSelectedDeploymentOperationStorage(f.ctx, f.selected, forkRunID)",
		"snapshot.id, snapshot.state, snapshot.result, snapshot.updatedAt = observed.ID, observed.State, observed.Result, observed.UpdatedAt",
	},
	"assertSelectedDeploymentRowSet": {
		"logDeploymentResourceFailure(t, f.selected, runID)", "waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)",
		"storetest.ReadRootReadyResourcePinStorage(f.ctx, f.selected, runID)", "storetest.ReadDeploymentIntentVersionStorage(f.ctx, f.selected, runID)",
		"storetest.CountRunNamedEventStorage(f.ctx, f.selected, runID, \"root.ready\")", "&events",
		"storetest.CountRootReadyDeliveredStorage(f.ctx, f.selected, runID)", "&delivered",
		"storetest.CountDeploymentCommittedOutcomesStorage(f.ctx, f.selected, runID)", "&committed",
		"storetest.CountRootReadySuccessfulPipelineReceiptsStorage(f.ctx, f.selected, runID)", "&pipelineReceipts",
		"storetest.CountRootReadyStampedHandoffsStorage(f.ctx, f.selected, runID)", "&stampedHandoffs",
		"count, err := check.read()", "*check.value = count", "deadline := time.Now().Add(5 * time.Second)", "time.Sleep(25 * time.Millisecond)",
		"pipelineReceipts, err = storetest.CountRootReadySuccessfulPipelineReceiptsStorage(f.ctx, f.selected, runID)",
		"stampedHandoffs, err = storetest.CountRootReadyStampedHandoffsStorage(f.ctx, f.selected, runID)",
		"storetest.ReadDeploymentParentRunStorage(f.ctx, f.selected, runID)", "if parentRunID.Valid",
		"storetest.CountClosedSelectedDeploymentExecutionsStorage(f.ctx, f.selected, runID)",
	},
	"logSelectedDeploymentForkFailure": {
		"selected any", "if forkRunID == \"\"", "context.WithTimeout(context.Background(), 3*time.Second)",
		"storetest.ReadDeploymentRunControlStorage(ctx, selected, forkRunID)", "control.Status, control.ControlStatus",
		"storetest.ReadDeploymentFeedStorage(ctx, selected, forkRunID)",
		"feedID, status, cursor, cardinality, claimant, generation, lease, retryAt, retryFailure, blocked := row.FeedID, row.Status, row.Cursor, row.Cardinality, row.Claimant, row.Generation, row.Lease, row.RetryAt, row.RetryFailure, row.Blocked",
		"storetest.ReadSelectedDeploymentExecutionsStorage(ctx, selected, forkRunID)", "executionID, generation, state, lease := row.ExecutionID, row.Generation, row.State, row.Lease",
		"storetest.CountNestedRunFanOutOutcomes(ctx, selected, forkRunID)", "storetest.CountPhysicalRunEvents(ctx, selected, forkRunID)",
		"storetest.CountDeploymentPipelineReceiptsStorage(ctx, selected, forkRunID)", "storetest.CountRunDeliveryStorage(ctx, selected, forkRunID)",
		"storetest.CountDeploymentPendingDeliveriesStorage(ctx, selected, forkRunID)", "storetest.CountDeploymentFailedDeliveriesStorage(ctx, selected, forkRunID)",
		"storetest.CountDeploymentStampedHandoffsStorage(ctx, selected, forkRunID)", "storetest.CountRunFlowInstanceStorage(ctx, selected, forkRunID)",
		"storetest.ReadDeploymentReceiptStorage(ctx, selected, forkRunID)", "eventID, subscriberType, subscriberID, outcome, reason, failure := row.EventID, row.SubscriberType, row.SubscriberID, row.Outcome, row.Reason, row.Failure",
		"storetest.ReadDeploymentDeliveryStorage(ctx, selected, forkRunID)", "deliveryID, eventID, subscriberType, subscriberID, status, retryCount, claimVersion, nextEligible, handoff, reason, failure := row.DeliveryID, row.EventID, row.SubscriberType, row.SubscriberID, row.Status, row.RetryCount, row.ClaimVersion, row.NextEligible, row.Handoff, row.Reason, row.Failure",
		"storetest.ReadDeploymentAttemptStorage(ctx, selected, forkRunID)", "deliveryID, claimVersion, closure, outcome, reason, failure := row.DeliveryID, row.ClaimVersion, row.Closure, row.Outcome, row.Reason, row.Failure",
		"storetest.ReadDeploymentRuntimeLogStorage(ctx, selected, forkRunID)", "eventID, payload := row.EventID, row.Payload",
	},
	"logDeploymentResourceFailure": {
		"selected any", "context.WithTimeout(context.Background(), 3*time.Second)", "storetest.ReadDeploymentIntentDiagnosticsStorage(ctx, selected, runID)",
		"status, cardinality, cursor := row.Status, row.Cardinality, row.Cursor",
		"storetest.CountNestedRunFanOutOutcomes(ctx, selected, runID)", "&outcomes", "storetest.CountPhysicalRunEvents(ctx, selected, runID)", "&events",
		"storetest.CountRunDeliveryStorage(ctx, selected, runID)", "&deliveries", "storetest.CountRunFlowInstanceStorage(ctx, selected, runID)", "&receivers",
		"count, err := check.read()", "*check.value = count", "storetest.ReadDeploymentDeliveryStatusStorage(ctx, selected, runID)", "status, count := row.Status, row.Count",
		"storetest.ReadDeploymentRecentEventStorage(ctx, selected, runID)", "name, payload := row.Name, row.Payload", "if name == \"platform.runtime_log\"",
	},
}

func family145DeploymentReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("deployment read lost original public rows, version, receiver, deadline, wake, lifecycle or receipt assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145DeploymentReadCuts[function] {
		at := strings.Index(after, cut)
		if at <= cursor {
			return fmt.Errorf("deployment read lost or reordered an independent query/field/cut: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryContext(") || strings.Contains(after, "QueryRowContext(") || strings.Contains(after, "*sql.DB") {
		return fmt.Errorf("deployment helper retains raw query/pool authority")
	}
	return nil
}

func TestFamily145DeploymentObservationWholeChainPreservesAllFieldsAndIndependentCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		cuts := family145DeploymentReadCuts[row.Function]
		if cuts == nil {
			continue
		}
		if seen[row.Function] {
			t.Fatal("duplicate deployment helper transition")
		}
		seen[row.Function] = true
		if err := family145DeploymentReadProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145DeploymentReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignDeploymentCut()", 1)) == nil {
				t.Fatalf("lost or rebound owner/field/context/clock/cut passed: %s", cut)
			}
		}
	}
	if len(seen) != 5 {
		t.Fatalf("whole deployment helper inventory=%d/5", len(seen))
	}
}

var family145DeploymentQueryFiles = map[string][]string{
	"internal/durabledata/deployment_observation.go":                                   {"ReadRootReadyResourcePinStorageTx"},
	"internal/backend/pipelinepersistence/deployment_observation.go":                   {"ReadDeploymentIntentVersionStorageTx", "ReadDeploymentIntentProgressStorageTx", "ReadDeploymentIntentMetadataStorageTx", "ReadDeploymentFeedStorageTx", "ReadDeploymentIntentDiagnosticsStorageTx", "CountDeploymentCommittedOutcomesStorageTx", "CountDeploymentPipelineReceiptsStorageTx", "CountRootReadySuccessfulPipelineReceiptsStorageTx", "ReadDeploymentReceiptStorageTx"},
	"internal/backend/runforkpersistence/deployment_observation.go":                    {"ReadSelectedDeploymentOperationStorageTx", "ReadSelectedDeploymentExecutionsStorageTx", "CountClosedSelectedDeploymentExecutionsStorageTx"},
	"internal/backend/runlifecycle/deployment_observation.go":                          {"ReadDeploymentRunStatusStorageTx", "ReadDeploymentRunControlStorageTx", "ReadDeploymentParentRunStorageTx"},
	"internal/backend/delivery/read_projections.go":                                    {"CountDeploymentPendingDeliveriesStorageTx", "CountDeploymentFailedDeliveriesStorageTx", "CountDeploymentStampedHandoffsStorageTx", "CountRootReadyDeliveredStorageTx", "CountRootReadyStampedHandoffsStorageTx", "ReadDeploymentDeliveryStorageTx", "ReadDeploymentAttemptStorageTx", "ReadDeploymentDeliveryStatusStorageTx", "CountRunDeliveryStorageTx"},
	"internal/backend/eventrecord/deployment_observation.go":                           {"ReadFirstRootReadyEventStorageTx", "ReadDeploymentRuntimeLogStorageTx", "ReadDeploymentRecentEventStorageTx"},
	"internal/backend/generationauthority/storage_observation.go":                      {"CountAdmittedUnboundBundleGrantsStorageTx"},
	"internal/backend/pipelinepersistence/nested_fan_out_observation.go":               {"CountNestedRunFanOutOutcomesTx"},
	"internal/backend/pipelinepersistence/runtime_outcome_construction_observation.go": {"CountRunFlowInstanceStorageTx"},
	"internal/runtimepersistence/test_api_event_storage.go":                            {"CountPhysicalRunEventsForTest"},
}

func TestFamily145DeploymentObservationEntire44QueryInventoryHasExactClosedOwners(t *testing.T) {
	owned := map[string]string{}
	for path, functions := range family145DeploymentQueryFiles {
		for _, function := range functions {
			queries := family145A2ClaimLiteralQueries(t, family145ConversationForkOwnerSource(t, path, function))
			if len(queries) != 1 || owned[queries[0]] != "" {
				t.Fatalf("ambiguous or missing exact deployment SQL owner: %s %v", function, queries)
			}
			owned[queries[0]] = function
		}
	}
	// This is the existing named-event owner, with a fixed literal supplied by
	// both migrated consumers, not a parallel literal-count implementation.
	query := family145A2ClaimLiteralQueries(t, family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/run_name_cardinality.go", "CountRunNamedEventStorageTx"))
	if len(query) != 1 || query[0] != "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2" {
		t.Fatal("existing named-event count scope changed")
	}
	owned[strings.Replace(query[0], "$2", "'root.ready'", 1)] = "CountRunNamedEventStorageTx"
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	queries, consumed := 0, map[string]bool{}
	for _, row := range rows {
		if family145DeploymentReadCuts[row.Function] == nil {
			continue
		}
		for _, original := range family145A2ClaimLiteralQueries(t, row.Before) {
			queries++
			if owned[original] == "" {
				t.Fatalf("original query lost its exact predicate, columns, NULL handling or order: %s", original)
			}
			consumed[original] = true
		}
	}
	if queries != 44 || len(consumed) != len(owned) {
		t.Fatalf("deployment query consumption is incomplete: occurrences=%d/44 consumed=%d owned=%d", queries, len(consumed), len(owned))
	}
}

func TestFamily145DeploymentObservationCallerPropagationChangesOnlyOriginalCarrier(t *testing.T) {
	var current []map[string]any
	if err := json.Unmarshal(family145TransitionBytes, &current); err != nil {
		t.Fatal(err)
	}
	prior, err := json.Marshal(restoreSourceForkHistory(t, current, false))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(prior, &rows); err != nil {
		t.Fatal(err)
	}
	callers, calls := 0, 0
	for _, row := range rows {
		if family145DeploymentReadCuts[row.Function] != nil || !strings.HasPrefix(row.File, "internal/runtime/conformance/") {
			continue
		}
		normalized, count := row.After, 0
		for _, helper := range []string{"logSelectedDeploymentForkFailure", "logDeploymentResourceFailure", "waitDeploymentForkRowEvent"} {
			from, to := helper+"(t, f.selected,", helper+"(t, f.db,"
			if helper == "waitDeploymentForkRowEvent" {
				from, to = helper+"(t, f.ctx, f.selected,", helper+"(t, f.ctx, f.db,"
			}
			count += strings.Count(normalized, from)
			normalized = strings.ReplaceAll(normalized, from, to)
		}
		if count == 0 {
			continue
		}
		callers++
		calls += count
		if row.Function == "assertDeploymentResourceRows" {
			if err := family145DeploymentPublicRowProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
		} else if normalized != row.Before {
			t.Fatalf("deployment propagation changed an original fixture, held-child/fault/replay/receipt/identity/public assertion: %s", row.Function)
		}
	}
	if callers != 6 || calls != 7 {
		t.Fatalf("deployment caller inventory=%d/6 calls=%d/7", callers, calls)
	}
}
