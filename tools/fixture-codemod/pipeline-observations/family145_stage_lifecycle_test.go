package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

var family145StageLifecycleCuts = map[string][]string{
	"TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)", "return selected, selected",
		"runtime.Bus.PublishAcknowledged(runCtx, setupEvent)", "requireStageLifecyclePipelineSettlement(t, runtime, selected, activation.RunID)",
		"closeConformanceRuntimeGeneration(runtime, processCapability)", "newStageLifecycleIdentityRuntime(t, selected, module)",
		"assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, setupEventID, activation)",
		"assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, memberBEventID, activation)",
		"assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, memberAEventID, activation)",
		"runtime.Pipeline.CommitDecision(runCtx, card, decisionEventID, decidedAt)", "ApplyDecisionForTest(runCtx,",
		"runtime.Bus.PublishAcknowledged(runCtx, decisionEvent)", "assertStageLifecyclePersistedRoute(t, runCtx, lifecycleStore, flowIdentity)",
	},
	"assertStageLifecycleDeliveryRoute": {
		"selected.ListEventDeliveryRoutes(ctx, eventID)", "runtimedelivery.DeliveryID(eventID, routes[0])",
		"selected.Snapshot(ctx, deliveryID)", "stageLifecycleRuntimeLogs(selected)",
		"time.Now().Add(10 * time.Second)", "time.Sleep(10 * time.Millisecond)",
		"snapshot.Failure.Message + \" \" + snapshot.Failure.Detail.Code", "snapshot.Failure.Detail.Attributes",
	},
	"stageLifecycleRuntimeLogs": {
		"storetest.ReadStageLifecycleRuntimeLogPayloads(context.Background(), selected)",
		"if selected == nil", "return err.Error()", "json.Marshal(logs)", "return string(raw)",
	},
}

func family145StageLifecyclePredecessor(before string) string {
	before = strings.ReplaceAll(before, "assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db,", "assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore,")
	return strings.ReplaceAll(before, "stageLifecycleRuntimeLogs(db)", "stageLifecycleRuntimeLogs(selected)")
}

func family145StageLifecycleProofError(function, before, after string) error {
	if function != "stageLifecycleRuntimeLogs" {
		if err := family145ConnectorProofError(function, family145StageLifecyclePredecessor(before), after); err != nil {
			return err
		}
	}
	for _, cut := range family145StageLifecycleCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("stage lifecycle lost original owner, scope or execution cut: %s", cut)
		}
	}
	if function == "TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends" &&
		(strings.Count(after, "assertStageLifecycleDeliveryRoute(") != 4 || strings.Count(after, "closeConformanceRuntimeGeneration(") != 2 || strings.Count(after, "newStageLifecycleIdentityRuntime(") != 2) {
		return fmt.Errorf("stage lifecycle whole caller/generation inventory changed")
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func TestFamily145StageLifecycleCompleteCallerChainRetainsIdentityAndExecutionAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found int
	for _, row := range rows {
		if _, ok := family145StageLifecycleCuts[row.Function]; !ok {
			continue
		}
		found++
		if err := family145StageLifecycleProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range family145StageLifecycleCuts[row.Function] {
			if family145StageLifecycleProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost stage lifecycle owner/cut accepted: %s", cut)
			}
		}
		assertFamily145StageLifecycleHostileAssertions(t, row.Function, row.Before, row.After)
	}
	if found != 3 {
		t.Fatalf("stage lifecycle source/caller inventory=%d want3", found)
	}
	data, err := os.ReadFile("../../../internal/runtime/conformance/workflow_stage_lifecycle_identity_conformance_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145DescriptorReadinessEscape(data); err != nil {
		t.Fatal(err)
	}
}

func assertFamily145StageLifecycleHostileAssertions(t *testing.T, function, before, after string) {
	t.Helper()
	if function == "stageLifecycleRuntimeLogs" {
		return
	}
	assertions, err := family145DescriptorBusinessAssertions(family145StageLifecyclePredecessor(before))
	if err != nil {
		t.Fatal(err)
	}
	for _, assertion := range assertions {
		hostile, err := family145BusAssertionCounterexample(after, strings.SplitN(assertion, " => ", 2)[0])
		if err != nil {
			t.Fatal(err)
		}
		if family145StageLifecycleProofError(function, before, hostile) == nil {
			t.Fatalf("lost original stage lifecycle assertion accepted: %s", assertion)
		}
	}
}

func TestFamily145StageLifecycleDiagnosticRetainsExactPhysicalQueryAndOwner(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original string
	for _, row := range rows {
		if row.Function == "stageLifecycleRuntimeLogs" {
			original = row.Before
		}
	}
	owner := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadStageLifecycleRuntimeLogPayloadsTx")
	want, got := family145ConnectorSQL(t, "package proof\n"+original), family145ConnectorSQL(t, "package proof\n"+owner)
	if len(want) != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("physical diagnostic name/scope/text/order/limit changed: got=%v want=%v", got, want)
	}
	for _, cut := range []string{"var payload string", "rows.Scan(&payload)", "out = append(out, payload)", "return out, rows.Err()", "errors.Join(err, rows.Close())", "out = nil"} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("physical diagnostic rendering/error settlement changed: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ReadStageLifecycleRuntimeLogPayloadsForTest")
	if !strings.Contains(bridge, "readFanOutSemanticStorage(ctx, selected, nil,") || !strings.Contains(bridge, "eventrecord.ReadStageLifecycleRuntimeLogPayloadsTx(ctx, tx)") {
		t.Fatal("diagnostic bridge lost original selected coordinator or widened its selector")
	}
}
