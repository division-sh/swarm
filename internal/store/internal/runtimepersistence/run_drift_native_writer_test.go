package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestVerifyRunNativeConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, record := constructWorkflowMutationFixture(t, backend, "drift-proof", time.Now().UTC().Truncate(time.Microsecond))
			reader := fixture.store.(interface {
				InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
			})
			runID := correlation.RunIDFromContext(fixture.ctx)
			report, err := reader.InspectRunMutationDrift(fixture.ctx, runID)
			if err != nil || report.EntitiesChecked != 2 || len(report.Rows) != 0 {
				t.Fatalf("native construction disagrees with physical history: run=%s report=%+v err=%v", runID, report, err)
			}
			history, err := ObserveEntityMutationHistoryForTest(fixture.ctx, fixture.store, runID)
			if err != nil || len(history) == 0 {
				t.Fatalf("read actual construction history: rows=%d err=%v", len(history), err)
			}
			var constructionBookkeeping map[string]any
			if err := json.Unmarshal(record.Bookkeeping, &constructionBookkeeping); err != nil {
				t.Fatal(err)
			}
			node := mustPersistenceNode("drift-proof", "advance")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "drift-proof", FlowInstance: "drift-proof/receiver", EntityID: record.EntityID})}
			selected := fixture.store.(stateOnlyAcquisitionStore)
			// Repeated native writes retain one timestamp; committed revisions,
			// not occurrence time or UUID order across revisions, distinguish them.
			for i, handled := range []bool{true, false, true} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "drift.advance", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticEventFixtureWithRoutes(fixture.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(fixture.ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
				} else {
					record.ExpectedState = "done"
					record.ExpectedRevision++
				}
				record.Fields = json.RawMessage(fmt.Sprintf(`{"receiver_key":"receiver","account_id":"preserved","handled":%t}`, handled))
				if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(fixture.ctx, pipeline.WorkflowEngineMutationCommand{State: record, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()}}); err != nil {
					t.Fatalf("native state mutation %d: %v", i, err)
				}
				expected := map[[2]string][2]any{
					{"authored_field", "handled"}: {!handled, handled},
				}
				if i == 0 {
					expected[[2]string{"authored_field", "handled"}] = [2]any{nil, handled}
					expected[[2]string{"lifecycle_state", ""}] = [2]any{"active", "done"}
					var bookkeeping map[string]any
					if err := json.Unmarshal(record.Bookkeeping, &bookkeeping); err != nil {
						t.Fatal(err)
					}
					expected[[2]string{"bookkeeping", "stage_entry"}] = [2]any{constructionBookkeeping["stage_entry"], bookkeeping["stage_entry"]}
				}
				current, err := ObserveEntityMutationHistoryForTest(fixture.ctx, fixture.store, runID)
				if err != nil {
					t.Fatal(err)
				}
				assertNativeMutationHistoryDelta(t, history, current, record.EntityID, expected)
				history = current
				report, err = reader.InspectRunMutationDrift(fixture.ctx, runID)
				if err != nil || report.EntitiesChecked != 2 || len(report.Rows) != 0 {
					t.Fatalf("native mutation %d disagrees with physical history: %+v %v", i, report, err)
				}
			}
		})
	}
}

func assertNativeMutationHistoryDelta(t *testing.T, before, after []EntityMutationEvidence, entity string, expected map[[2]string][2]any) {
	t.Helper()
	retained := make(map[string]EntityMutationEvidence, len(before))
	for _, row := range before {
		if _, duplicate := retained[row.MutationID]; duplicate || row.MutationID == "" {
			t.Fatalf("invalid retained mutation identity: %+v", row)
		}
		retained[row.MutationID] = row
	}
	seen := make(map[string]bool, len(after))
	for _, row := range after {
		if seen[row.MutationID] || row.MutationID == "" {
			t.Fatalf("duplicate or absent native mutation identity: %+v", row)
		}
		seen[row.MutationID] = true
		if previous, found := retained[row.MutationID]; found {
			if !reflect.DeepEqual(previous, row) {
				t.Fatalf("native commit rewrote retained history: before=%+v after=%+v", previous, row)
			}
			delete(retained, row.MutationID)
			continue
		}
		key := [2]string{row.Domain, row.Path}
		values, found := expected[key]
		if !found || row.EntityID != entity || row.WriterType != "platform" || row.WriterID != "workflow_engine" || row.HandlerStep != "mutate" {
			t.Fatalf("duplicate, unexpected or wrongly owned native mutation: %+v", row)
		}
		for index, raw := range []json.RawMessage{row.OldValue, row.NewValue} {
			var value any
			if err := json.Unmarshal(raw, &value); err != nil || !reflect.DeepEqual(value, values[index]) {
				t.Fatalf("native mutation %v value %d: got=%s want=%+v err=%v", key, index, raw, values[index], err)
			}
		}
		delete(expected, key)
	}
	if len(retained) != 0 || len(expected) != 0 {
		t.Fatalf("native commit lost immutable history or an exact contribution: retained=%+v missing=%+v", retained, expected)
	}
}
