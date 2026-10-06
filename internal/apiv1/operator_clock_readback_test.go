package apiv1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type clockReadbackStore interface {
	RunReadStore
	storetest.RunFixtureStore
	AdmitGenericScheduleOutcome(context.Context, genericschedule.AdmissionCommand) (genericschedule.AdmissionCommit, error)
	CancelGenericScheduleOutcome(context.Context, genericschedule.CancelCommand) (genericschedule.CancelCommit, error)
}

func TestClockScheduleHTTPReadbackOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected clockReadbackStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t)
			}
			artifact := sourceartifactfixture.New("schema.yaml", []byte("name: clock-readback\n"))
			ctx := testAuthorActivityContextForSource(context.Background(), sourceartifactfixture.FactFor(artifact))
			runID := uuid.NewString()
			fixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: artifact, StartedAt: time.Now().UTC()}
			storetest.RequireRun(t, ctx, selected, fixture)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			committed, err := selected.AdmitGenericScheduleOutcome(ctx, genericschedule.AdmissionCommand{
				ScheduleKey: "morning", RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance, OwnerID: ".",
				EventType: "report.requested", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
				ExecutionMode: executionmode.Live, Due: genericschedule.CronDue("0 9 * * *"),
			})
			if err != nil || !committed.Acknowledged {
				t.Fatalf("clock admission: %#v, %v", committed, err)
			}
			activation := committed.Result.Activation
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Runs: selected})})
			for _, status := range []string{"active", "cancelled"} {
				for _, method := range []string{"run.get", "run.diagnose"} {
					response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"clock","method":%q,"params":{"run_id":%q}}`, method, runID))
					if response.Error != nil {
						t.Fatalf("%s error: %#v", method, response.Error)
					}
					run := asMap(t, asMap(t, response.Result)["run"])
					clocks, ok := run["clock_schedules"].([]any)
					if !ok || len(clocks) != 1 {
						t.Fatalf("%s lost durable clock inventory: %#v", method, run)
					}
					clock := asMap(t, clocks[0])
					if clock["activation_id"] != activation.ID || clock["status"] != status || clock["cron"] != "0 9 * * *" || clock["run_id"] != runID || clock["flow_instance"] != runID || clock["retains_run"] != (status == "active") {
						t.Fatalf("%s changed admitted evidence: %#v", method, clock)
					}
					if status == "active" && clock["next_due_at"] != activation.CurrentDueAt.Format(time.RFC3339Nano) {
						t.Fatalf("%s recomputed next due: %#v", method, clock)
					}
					if status == "cancelled" {
						if _, exists := clock["next_due_at"]; exists || clock["cancel_cause"] != "clock_removed" {
							t.Fatalf("%s invented next occurrence after cancellation: %#v", method, clock)
						}
					}
				}
				if status == "active" {
					cancelled, err := selected.CancelGenericScheduleOutcome(ctx, genericschedule.CancelCommand{
						ActivationID: activation.ID, Cause: "clock_removed", CancelledAt: time.Now().UTC(),
					})
					if err != nil || !cancelled.Acknowledged {
						t.Fatalf("clock cancellation: %#v, %v", cancelled, err)
					}
				}
			}
			if err := storetest.CorruptClockImmutableHash(ctx, selected, runID, activation.ID); err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"run.get", "run.diagnose"} {
				response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"corrupt-clock","method":%q,"params":{"run_id":%q}}`, method, runID))
				if response.Error == nil || response.Result != nil {
					t.Fatalf("%s returned partial clock evidence: %#v", method, response)
				}
			}
			physical, err := storetest.ReadClockStorage(ctx, selected, runID, activation.ID)
			if err != nil || physical.Rows != 1 || physical.Status != "cancelled" || physical.ImmutableHash != "corrupt" {
				t.Fatalf("inspection mutated corrupt evidence: physical=%+v err=%v", physical, err)
			}
		})
	}
}
