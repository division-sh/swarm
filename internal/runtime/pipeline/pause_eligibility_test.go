package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The real engine receives a handed delivery while paused, then the same route
// must execute exactly once after the existing continue owner releases it.
func TestPausedHandedNodeParksUntilContinueBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, s, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopySelectionRetry(t), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			var nodes []pipeline.WorkflowNode
			for _, row := range []struct{ id, event string }{{"seed", "seed"}, {"select", "select"}, {"final", "selected"}} {
				nodes = append(nodes, pipeline.WorkflowNode{Node: externalPipelineSourceNode(t, source, ".", row.id), Subscriptions: []events.EventType{events.EventType(row.event)}, ExecutionType: contracts.SystemNodeExecutionType})
			}
			bus, err := newScopedTestEventBus(t, s.events, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			coordinator := newGateRecoveryCoordinator(bus, s, pipeline.PipelineCoordinatorOptions{Module: proposedEffectProofModule{source: source, nodes: nodes}})
			commitKeylessConstructorComponent(t, ctx, s, coordinator, source)
			bus.SetInterceptors(coordinator)
			publish := func(name string) events.Event {
				t.Helper()
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				wait, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := bus.WaitForQuiescence(wait); err != nil {
					t.Fatal(err)
				}
				return event
			}
			publish("seed")
			controller := runcontrol.NewController(s.events.(runcontrol.Store), bus, runcontrol.Options{})
			bus.SetRunDispatchGate(controller)
			if _, err := controller.Pause(ctx, runcontrol.TransitionRequest{RunID: runID, Reason: "audit"}); err != nil {
				t.Fatal(err)
			}
			event := publish("select")
			var before int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='selected'`, runID).Scan(&before); err != nil || before != 0 {
				t.Fatalf("paused publication executed: %d %v", before, err)
			}
			owner := s.events.PipelineObligations()
			work, err := owner.ClaimEvent(ctx, event.ID(), pipelineobligation.PurposePublication)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.Settle(ctx, work.Claim, pipelineobligation.Acknowledged("audit_handed")); err != nil {
				t.Fatal(err)
			}
			prepared, found, err := s.events.LoadPreparedPublishEvent(ctx, event.ID())
			if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
				t.Fatalf("route: %+v %v", prepared, err)
			}
			proof, err := s.events.ProveHandoff(ctx, event.ID(), prepared.DeliveryRoutes[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := bus.ReleaseDeliveryContinuation(proof.DeliveryID()); err != nil {
				t.Fatal(err)
			}
			if err := bus.AcceptCommittedDeliveryHandoffs([]deliverylifecycle.DurableHandoffProof{proof}); err != nil {
				t.Fatal(err)
			}
			result := bus.DispatchDeliveryContinuation(ctx, prepared.Event.Event(), prepared.DeliveryRoutes[0])
			if err := result.Failure(); err != nil {
				t.Fatal(err)
			}
			var after int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='selected'`, runID).Scan(&after); err != nil || after != 0 {
				t.Fatalf("paused node mutation = %d %v", after, err)
			}
			if blocked, err := controller.QueueableRunDispatchBlocked(ctx, runID); err != nil || !blocked {
				t.Fatalf("pause = %t %v", blocked, err)
			}
			if _, err := controller.Continue(ctx, runcontrol.TransitionRequest{RunID: runID}); err != nil {
				t.Fatal(err)
			}
			result = bus.DispatchDeliveryContinuation(ctx, prepared.Event.Event(), prepared.DeliveryRoutes[0])
			if err := result.Failure(); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='selected'`, runID).Scan(&after); err != nil || after != 1 {
				t.Fatalf("released node mutation = %d %v", after, err)
			}
			snapshot, err := s.events.Snapshot(ctx, proof.DeliveryID())
			if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
				t.Fatalf("node settlement = %+v, %v", snapshot, err)
			}
		})
	}
}
