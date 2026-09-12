package cataloge2e

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestReceiverMaterializationSelectedForkExecutionBothStores(t *testing.T) {
	runReceiverMaterializationSelectedForkExecution(t, false)
}

func TestReceiverMaterializationSelectedForkPublicationFrontierBothStores(t *testing.T) {
	runReceiverMaterializationSelectedForkExecution(t, true)
}

func runReceiverMaterializationSelectedForkExecution(t *testing.T, publicationFrontier bool) {
	t.Helper()
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyReceiverMaterializationWithAgent(t, "renamed-observer")
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			selected := runScopedCatalogStore(t, h)
			if !publicationFrontier {
				if _, err := selected.PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "receiver dependency fork", ControlledBy: "cataloge2e"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "selected"}}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			var inputID string
			inputName := "start.seeded"
			if publicationFrontier {
				inputName = "receiver.seeded"
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, catalogRuntimeRunID, inputName).Scan(&inputID); err != nil {
				t.Fatal(err)
			}
			var sourceExecutionsBefore int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, catalogRuntimeRunID).Scan(&sourceExecutionsBefore); err != nil {
				t.Fatal(err)
			}
			var sourceStore interface {
				storetest.DurableDataCatalogStore
				forkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			var store interface {
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			var lifecycle forkexecution.SelectedContractForkLifecycle = h.pg
			if h.sqlite != nil {
				sourceStore, store, lifecycle = h.sqlite, h.sqlite, h.sqlite
			}
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: inputID, AllowSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h, lifecycle), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
			})
			if err != nil {
				logSelectedForkRecoveryFailure(t, ctx, h, result.Materialization.ForkRunID, err)
				t.Fatalf("real selected dependency execution: %+v %v", result, err)
			}
			child := result.Materialization.ForkRunID
			if child == "" || child == catalogRuntimeRunID {
				t.Fatal("missing distinct selected child")
			}
			var eventID string
			deadline := time.Now().Add(20 * time.Second)
			for {
				err = h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='receiver.seeded'`, child).Scan(&eventID)
				if err == nil || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("selected downstream publication: %v", err)
			}
			publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
			if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
				t.Fatalf("selected receiver obligations: %+v %v", publication, err)
			}
			var node, agent deliverylifecycle.Snapshot
			for _, route := range publication.DeliveryRoutes {
				id, err := deliverylifecycle.DeliveryID(eventID, route)
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(20 * time.Second)
				var snapshot deliverylifecycle.Snapshot
				for {
					snapshot, err = store.Snapshot(ctx, id)
					if err != nil || snapshot.Status == deliverylifecycle.StatusDelivered || snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
					t.Fatalf("selected final dependency: %+v %v", snapshot, err)
				}
				if route.Recipient.IsNode() {
					node = snapshot
				} else {
					agent = snapshot
				}
			}
			if agent.Route.Materialization.Empty() || agent.Route.Materialization.RunID() != child || agent.Route.Materialization.Materializer() != node.RouteIdentity || agent.StartedAt.Before(node.SettledAt) || agent.Route.AgentIdentity.RunID != child || !agent.Authority.Equal(node.Authority) {
				t.Fatalf("selected ownership relation changed: node=%+v agent=%+v", node, agent)
			}
			var providerCalls int
			h.llm.mu.Lock()
			calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
			h.llm.mu.Unlock()
			for _, call := range calls {
				if call.RunID == child && call.EventID == eventID && call.AgentID == agent.SubscriberID && call.TargetEntityID == node.Route.Target.Route().EntityID {
					providerCalls++
				}
			}
			if providerCalls != 1 || node.ClaimVersion != 1 || agent.ClaimVersion != 1 {
				t.Fatalf("selected exact provider execution count=%d node=%+v agent=%+v calls=%+v", providerCalls, node, agent, calls)
			}
			var sourceExecutions int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, catalogRuntimeRunID).Scan(&sourceExecutions); err != nil || sourceExecutions != sourceExecutionsBefore {
				t.Fatalf("fork executed source: before=%d after=%d err=%v", sourceExecutionsBefore, sourceExecutions, err)
			}
		})
	}
}
