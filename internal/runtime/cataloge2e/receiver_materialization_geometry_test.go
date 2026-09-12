package cataloge2e

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverMaterializationRootPreflightBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverMaterializationIntoRoot(t), backend, true)
			route := events.RouteIdentity{FlowID: "child", FlowInstance: "child"}
			source, err := events.NewStaticFlowRoutingSource(route)
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "child/child.ready", eventtest.Producer(events.EventProducerNode, "producer"), "", []byte(`{"token":"root-owned"}`), 0,
				events.EventLineage{RunID: catalogRuntimeRunID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
				events.EnvelopeForSourceRoute(events.EventEnvelope{}, route), source, time.Now().UTC())
			plan, err := h.rt.Bus.CheckPublishRecipientPlan(catalogRunContext(h, catalogRuntimeRunID), event)
			if err != nil {
				t.Fatal(err)
			}
			if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 2 {
				t.Fatalf("root preflight: %+v", plan)
			}
			for _, delivery := range plan.DeliveryRoutes {
				if !delivery.Target.MaterializingEntity() || delivery.Target.Route() != (events.RouteIdentity{FlowID: ".", FlowInstance: catalogRuntimeRunID, EntityID: catalogRuntimeRunID}) {
					t.Fatalf("root preflight inherited source or lost future ownership: %+v", delivery)
				}
			}
			if err := events.ValidateReceiverMaterializations(event, plan.DeliveryRoutes); err != nil {
				t.Fatal(err)
			}
			foreign := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), event.Type(), event.Producer(), "", event.Payload(), 0,
				events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live},
				event.NormalizedEnvelope(), source, time.Now().UTC())
			// Preflight installs the event's admitted run, rather than borrowing the
			// caller's correlation scope as receiver ownership.
			other, err := h.rt.Bus.CheckPublishRecipientPlan(catalogRunContext(h, catalogRuntimeRunID), foreign)
			if err != nil || other.TargetFailure != "" || len(other.DeliveryRoutes) != 2 {
				t.Fatalf("second run preflight: %+v %v", other, err)
			}
			for _, delivery := range other.DeliveryRoutes {
				if delivery.Target.Route().FlowInstance != foreign.RunID() || delivery.Target.Route().EntityID != foreign.RunID() {
					t.Fatalf("preflight reused another run's root: %+v", delivery)
				}
			}
			var count int
			if err := h.db.QueryRowContext(h.ctx, `SELECT COUNT(*) FROM events WHERE event_id IN ($1,$2)`, event.ID(), foreign.ID()).Scan(&count); err != nil || count != 0 {
				t.Fatalf("preflight persisted event: count=%d err=%v", count, err)
			}
		})
	}
}

func TestReceiverMaterializationDuplicateNamesAcrossSiblingAndNestedScopesBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, geometry := range []struct {
			name, seed, event, prefix string
			nested                    bool
		}{
			{"siblings", "start.seeded", "receiver.seeded", "", false},
			{"nested", "outer.seeded", "branch/receiver.seeded", "branch/", true},
		} {
			t.Run(string(backend)+"/"+geometry.name, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverMaterializationGeometry(t, geometry.nested), backend, true)
				if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: geometry.seed, Payload: map[string]any{"token": "scoped"}}, 20*time.Second, true); err != nil {
					t.Fatal(err)
				}
				ctx := catalogRunContext(h, catalogRuntimeRunID)
				var eventID string
				if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, catalogRuntimeRunID, geometry.event).Scan(&eventID); err != nil {
					t.Fatal(err)
				}
				var store interface {
					bus.PreparedPublishEventReader
					deliverylifecycle.Store
				} = h.pg
				if h.sqlite != nil {
					store = h.sqlite
				}
				publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
				if err != nil || !found || len(publication.DeliveryRoutes) != 4 {
					t.Fatalf("exact sibling publication: %+v %v", publication, err)
				}
				snapshots := map[events.DeliveryRouteIdentity]deliverylifecycle.Snapshot{}
				for _, route := range publication.DeliveryRoutes {
					id, err := deliverylifecycle.DeliveryID(eventID, route)
					if err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(15 * time.Second)
					var snapshot deliverylifecycle.Snapshot
					for {
						snapshot, err = store.Snapshot(ctx, id)
						if err != nil || snapshot.Status == deliverylifecycle.StatusDelivered || snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered || snapshot.ClaimVersion != 1 {
						t.Fatalf("exact scoped execution: %+v %v", snapshot, err)
					}
					snapshots[snapshot.RouteIdentity] = snapshot
				}
				owners := map[string]string{}
				for _, snapshot := range snapshots {
					if !snapshot.Route.Recipient.IsAgent() {
						continue
					}
					dependency := snapshot.Route.Materialization
					node, found := snapshots[dependency.Materializer()]
					if dependency.Empty() || !found || !node.Route.Recipient.IsNode() || !events.SameDeliveryTargetOwnership(node.Route.Target, snapshot.Route.Target) || snapshot.StartedAt.Before(node.SettledAt) {
						t.Fatalf("crossed sibling materializer: node=%+v agent=%+v", node, snapshot)
					}
					target := snapshot.Route.Target.Route()
					if target.FlowID != geometry.prefix+"consumer" && target.FlowID != geometry.prefix+"sibling" {
						t.Fatalf("foreign target: %+v", target)
					}
					if _, exists := owners[target.FlowID]; exists {
						t.Fatal("duplicate agent replaced another scoped owner")
					}
					owners[target.FlowID] = target.EntityID
					var turns int
					h.llm.mu.Lock()
					for _, call := range h.llm.deliveryCalls {
						if call.RunID == catalogRuntimeRunID && call.EventID == eventID && call.TargetEntityID == target.EntityID {
							turns++
						}
					}
					h.llm.mu.Unlock()
					if turns != 1 {
						t.Fatalf("provider execution for %s=%d", target.FlowID, turns)
					}
				}
				if len(owners) != 2 || owners[geometry.prefix+"consumer"] == owners[geometry.prefix+"sibling"] {
					t.Fatalf("receivers share mutable ownership: %+v", owners)
				}
			})
		}
	}
}

func TestReceiverMaterializationSourceLocalTargetlessAgentControlBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverMaterializationSourceLocalObserver(t), backend, true)
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "start.seeded", Payload: map[string]any{"token": "local"}}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			var id string
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='start.seeded'`, catalogRuntimeRunID).Scan(&id); err != nil {
				t.Fatal(err)
			}
			var store interface {
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			if h.sqlite != nil {
				store = h.sqlite
			}
			publication, found, err := store.LoadPreparedPublishEvent(ctx, id)
			if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
				t.Fatalf("source-local publication: %+v %v", publication, err)
			}
			var observers int
			for _, route := range publication.DeliveryRoutes {
				if !route.Recipient.IsAgent() {
					continue
				}
				observers++
				if !route.Target.Empty() || !route.Materialization.Empty() || route.AgentIdentity.RunID != catalogRuntimeRunID {
					t.Fatalf("source-local observer invented receiving state: %+v", route)
				}
				deliveryID, err := deliverylifecycle.DeliveryID(id, route)
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(15 * time.Second)
				for {
					snapshot, err := store.Snapshot(ctx, deliveryID)
					if err != nil {
						t.Fatal(err)
					}
					if snapshot.Status == deliverylifecycle.StatusDelivered {
						break
					}
					if snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
						t.Fatalf("source-local observer failed: %+v", snapshot)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			if observers != 1 {
				t.Fatal("source-local declared observer omitted")
			}
		})
	}
}

func TestReceiverMaterializationChildToRootBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverMaterializationIntoRoot(t), backend, true)
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "child/work.requested", Payload: map[string]any{"token": "root-owned"}}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			var eventID string
			deadline := time.Now().Add(10 * time.Second)
			for {
				err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='child/child.ready'`, catalogRuntimeRunID).Scan(&eventID)
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					deliveryRows, queryErr := h.db.QueryContext(ctx, `SELECT subscriber_type, subscriber_id, status, COALESCE(CAST(failure AS TEXT), '') FROM event_deliveries WHERE run_id=$1`, catalogRuntimeRunID)
					if queryErr == nil {
						for deliveryRows.Next() {
							var kind, recipient, status, reason string
							_ = deliveryRows.Scan(&kind, &recipient, &status, &reason)
							t.Log(kind, recipient, status, reason)
						}
						deliveryRows.Close()
					}
					rows, queryErr := h.db.QueryContext(ctx, `SELECT event_name, payload FROM events WHERE run_id=$1 ORDER BY created_at`, catalogRuntimeRunID)
					if queryErr == nil {
						defer rows.Close()
						for rows.Next() {
							var name, payload string
							_ = rows.Scan(&name, &payload)
							t.Log(name, payload)
						}
					}
					t.Fatal(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			var store interface {
				bus.PreparedPublishEventReader
				deliverylifecycle.Store
			} = h.pg
			if h.sqlite != nil {
				store = h.sqlite
			}
			publication, found, err := store.LoadPreparedPublishEvent(ctx, eventID)
			if err != nil || !found || len(publication.DeliveryRoutes) != 2 {
				t.Fatalf("root receiver publication: %+v %v", publication, err)
			}
			var node, agent deliverylifecycle.Snapshot
			for _, route := range publication.DeliveryRoutes {
				id, err := deliverylifecycle.DeliveryID(eventID, route)
				if err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(15 * time.Second)
				var snapshot deliverylifecycle.Snapshot
				for {
					snapshot, err = store.Snapshot(ctx, id)
					if err != nil || snapshot.Status == deliverylifecycle.StatusDelivered || snapshot.Status == deliverylifecycle.StatusDeadLetter || time.Now().After(deadline) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
					t.Fatalf("root receiver execution: %+v failure=%+v %v", snapshot, snapshot.Failure, err)
				}
				if route.Recipient.IsNode() {
					node = snapshot
				} else {
					agent = snapshot
				}
			}
			if agent.Route.Materialization.Empty() || agent.Route.Materialization.Materializer() != node.RouteIdentity || agent.Route.Target.Route().FlowInstance != catalogRuntimeRunID || agent.Route.Target.Route().EntityID != catalogRuntimeRunID || agent.StartedAt.Before(node.SettledAt) {
				t.Fatalf("root dependency lost ownership: node=%+v agent=%+v", node, agent)
			}
		})
	}
}
