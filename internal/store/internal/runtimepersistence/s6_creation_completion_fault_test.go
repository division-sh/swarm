package runtimepersistence_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func (f s6CreationActivityFixture) creationRequest() pipeline.DynamicFlowRuntimeCreationOccurrenceRequest {
	return pipeline.DynamicFlowRuntimeCreationOccurrenceRequest{RunID: f.plan.RunID, InstancePath: f.plan.Identity.InstancePath, Plan: f.plan, Attempt: f.attempt, Event: f.event, OccurredAt: time.Now().UTC()}
}

func (f s6CreationActivityFixture) peer(t *testing.T, run, key string) s6CreationActivityFixture {
	t.Helper()
	ctx := correlation.WithRunID(f.ctx, run)
	root := flowidentity.Stored(f.source, ".", run, run, "", "")
	if run != f.plan.RunID {
		storetest.RequireRun(t, ctx, f.store, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: run, BundleHash: f.plan.BundleHash, StartedAt: time.Now().UTC()})
		f.construct(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: f.source, Instance: root, OccurredAt: time.Now().UTC()})
	}
	parent := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.recorded", "operator-api", "", []byte(fmt.Sprintf(`{"worker_id":%q}`, key)), 0, run, events.EventEnvelope{}, time.Now().UTC())
	if err := f.bus.Publish(ctx, parent); err != nil {
		t.Fatal(err)
	}
	waitS6CreationPublication(t, ctx, f.bus)
	child, err := flowidentity.KeyedChild(f.source, root, "worker-flow", key)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := f.source.FlowScopeByID("worker-flow")
	activation, attempt := f.construct(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: f.source, Instance: child, ConstructorInput: scope.AutoEmitEvent, ResolvedKey: key, TriggerEvent: parent, OccurredAt: time.Now().UTC()})
	f.ctx, f.plan, f.attempt, f.parent = ctx, activation.Readiness, attempt, parent
	f.event = newS6CreationActivityEvent(t, f.source, f.plan)
	return f
}

func TestS6CreationCompletionFaultRollsBackNativeOccurrenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, frontier := range []string{"node", "own_schema"} {
			t.Run(backend+"/"+frontier, func(t *testing.T) {
				f := newS6CreationActivityFixture(t, backend, frontier)
				catalog := f.store.(interface {
					ResolveAuthorActivityEventDescriptor(authoractivity.Scope, string) (authoractivity.EventDescriptor, bool)
					AuthorActivityEventCatalogRegistered(authoractivity.Scope) bool
				})
				scope, hasScope := authoractivity.ScopeFromContext(f.ctx)
				declaration, err := pinrouting.PublicationDeclarationForSourceEvent(f.event.Type(), f.event.RoutingSource())
				if err != nil {
					t.Fatal(err)
				}
				declared := semanticview.ResolveFlowEventProof(f.source, declaration.Flow(), declaration.Local())
				descriptor, registered := catalog.ResolveAuthorActivityEventDescriptor(scope, declared.EventKey())
				proof := semanticview.ResolveFlowEventProof(f.source, f.plan.Identity.TemplateID, string(f.event.Type()))
				if !hasScope || !catalog.AuthorActivityEventCatalogRegistered(scope) || !registered || descriptor.EventType != declared.EventKey() || descriptor.Disposition != authoractivity.StoryAuthored || !proof.IsAuthored(f.source) || !declared.IsAuthored(f.source) {
					t.Fatalf("completion cut did not execute the live authored draft path: scope=%+v descriptor=%+v proof=%+v", scope, descriptor, proof)
				}
				remove, err := storetest.InstallDynamicFlowCreationCompletionFailure(f.ctx, f.store, f.creationRequest())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := remove(f.ctx); err != nil {
						t.Error(err)
					}
				})
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.commit(f.event); err == nil || !strings.Contains(err.Error(), "s6_creation_completion_after_native_append") {
					t.Fatalf("fault did not witness native event/intended delivery after authored draft admission: %v", err)
				}
				waitS6CreationPublication(t, f.ctx, f.bus)
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("completion rejection changed full application rows: err=%v before=%+v after=%+v", err, before, after)
				}
				t.Log("completion mark rejected after native event/delivery append and authored draft admission; activity finalization/SQL INSERT unreached; no native rows, occurrence or sequence escaped")
				if err := remove(f.ctx); err != nil {
					t.Fatal(err)
				}
				if err := f.commit(f.event); err != nil {
					t.Fatal(err)
				}
				waitS6CreationPublication(t, f.ctx, f.bus)
				stored, found, err := f.store.LoadPreparedPublishEvent(f.ctx, f.event.ID())
				if err != nil || !found || len(stored.DeliveryRoutes) != 1 {
					t.Fatalf("removal did not restore real publication/delivery: found=%t err=%v routes=%+v", found, err, stored.DeliveryRoutes)
				}
				ready, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, f.plan.RunID, f.plan.Identity.Route())
				if err != nil || !found || ready.CreationEventEmittedAt.IsZero() {
					t.Fatalf("removal did not restore creation completion: %+v found=%t err=%v", ready, found, err)
				}
				activity := f.activity(t, "")
				count := 0
				for _, occurrence := range activity {
					if occurrence.SourceIdentity == f.event.ID() {
						count++
						if occurrence.Kind != "event.emitted" || occurrence.Sequence <= 0 || occurrence.FlowID != f.plan.Identity.InstancePath {
							t.Fatalf("restored creation has no exact authored child occurrence: %+v", occurrence)
						}
					}
				}
				if count != 1 {
					t.Fatalf("restored creation occurrence count=%d, want1", count)
				}
				before, err = storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.commit(f.event); err != nil {
					t.Fatal(err)
				}
				waitS6CreationPublication(t, f.ctx, f.bus)
				after, err = storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("exact creation duplicate changed application rows: %v", err)
				}
			})
		}
	}
}

func TestS6CreationCompletionFaultPreservesOtherScopesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newS6CreationActivityFixture(t, backend, "node")
			remove, err := storetest.InstallDynamicFlowCreationCompletionFailure(f.ctx, f.store, f.creationRequest())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := remove(f.ctx); err != nil {
					t.Error(err)
				}
			})
			for _, peer := range []s6CreationActivityFixture{f.peer(t, f.plan.RunID, "worker-002"), f.peer(t, uuid.NewString(), "worker-003")} {
				if err := peer.commit(peer.event); err != nil {
					t.Fatalf("exact fault leaked to sibling/foreign run: %v", err)
				}
				waitS6CreationPublication(t, peer.ctx, peer.bus)
			}
			if err := f.store.RetireDynamicFlowRuntimeActivationAttempt(f.ctx, f.attempt); err != nil {
				t.Fatal(err)
			}
			if staleRemove, err := storetest.InstallDynamicFlowCreationCompletionFailure(f.ctx, f.store, f.creationRequest()); err == nil || staleRemove != nil {
				t.Fatalf("retired attempt installed a fault: remove=%v err=%v", staleRemove != nil, err)
			}
			row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, f.plan.RunID, f.plan.Identity.Route())
			if err != nil || !found {
				t.Fatal(err)
			}
			admitted, err := f.store.BeginDynamicFlowRuntimeActivation(f.ctx, pipeline.NewDynamicFlowRuntimeActivationRequest(row.Plan, row.AttemptOrdinal, row.AttemptState, f.attempt.ProcessBinding()))
			if err != nil || !admitted.Acknowledged || admitted.Attempt.ID() == f.attempt.ID() {
				t.Fatalf("real successor admission: %+v err=%v", admitted, err)
			}
			if ready, err := completeFlowAttachmentFixture(f.ctx, f.workflow, admitted.Attempt, time.Now().UTC()); err != nil || !ready.Acknowledged {
				t.Fatalf("real successor attachment: %+v err=%v", ready, err)
			}
			f.attempt = admitted.Attempt
			if err := f.commit(f.event); err != nil {
				t.Fatalf("old fault leaked to replacement attempt: %v", err)
			}
			waitS6CreationPublication(t, f.ctx, f.bus)
			if err := remove(f.ctx); err != nil {
				t.Fatalf("exact installation could not be removed after advancement: %v", err)
			}
		})
	}
}

func TestS6CreationCompletionFaultRefusesInvalidAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newS6CreationActivityFixture(t, backend, "node")
			request := f.creationRequest()
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			foreign := newS6CreationActivityFixture(t, backend, "node")
			var closed interface{ Close() error }
			if backend == "postgres" {
				closed = storetest.StartPostgresRuntimeStore(t)
			} else {
				closed = storetest.StartSQLiteRuntimeStore(t)
			}
			if err := closed.Close(); err != nil {
				t.Fatal(err)
			}
			wrongPlan := request
			creation := *request.Plan.CreationEvent
			creation.CreatedAt = creation.CreatedAt.Add(time.Microsecond)
			wrongPlan.Plan.CreationEvent = &creation
			wrongAttempt := request
			wrongAttempt.Attempt = pipeline.DynamicFlowRuntimeActivationAttempt{}
			for _, test := range []struct {
				name    string
				ctx     context.Context
				store   any
				request pipeline.DynamicFlowRuntimeCreationOccurrenceRequest
			}{
				{"canceled", canceled, f.store, request},
				{"foreign_owner", f.ctx, foreign.store, request},
				{"foreign_request", f.ctx, f.store, foreign.creationRequest()},
				{"wrong_plan", f.ctx, f.store, wrongPlan},
				{"unissued_attempt", f.ctx, f.store, wrongAttempt},
				{"non_owner", f.ctx, struct{}{}, request},
				{"closed_owner", f.ctx, closed, request},
			} {
				t.Run(test.name, func(t *testing.T) {
					remove, err := storetest.InstallDynamicFlowCreationCompletionFailure(test.ctx, test.store, test.request)
					if err == nil || remove != nil {
						t.Fatalf("invalid admission installed a fault: remove=%v err=%v", remove != nil, err)
					}
					after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.store)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("fault admission refusal changed application rows: %v", err)
					}
				})
			}
			if err := f.commit(f.event); err != nil {
				t.Fatalf("refused install left an active rejection: %v", err)
			}
			waitS6CreationPublication(t, f.ctx, f.bus)
			if remove, err := storetest.InstallDynamicFlowCreationCompletionFailure(f.ctx, f.store, request); err == nil || remove != nil {
				t.Fatalf("already-emitted readiness installed a fault: remove=%v err=%v", remove != nil, err)
			}
		})
	}
}

func TestS6CreationCompletionFaultRemovalIsExactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newS6CreationActivityFixture(t, backend, "node")
			sibling := f.peer(t, f.plan.RunID, "worker-002")
			install := func(fixture s6CreationActivityFixture) func(context.Context) error {
				remove, err := storetest.InstallDynamicFlowCreationCompletionFailure(fixture.ctx, fixture.store, fixture.creationRequest())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := remove(fixture.ctx); err != nil {
						t.Error(err)
					}
				})
				return remove
			}
			remove := install(f)
			removeSibling := install(sibling)
			if err := remove(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.commit(f.event); err != nil {
				t.Fatalf("exact removal did not restore its own occurrence: %v", err)
			}
			waitS6CreationPublication(t, f.ctx, f.bus)
			if err := sibling.commit(sibling.event); err == nil || !strings.Contains(err.Error(), "s6_creation_completion_after_native_append") {
				t.Fatalf("removal deleted another installation: %v", err)
			}
			if err := removeSibling(sibling.ctx); err != nil {
				t.Fatal(err)
			}
			if err := sibling.commit(sibling.event); err != nil {
				t.Fatal(err)
			}
			waitS6CreationPublication(t, sibling.ctx, sibling.bus)
		})
	}
}
