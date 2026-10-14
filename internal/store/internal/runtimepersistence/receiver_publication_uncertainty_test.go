package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverPublicationCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			cut := "rollback_before_lost_ack"
			if committed {
				cut = "commit_before_lost_ack"
			}
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, _, connector := newP16RaceStore(t, backend)
				f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, canonicalrouting.ReceiverPublicationUncertaintyDocuments(t), nil, ownStoreTestAgentManager, nil)
				source := semanticview.Wrap(f.bundle)
				runID := correlation.RunIDFromContext(f.ctx)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "work.requested", "uncertain-receiver-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
				request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				request.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
				request.TriggerEvent, request.OccurredAt = event, event.CreatedAt()
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
				if err != nil || len(plan.Children) != 1 {
					t.Fatalf("prepare original receiver: %+v %v", plan, err)
				}
				if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
					t.Fatalf("construct original receiver: %+v %v", result, err)
				}
				descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
				if err != nil {
					t.Fatal(err)
				}
				scope, _ := authoractivity.ScopeFromContext(f.ctx)
				catalog, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(catalog.Release)
				options := bus.EventBusOptions{TemplateInstancePlanner: f.manager}
				publisher := f.newRuntimeEventBus(t, options)
				plans, err := publisher.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(plans) != 1 {
					t.Fatalf("prepare publication: %v", err)
				}
				t.Cleanup(func() {
					if err := publisher.ReleaseEnginePublications(context.WithoutCancel(f.ctx), plans); err != nil {
						t.Error(err)
					}
				})
				command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
				if len(command.Activations) != 0 || len(command.Commit.DeliveryRoutes) != 1 || !command.Commit.DeliveryRoutes[0].Target.EntitylessReceiver() || command.Commit.DeliveryRoutes[0].Initialization.Empty() {
					t.Fatalf("publication lost original actor/construction: %+v", command)
				}
				store := f.store.(interface {
					CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
					LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
					deliverylifecycle.Store
				})
				cause := errors.New("injected receiver publication COMMIT acknowledgment unavailable")
				var commits atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					commits.Add(1)
					if committed {
						return errors.Join(cause, tx.Commit())
					}
					return errors.Join(cause, tx.Rollback())
				})
				result, err := store.CommitPublication(f.ctx, command)
				if !errors.Is(err, cause) || result.Acknowledged || len(result.DeliveryHandoffs) != 0 || commits.Load() != 1 {
					t.Fatalf("uncertain commit invented acknowledgment or retried: %+v commits=%d err=%v", result, commits.Load(), err)
				}
				stored, found, err := store.LoadPreparedPublishEvent(f.ctx, event.ID())
				if err != nil || found != committed {
					t.Fatalf("uncertain publication presence: found=%t committed=%t err=%v", found, committed, err)
				}
				if committed && (stored.Validate() != nil || !reflect.DeepEqual(stored.DeliveryRoutes, command.Commit.DeliveryRoutes)) {
					t.Fatal("committed actor/dependency receipt changed despite missing acknowledgment")
				}
				resolved, err := store.CommitPublication(f.ctx, command)
				want := bus.EventAppendInserted
				if committed {
					want = bus.EventAppendExactDuplicate
				}
				wantHandoffs := 1
				if committed {
					wantHandoffs = 0
				}
				if err != nil || !resolved.Acknowledged || resolved.AppendOutcome != want || len(resolved.DeliveryHandoffs) != wantHandoffs {
					t.Fatalf("exact uncertainty resolution: %+v %v", resolved, err)
				}
				id := mustReceiverDeliveryID(t, event.ID(), command.Commit.DeliveryRoutes[0])
				before, err := store.Snapshot(f.ctx, id)
				if err != nil || before.ClaimVersion != 0 || before.Status != deliverylifecycle.StatusPending {
					t.Fatalf("uncertainty launched an unready receiver: %+v %v", before, err)
				}
				if err := publisher.ReleaseEnginePublications(f.ctx, plans); err != nil {
					t.Fatal(err)
				}
				fresh := f.newRuntimeEventBus(t, options)
				recovered, err := fresh.PrepareEnginePublications(f.ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(recovered) != 1 {
					t.Fatalf("restore uncertain publication: %v", err)
				}
				t.Cleanup(func() {
					if err := fresh.ReleaseEnginePublications(context.WithoutCancel(f.ctx), recovered); err != nil {
						t.Error(err)
					}
				})
				if routes := recovered[0].(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes; !reflect.DeepEqual(routes, []events.DeliveryRoute{before.Route}) {
					t.Fatalf("restoration reselected actor/dependency ownership: %+v", routes)
				}
				for _, omission := range []bool{true, false} {
					hostile := command
					hostile.Commit.DeliveryRoutes = []events.DeliveryRoute{before.Route}
					if omission {
						hostile.Commit.DeliveryRoutes[0].Initialization = events.ReceiverInitialization{}
					} else {
						original := before.Route.Initialization
						replacement, err := events.AdmitStoredFlowReceiverInitialization(runID, uuid.NewString(), original.ConstructionTarget(), original.ReceiverRoute())
						if err != nil {
							t.Fatal(err)
						}
						hostile.Commit.DeliveryRoutes[0].Initialization = replacement
					}
					// Exact publication replay owns its already-stored routes. A caller
					// cannot acquire a new handoff by supplying different route facts.
					if changed, err := store.CommitPublication(f.ctx, hostile); err != nil || !changed.Acknowledged || changed.AppendOutcome != bus.EventAppendExactDuplicate || len(changed.DeliveryHandoffs) != 0 {
						t.Fatalf("duplicate invented new route authority: omission=%t result=%+v err=%v", omission, changed, err)
					}
					refused, err := store.ClaimDelivery(f.ctx, before.Authority, command.Commit.Event.Event(), hostile.Commit.DeliveryRoutes[0])
					if err != nil || refused.Disposition != deliverylifecycle.ClaimAbsent {
						t.Fatalf("hostile receipt acquired execution: omission=%t claim=%+v err=%v", omission, refused, err)
					}
					if after, err := store.Snapshot(f.ctx, id); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("hostile replay changed original claim/route: omission=%t after=%+v err=%v", omission, after, err)
					}
				}
			})
		}
	}
}
