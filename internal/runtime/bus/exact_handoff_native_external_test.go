package bus_test

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"testing"
	"time"
)

type exactHandoffNativeOwner interface {
	deliverylifecycle.Store
	selectedtest.SelectedDeliveryFixtureStore
	sessions.Registry
	storetest.AgentFixtureStore
}

func openExactHandoffNative(t *testing.T, backend string, selectedExecution bool) runtimebus.ExactHandoffNativeFixtureForTest {
	t.Helper()
	var owner exactHandoffNativeOwner
	if backend == "postgres" {
		owner = storetest.StartPostgresRuntimeStore(t)
	} else {
		owner = storetest.StartSQLiteRuntimeStore(t)
	}
	ctx := effects.WithDifferentOwner(context.Background(), effects.OwnerBuildTestInfrastructure)
	ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
	runtimeID := uuid.NewString()
	ctx = correlation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, sourceartifactfixture.BundleHash))
	if err := sourceartifactfixture.Ensure(ctx, owner); err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), "exact-handoff-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	fixture := runtimebus.ExactHandoffNativeFixtureForTest{Store: owner, Context: ctx, Authority: authority}
	if selectedExecution {
		execution := selectedtest.OpenSelectedDeliveryExecution(t, ctx, owner)
		ctx, authority = execution.Context, execution.Authority
		fixture.Context, fixture.Authority = ctx, authority
		fixture.SelectedEvent, fixture.SelectedRoute = execution.SelectedEvent, execution.SelectedRoute
	} else if err := owner.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	var prepared agentidentity.Identity
	fixture.Seed = func(t *testing.T, eventID, runID string, route events.DeliveryRoute) (events.Event, deliverylifecycle.ExecutionAuthority) {
		t.Helper()
		var event events.Event
		if selectedExecution {
			if eventID != fixture.SelectedEvent.ID() || runID != fixture.SelectedEvent.RunID() {
				t.Fatal("selected handoff fixture requires its genuinely issued exact event")
			}
			event = fixture.SelectedEvent
		} else {
			storetest.RequireRunningRun(t, ctx, owner, runID, time.Now().UTC())
			if prepared == (agentidentity.Identity{}) {
				prepared = route.AgentIdentity
				config := busExecutionFixtureConfig(t, actors.AgentConfig{
					ID: prepared.AgentID(), Identity: prepared, Type: "stub", Role: "worker", Model: "regular", ExecutionMode: "live", Config: []byte(`{}`),
				})
				if err := storetest.UpsertStaticAgentFixtureForSource(t, ctx, owner, manager.PersistedAgent{
					Config: config, Status: "active", HiredBy: "exact-handoff-native-proof", StartedAt: time.Now().UTC(),
				}, sourceartifactfixture.Fact()); err != nil {
					t.Fatal(err)
				}
				authority, err = agentfixture.AdmitExecutionAuthority(t, ctx, owner, sourceartifactfixture.Fact())
				if err != nil {
					t.Fatal(err)
				}
			} else if prepared != route.AgentIdentity {
				t.Fatal("exact handoff fixture cannot extend its admitted recipient set")
			}
			event = eventtest.RuntimeControl(eventID, "test.work", "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
		}
		storetest.CommitNativeDeliveryPublication(t, ctx, owner, storetest.AdmitNativeDeliveryEvent(t, event), []events.DeliveryRoute{route}, authority, nil)
		return storetest.LoadCanonicalEventRecord(t, ctx, owner, eventID), authority
	}
	fixture.Load = func(t *testing.T, eventID string) events.Event {
		t.Helper()
		return storetest.LoadCanonicalEventRecord(t, ctx, owner, eventID)
	}
	fixture.Session = func(t *testing.T, identity agentidentity.Identity) string {
		t.Helper()
		if prepared != identity {
			t.Fatal("exact handoff session requires its already prepared recipient")
		}
		lease, err := owner.Acquire(ctx, identity, "exact-handoff-session")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			result, err := owner.ReleaseOutcome(context.WithoutCancel(ctx), lease)
			if err != nil || !result.Acknowledged {
				t.Errorf("release exact handoff session: %+v %v", result, err)
			}
		})
		return lease.SessionID
	}
	return fixture
}

func TestSelectedDeliveryTransfersAcceptCommittedIsAtomic(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifySelectedDeliveryTransfersAcceptCommittedIsAtomicForTest(t, func(t *testing.T, selected bool) runtimebus.ExactHandoffNativeFixtureForTest {
				return openExactHandoffNative(t, backend, selected)
			})
		})
	}
}

func TestEventBusSnapshottedAgentRouteSendLinearizesWithRemoval(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifyEventBusSnapshottedAgentRouteSendLinearizesWithRemovalForTest(t, func(t *testing.T, selected bool) runtimebus.ExactHandoffNativeFixtureForTest {
				return openExactHandoffNative(t, backend, selected)
			})
		})
	}
}

func TestCommittedFinalizersScopeIngressAdmissionWithoutLosingRuntimeAuthority(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifyCommittedFinalizersScopeIngressAdmissionWithoutLosingRuntimeAuthorityForTest(t, func(t *testing.T, selected bool) runtimebus.ExactHandoffNativeFixtureForTest {
				return openExactHandoffNative(t, backend, selected)
			})
		})
	}
}

func TestDeliverySessionBindingRejectsForeignSourceWithExactClaimBeforeStoreMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifyDeliverySessionBindingRejectsForeignSourceWithExactClaimBeforeStoreMutationForTest(t, func(t *testing.T, selected bool) runtimebus.ExactHandoffNativeFixtureForTest {
				return openExactHandoffNative(t, backend, selected)
			})
		})
	}
}

func TestEventBusResetPreservesPendingOperationUntilPriorRetirementSucceeds(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimebus.VerifyEventBusResetPreservesPendingOperationUntilPriorRetirementSucceedsForTest(t, func(t *testing.T, selected bool) runtimebus.ExactHandoffNativeFixtureForTest {
				return openExactHandoffNative(t, backend, selected)
			})
		})
	}
}

func TestSubscribedRoutePreparationNativeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			verifyNativeObserverCreationOrder(t, backend, false)
		})
	}
}
