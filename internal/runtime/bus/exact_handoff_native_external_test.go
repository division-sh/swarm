package bus_test

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
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
	sourceartifactfixture.RootedWriter
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
	var authority deliverylifecycle.ExecutionAuthority
	fixture := runtimebus.ExactHandoffNativeFixtureForTest{Store: owner}
	if !selectedExecution {
		ctx = originalEventBusFixtureContext(t, ctx, owner)
	}
	if selectedExecution {
		original, err := agentfixture.ProcessCapability(t, ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		execution := selectedtest.OpenSelectedDeliveryExecution(t, ctx, owner, original)
		ctx, authority = execution.Context, execution.Authority
		fixture.Source = execution.SemanticSource
		fixture.SelectedEvent, fixture.SelectedRoute = execution.SelectedEvent, execution.SelectedRoute
	} else {
		artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
			"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
			"schema.yaml":   []byte("stages: []\n"),
			"events.yaml":   []byte("test.work:\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		rooted, _ := sourceartifactfixture.Rooted(t, "exact-handoff", artifact)
		repo := canonicalrouting.RepoRoot(t)
		bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		fixture.Source = semanticview.Wrap(bundle)
		fact := sourceartifactfixture.FactFor(bundle.SourceArtifact)
		ctx = correlation.WithSourceArtifactFact(ctx, fact)
		ctx = correlation.WithRootAdmission(ctx, rooted.Admission())
		ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(uuid.NewString(), fact.BundleHash()))
		if _, err := owner.EnsureRootedSource(ctx, rooted); err != nil {
			t.Fatal(err)
		}
		authority, err = deliverylifecycle.NewNormalExecutionAuthority(fact, "exact-handoff-test", 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.ActivateDeliveryAuthority(ctx, authority); err != nil {
			t.Fatal(err)
		}
	}
	fixture.OriginalCommandContext = startupownership.CarryMutationAuthority(context.Background(), ctx)
	fixture.Context, fixture.Authority = ctx, authority
	fixture.Seed = func(t *testing.T, eventID, runID string, route events.DeliveryRoute) events.Event {
		t.Helper()
		var event events.Event
		if selectedExecution {
			if eventID != fixture.SelectedEvent.ID() || runID != fixture.SelectedEvent.RunID() {
				t.Fatal("selected handoff fixture requires its genuinely issued exact event")
			}
			event = fixture.SelectedEvent
		} else {
			storetest.RequireRunningRun(t, ctx, owner, runID, time.Now().UTC())
			event = eventtest.RuntimeControl(eventID, "test.work", "test", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
		}
		storetest.CommitNativeDeliveryPublication(t, ctx, owner, storetest.AdmitNativeDeliveryEvent(t, event), []events.DeliveryRoute{route}, authority, nil)
		return storetest.LoadCanonicalEventRecord(t, ctx, owner, eventID)
	}
	fixture.Load = func(t *testing.T, eventID string) events.Event {
		t.Helper()
		return storetest.LoadCanonicalEventRecord(t, ctx, owner, eventID)
	}
	fixture.Session = func(t *testing.T, identity agentidentity.Identity) string {
		t.Helper()
		if err := storetest.UpsertStaticAgentFixtureForSource(t, ctx, owner, manager.PersistedAgent{
			Config: busTestAgentConfig(t, actors.AgentConfig{ID: identity.AgentID(), Identity: identity, Type: "stub", Role: "worker", Model: "regular", ExecutionMode: "live", ResolvedLLMBackend: "anthropic", Config: []byte(`{}`)}),
			Status: "active", HiredBy: "exact-handoff-native-proof", StartedAt: time.Now().UTC(),
		}, authority.SourceArtifact()); err != nil {
			t.Fatal(err)
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
