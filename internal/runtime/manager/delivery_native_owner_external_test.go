package manager_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type managerNativeDeliveryOwner interface {
	deliverylifecycle.Store
	storetest.RunFixtureStore
	sourceartifactfixture.Writer
	PipelineObligations() pipelineobligation.Store
	CommitPublication(context.Context, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
	RegisterAuthorActivityEventCatalog(authoractivity.Scope, []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error)
	ListAuthorActivity(context.Context, authoractivity.ListOptions) (authoractivity.ListResult, error)
}

func openManagerNativeDelivery(t *testing.T, backend string) *manager.ManagerDeliveryNativeFixture {
	t.Helper()
	var selected managerNativeDeliveryOwner
	switch backend {
	case "sqlite":
		selected = storetest.StartSQLiteRuntimeStore(t)
	case "postgres":
		selected = storetest.StartPostgresRuntimeStore(t)
	default:
		t.Fatalf("unsupported manager delivery fixture backend %q", backend)
	}
	source := sourceartifactfixture.Fact()
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, "manager-delivery-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := manager.TerminalPanicFixtureContext()
	if err := sourceartifactfixture.Ensure(ctx, selected); err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	return &manager.ManagerDeliveryNativeFixture{
		Store: selected, Authority: authority, Context: ctx, NormalAuthority: authority,
		RequireRun: func(t *testing.T, runID string) {
			t.Helper()
			storetest.RequireRunningRun(t, ctx, selected, runID, time.Now().UTC())
		},
		Publish: func(t *testing.T, ctx context.Context, event events.Event, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
			t.Helper()
			publishManagerNativeDelivery(t, ctx, selected, event, routes, authority)
		},
		ClaimHeartbeat: func(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
			return storetest.ClaimManagerHeartbeatProof(ctx, selected, authority, event, route)
		},
		RenewHeartbeat: func(ctx context.Context, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
			return storetest.RenewManagerHeartbeatProof(ctx, selected, claim)
		},
		RetryReady: func(t *testing.T, event events.Event, route events.DeliveryRoute) {
			t.Helper()
			if err := storetest.MakeManagerRetryEligible(ctx, selected, event, route); err != nil {
				t.Fatal(err)
			}
		},
		AgeClaim: func(t *testing.T, claim deliverylifecycle.Claim, at time.Time) {
			t.Helper()
			if err := storetest.AgeManagerClaimStartedAt(ctx, selected, claim, at); err != nil {
				t.Fatal(err)
			}
		},
		Transitions: func(t *testing.T) []string {
			t.Helper()
			scope, ok := authoractivity.ScopeFromContext(ctx)
			if !ok {
				t.Fatal("manager delivery observation requires its exact author scope")
			}
			var transitions []string
			cursor := int64(0)
			for {
				page, err := selected.ListAuthorActivity(ctx, authoractivity.ListOptions{AfterSequence: cursor, Limit: 500, RuntimeInstanceID: scope.RuntimeInstanceID, BundleHashes: []string{source.BundleHash()}})
				if err != nil {
					t.Fatal(err)
				}
				for _, occurrence := range page.Occurrences {
					if occurrence.Kind == authoractivity.KindDeliveryLifecycle {
						transitions = append(transitions, occurrence.Transition)
					}
				}
				if len(page.Occurrences) == 0 {
					return transitions
				}
				if page.NextCursor <= cursor {
					t.Fatal("manager delivery activity cursor did not advance")
				}
				cursor = page.NextCursor
			}
		},
	}
}

func publishManagerNativeDelivery(t *testing.T, ctx context.Context, selected managerNativeDeliveryOwner, event events.Event, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	publishManagerNativeDeliveryWithRoot(t, ctx, selected, event, routes, authority, nil)
}

func publishManagerNativeDeliveryWithRoot(t *testing.T, ctx context.Context, selected managerNativeDeliveryOwner, event events.Event, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, root *runtimebus.FlowInstanceActivationCommand) {
	t.Helper()
	storetest.CommitNativeDeliveryPublication(t, ctx, selected, storetest.AdmitNativeDeliveryEvent(t, event), routes, authority, root)
}

func TestManagerDeliveryNativeOwnerRequiresExplicitPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			source := sourceartifactfixture.Fact()
			ctx := correlation.WithSourceArtifactFact(context.Background(), source)
			ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope("11111111-1111-1111-1111-111111111111", source.BundleHash()))
			runID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "source", "", nil, 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			identity := agentidentitytest.RootRuntimeForRun(t, runID, "agent-a", "manager.delivery_test")
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			if missing, err := fixture.ClaimDelivery(ctx, fixture.Authority, event, route); err != nil || missing.Disposition != deliverylifecycle.ClaimAbsent {
				t.Fatalf("unpublished event was claimable: %+v,error=%v", missing, err)
			}
			fixture.Publish(t, ctx, event, []events.DeliveryRoute{route}, fixture.Authority)
			claimed, err := fixture.ClaimDelivery(ctx, fixture.Authority, event, route)
			if err != nil || !claimed.Acknowledged || claimed.Disposition != deliverylifecycle.ClaimAcquired {
				t.Fatalf("explicit native publication was not claimable: %+v,error=%v", claimed, err)
			}
			acquired, ok := claimed.Acquired()
			if !ok {
				t.Fatal("acquired result lacked exact claim")
			}
			if _, err := fixture.SettleSuccess(ctx, acquired.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
				t.Fatal(err)
			}
			settled, err := fixture.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil || settled.Status != deliverylifecycle.StatusDelivered {
				t.Fatalf("native settlement lost: %+v,error=%v", settled, err)
			}
		})
	}
}

func TestManagerDeliveryStorageOnlySourceRefusesSelectedExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			selected, ok := fixture.Store.(runforkexecution.SourceArtifactSelectedContractSourceStore)
			if !ok {
				t.Fatal("original native delivery owner has no retained selected source role")
			}
			repo := canonicalrouting.RepoRoot(t)
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected}
			_, err := loader.InspectRunForkSelectedContractSourceForRequest(manager.TerminalPanicFixtureContext(), runforkexecution.SelectedContractSourceLoadRequest{
				BundleHash: sourceartifactfixture.BundleHash,
				Selection:  runfork.RunForkContractSelection{Mode: "bundle_hash", BundleHash: sourceartifactfixture.BundleHash},
			})
			if err == nil || !strings.Contains(err.Error(), "BUNDLE_DATA_INTEGRITY_ERROR") || !strings.Contains(err.Error(), "agent intent source is required") {
				t.Fatalf("storage-only artifact must not mint executable selected authority: %v", err)
			}
		})
	}
}

func TestManagerNativeDeliveryAgeFaultRollsBackForeignTokenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			ctx := fixture.Context
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "source", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			identity := agentidentitytest.RootRuntimeForRun(t, event.RunID(), "age-proof", "manager.delivery_test")
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			fixture.Publish(t, ctx, event, []events.DeliveryRoute{route}, fixture.Authority)
			result, err := fixture.ClaimDelivery(ctx, fixture.Authority, event, route)
			claimed, ok := result.Acquired()
			if err != nil || !ok || !result.Acknowledged {
				t.Fatalf("native claim=%+v,error=%v", result, err)
			}
			// The native claim includes an acknowledged immediate renewal; its
			// updated time, not its original start, owns this lease deadline.
			if claimed.Snapshot.ClaimExpiresAt.Sub(claimed.Snapshot.UpdatedAt) != deliverylifecycle.DefaultLeaseTTL {
				t.Fatalf("ordinary production claim changed default lease: %+v", claimed.Snapshot)
			}
			before, err := fixture.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := deliverylifecycle.AdmitPersistedClaim(claimed.Claim.DeliveryID(), claimed.Claim.RunID(), claimed.Claim.RouteIdentity(), uuid.NewString(), claimed.Claim.Version(), claimed.Claim.SubscriberClass(), claimed.Claim.SubscriberID())
			if err != nil {
				t.Fatal(err)
			}
			if err := storetest.AgeManagerClaimStartedAt(ctx, fixture.Store, foreign, time.Now().Add(-15*time.Minute)); err == nil {
				t.Fatal("age fault admitted a foreign open-attempt token")
			}
			after, err := fixture.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failed age cut retained its first write: before=%+v,after=%+v,error=%v", before, after, err)
			}
			at := time.Now().UTC().Add(-15 * time.Minute).Truncate(time.Microsecond)
			fixture.AgeClaim(t, claimed.Claim, at)
			after, err = fixture.Snapshot(ctx, claimed.Claim.DeliveryID())
			if err != nil || !after.StartedAt.Equal(at) || !after.CreatedAt.Equal(at) || !after.ClaimExpiresAt.Equal(before.ClaimExpiresAt) || after.ClaimVersion != before.ClaimVersion {
				t.Fatalf("age cut changed live lease authority: %+v,error=%v", after, err)
			}
		})
	}
}
