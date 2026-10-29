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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type managerNativeDeliveryOwner interface {
	deliverylifecycle.Store
	storetest.RunFixtureStore
	sourceartifactfixture.Writer
	sourceartifactfixture.RootedWriter
	PipelineObligations() pipelineobligation.Store
	CommitPublication(context.Context, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
	RegisterAuthorActivityEventCatalog(authoractivity.Scope, []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error)
	ListAuthorActivity(context.Context, authoractivity.ListOptions) (authoractivity.ListResult, error)
}

func originalManagerFixtureContext(t testing.TB, ctx context.Context, selected agentfixture.Store) context.Context {
	t.Helper()
	if startupownership.HasMutationAuthority(ctx) {
		if _, err := startupownership.CaptureMutationAuthority(ctx); err != nil {
			t.Fatal(err)
		}
		return ctx
	}
	if _, present := correlation.RuntimeInstanceIDFromContext(ctx); !present {
		scope, ok := authoractivity.ScopeFromContext(ctx)
		if !ok || scope.RuntimeInstanceID == "" {
			t.Fatal("native manager fixture requires its declared runtime scope before acquisition")
		}
		ctx = correlation.WithRuntimeInstanceID(ctx, scope.RuntimeInstanceID)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func openManagerNativeDelivery(t *testing.T, backend string, sources ...semanticview.Source) *manager.ManagerDeliveryNativeFixture {
	t.Helper()
	if len(sources) > 1 {
		t.Fatal("native delivery fixture accepts one exact compiled source")
	}
	var selected managerNativeDeliveryOwner
	switch backend {
	case "sqlite":
		selected = storetest.StartSQLiteRuntimeStore(t)
	case "postgres":
		selected = storetest.StartPostgresRuntimeStore(t)
	default:
		t.Fatalf("unsupported manager delivery fixture backend %q", backend)
	}
	var rooted sourceartifact.RootedSource
	ctx := manager.TerminalPanicFixtureContext()
	var semanticSource semanticview.Source
	if len(sources) == 1 {
		semanticSource = sources[0]
		bundle, present := semanticview.Bundle(semanticSource)
		if !present || bundle == nil {
			t.Fatal("native delivery fixture requires its compiled source")
		}
		var err error
		rooted, err = bundle.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		rooted, _ = sourceartifactfixture.Rooted(t, "manager-delivery", sourceartifactfixture.Artifact())
	}
	artifact := rooted.Artifact()
	ctx = correlation.WithRootAdmission(ctx, rooted.Admission())
	source := sourceartifactfixture.FactFor(artifact)
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	scope, present := authoractivity.ScopeFromContext(ctx)
	if !present {
		t.Fatal("native delivery fixture requires its original author scope")
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(scope.RuntimeInstanceID, source.BundleHash()))
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, "manager-delivery-test", 1)
	if err != nil {
		t.Fatal(err)
	}
	original, ok := selected.(agentfixture.Store)
	if !ok {
		t.Fatalf("native manager fixture lacks its original process owner: %T", selected)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := selected.EnsureRootedSource(ctx, rooted); err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	originalContext := ctx
	return &manager.ManagerDeliveryNativeFixture{
		Store: selected, Authority: authority, Context: ctx, NormalAuthority: authority, SemanticSource: semanticSource,
		MutationContext: func(operation context.Context) (context.Context, error) {
			if startupownership.HasMutationAuthority(operation) {
				_, err := startupownership.CaptureMutationAuthority(operation)
				return operation, err
			}
			return startupownership.WithProcessMutationAuthority(operation, process)
		},
		CarryMutationContext: startupownership.CarryMutationAuthority,
		RequireRun: func(t *testing.T, runID string) {
			t.Helper()
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
				RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: artifact,
				RootAdmission: rooted.Admission(), StartedAt: time.Now().UTC(),
			})
		},
		Publish: func(t *testing.T, ctx context.Context, event events.Event, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) {
			t.Helper()
			ctx = startupownership.CarryMutationAuthority(ctx, originalContext)
			publishManagerNativeDelivery(t, ctx, selected, event, routes, authority)
		},
		ClaimHeartbeat: func(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
			ctx = startupownership.CarryMutationAuthority(ctx, originalContext)
			return storetest.ClaimManagerHeartbeatProof(ctx, selected, authority, event, route)
		},
		RenewHeartbeat: func(ctx context.Context, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
			ctx = startupownership.CarryMutationAuthority(ctx, originalContext)
			return storetest.RenewManagerHeartbeatProof(ctx, selected, claim)
		},
		RetryReady: func(t *testing.T, event events.Event, route events.DeliveryRoute) {
			t.Helper()
			if err := storetest.WaitManagerRetryDecision(ctx, selected, event, route); err != nil {
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

func managerProjectionDeliverySource(t *testing.T) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.WriteManagerExecutionProjection(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func managerSourceSetDeliverySource(t *testing.T) semanticview.Source {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"events.yaml":   []byte("test.source_set_wait:\ntest.source_set_cancelled:\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "delivery", artifact)
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func managerFailureDeliverySource(t *testing.T) semanticview.Source {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"events.yaml":   []byte("work.requested:\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "delivery", artifact)
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func managerClaimedExecutorDeliverySource(t *testing.T) semanticview.Source {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"events.yaml":   []byte("input.first:\ninput.second:\ntest.lane.child:\ntest.lane.later:\ntest.lane.root:\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "delivery", artifact)
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
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
			ctx := correlation.WithSourceArtifactFact(fixture.Context, source)
			root, present, err := correlation.RootAdmissionFromContext(fixture.Context)
			if err != nil || !present {
				t.Fatalf("native publication requires its captured root: present=%t err=%v", present, err)
			}
			ctx = correlation.WithRootAdmission(ctx, root)
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
			fixture := openManagerNativeDelivery(t, backend, managerProjectionDeliverySource(t))
			selected, ok := fixture.Store.(runforkexecution.SourceArtifactSelectedContractSourceStore)
			if !ok {
				t.Fatal("original native delivery owner has no retained selected source role")
			}
			repo := canonicalrouting.RepoRoot(t)
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, Store: selected}
			ctx := fixture.Context
			hash := fixture.Authority.SourceArtifact().BundleHash()
			admitted, err := loader.InspectRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{
				Responsibility: runforkexecution.FreshSelectedSource(hash),
				BundleHash:     hash,
				Selection:      runfork.RunForkContractSelection{Mode: "bundle_hash", BundleHash: hash},
			})
			if err != nil || admitted.Source == nil || admitted.Module == nil || admitted.SourceArtifactFact.BundleHash() != hash || admitted.RuntimeProjection != nil || admitted.Cleanup != nil {
				t.Fatalf("admitted source inspection changed authority or materialized execution: %+v,error=%v", admitted, err)
			}
			writer, ok := fixture.Store.(sourceartifactfixture.Writer)
			if !ok {
				t.Fatal("original native delivery owner has no source artifact writer")
			}
			// The shared fixture is now valid; this negative requires a declaration
			// whose bytes can be stored but whose agent intent is not admitted.
			storageArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
				"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"agents.yaml":   []byte("storage-only: {}\n"),
			})
			if err != nil {
				t.Fatal(err)
			}
			storageRoot, _ := sourceartifactfixture.Rooted(t, "storage-only", storageArtifact)
			storageOnly := storageRoot.Artifact()
			storageCtx := correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(storageOnly))
			storageCtx = correlation.WithRootAdmission(storageCtx, storageRoot.Admission())
			if err := sourceartifactfixture.EnsureArtifact(storageCtx, writer, storageOnly); err != nil {
				t.Fatal(err)
			}
			before, err := selected.GetSourceArtifact(storageCtx, storageOnly.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, fixture.Store, storetest.TransactionProbeOptions{})
			_, err = loader.InspectRunForkSelectedContractSourceForRequest(storageCtx, runforkexecution.SelectedContractSourceLoadRequest{
				Responsibility: runforkexecution.FreshSelectedSource(storageOnly.BundleHash()),
				BundleHash:     storageOnly.BundleHash(),
				Selection:      runfork.RunForkContractSelection{Mode: "bundle_hash", BundleHash: storageOnly.BundleHash()},
			})
			if err == nil || !strings.Contains(err.Error(), "BUNDLE_DATA_INTEGRITY_ERROR") || !strings.Contains(err.Error(), "agent intent source is required") {
				t.Fatalf("storage-only artifact must not mint executable selected authority: %v", err)
			}
			after, err := selected.GetSourceArtifact(storageCtx, storageOnly.BundleHash())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused inspection changed its stored source: before=%+v,after=%+v,error=%v", before, after, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.WriteCommits != 0 {
				t.Fatalf("read-only source inspection retained transaction/write authority: %+v", counts)
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
