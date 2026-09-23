package runtimepersistence_test

import (
	"context"
	"errors"
	"testing"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type routeTopologyAfterCommitFaultStore struct {
	flowInstanceDescriptorAuthorityStore
	fault  error
	refuse bool
	calls  int
}

func (s *routeTopologyAfterCommitFaultStore) ReplaceFlowInstanceRouteTopology(ctx context.Context, sets []runtimebus.FlowInstanceRouteRecordSet) (runtimebus.FlowInstanceRouteTopologyResult, error) {
	if s.refuse {
		return runtimebus.FlowInstanceRouteTopologyResult{}, s.fault
	}
	result, err := s.flowInstanceDescriptorAuthorityStore.ReplaceFlowInstanceRouteTopology(ctx, sets)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	s.calls++
	return result, s.fault
}

func TestFlowRouteTopologyAcknowledgedFaultCompletesProcessFollowupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected flowInstanceDescriptorAuthorityStore
			var sqlite bool
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
				sqlite = true
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			db := storetest.Database(selected)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
			bundle := notifyallchildren.LoadBundle(t, notifyallchildren.Options{})
			source := semanticview.Wrap(bundle)
			fact := sourceartifactfixture.FactFor(bundle.SourceArtifact)
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
			scope, _ := runtimeauthoractivity.ScopeFromContext(ctx)
			ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(scope.RuntimeInstanceID, fact.BundleHash()))
			storetest.RequireBundleDataCatalog(t, ctx, selected, bundle)
			seedFlowInstanceDescriptorAuthorityCase(t, ctx, db, sqlite, runID, uuid.NewString(), fact.BundleHash(), fact.BundleHash(), source.WorkflowVersion(), "exact", false)
			fault := errors.New("injected route topology postcommit cleanup failure")
			wrapped := &routeTopologyAfterCommitFaultStore{flowInstanceDescriptorAuthorityStore: selected, fault: fault}
			eventBus, err := newStoreTestEventBus(t, wrapped, runtimebus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact})
			if err != nil {
				t.Fatal(err)
			}
			identity := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.DeriveRoute(notifyallchildren.ChildFlowID, "current")}
			req := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity, ActivationVariables: map[string]string{"account_id": "current"}}
			if err := eventBus.AddFlowInstanceRouteContext(ctx, req); !errors.Is(err, fault) || wrapped.calls != 1 {
				t.Fatalf("acknowledged route add err=%v commits=%d", err, wrapped.calls)
			}
			routes, err := selected.ListFlowInstanceRouteRecords(ctx, identity)
			if err != nil || len(routes) == 0 || !eventBus.HasFlowInstanceRoute(identity) {
				t.Fatalf("acknowledged route not durable and visible: routes=%#v visible=%v err=%v", routes, eventBus.HasFlowInstanceRoute(identity), err)
			}
			if err := eventBus.VerifyFlowInstanceRoute(ctx, identity); err != nil {
				t.Fatalf("process route differs from committed route: %v", err)
			}
			if err := eventBus.RemoveFlowInstanceRouteContext(ctx, identity); !errors.Is(err, fault) || wrapped.calls != 2 {
				t.Fatalf("acknowledged route retirement err=%v commits=%d", err, wrapped.calls)
			}
			routes, err = selected.ListFlowInstanceRouteRecords(ctx, identity)
			if err != nil || len(routes) != 0 || eventBus.HasFlowInstanceRoute(identity) {
				t.Fatalf("acknowledged route retirement incomplete: routes=%#v visible=%v err=%v", routes, eventBus.HasFlowInstanceRoute(identity), err)
			}
			wrapped.refuse = true
			if err := eventBus.AddFlowInstanceRouteContext(ctx, req); !errors.Is(err, fault) || wrapped.calls != 2 || eventBus.HasFlowInstanceRoute(identity) {
				t.Fatalf("unacknowledged add published route: err=%v commits=%d visible=%v", err, wrapped.calls, eventBus.HasFlowInstanceRoute(identity))
			}
			routes, err = selected.ListFlowInstanceRouteRecords(ctx, identity)
			if err != nil || len(routes) != 0 {
				t.Fatalf("unacknowledged add changed durable routes: %#v err=%v", routes, err)
			}
		})
	}
}
