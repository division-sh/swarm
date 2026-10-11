package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func VerifyNativeWorkflowInstanceStoreLoadRouteRecoveryProjectionForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, instance := nativeRouteRecoveryFixtureForTest(t, backend, open)
			run := runtimeRunID(ctx)
			route := testWorkflowInstanceRoute(instance.StorageRef)
			owner := testRunScopedWorkflowRoute(ctx, route)
			projection, err := pc.workflowStore.LoadRouteRecoveryProjection(ctx, owner)
			if err != nil {
				t.Fatal(err)
			}
			if projection.Identity.Route() != route || projection.Identity.EntityID != instance.EntityID {
				t.Fatalf("native recovery identity=%#v, want exact constructed owner %#v", projection.Identity, instance)
			}
			if parent := projection.Identity.ParentRoute; parent.FlowID != instance.ParentFlowID || parent.FlowInstance != instance.ParentFlowInstance || parent.EntityID != instance.ParentEntityID {
				t.Fatalf("recovered parent=%#v, want exact structural constructor parent", parent)
			}
			again, err := pc.workflowStore.LoadRouteRecoveryProjection(ctx, owner)
			if err != nil || again.Identity != projection.Identity {
				t.Fatalf("recovery readback changed identity: %#v err=%v", again, err)
			}

			historicalRun := uuid.NewString()
			historicalCtx := runtimecorrelation.WithRunID(fixture.Context, historicalRun)
			if err := fixture.RequireRun(historicalCtx, historicalRun); err != nil {
				t.Fatal(err)
			}
			historical := constructNativeRouteRecoveryInstanceForTest(t, fixture, pc, historicalCtx)
			if historical.StorageRef != instance.StorageRef || historical.ParentEntityID == instance.ParentEntityID {
				t.Fatal("same authored child path must retain the distinct structural parent of each run")
			}
			historicalProjection, err := pc.workflowStore.LoadRouteRecoveryProjection(historicalCtx, testRunScopedWorkflowRoute(historicalCtx, route))
			if err != nil || historicalProjection.Identity.EntityID != historical.EntityID || historicalProjection.Identity.ParentRoute.EntityID != historical.ParentEntityID {
				t.Fatalf("historical exact recovery=%#v err=%v", historicalProjection, err)
			}
			terminal, _, err := fixture.Runs.MarkTerminalRun(historicalCtx, runtimerunlifecycle.TerminalRequest{
				RunID: historicalRun, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			})
			if err != nil || terminal.State != runtimerunlifecycle.StateCancelled {
				t.Fatalf("retire exact historical run: %+v err=%v", terminal, err)
			}
			projection, err = pc.workflowStore.LoadRouteRecoveryProjection(ctx, owner)
			if err != nil || projection.Identity.EntityID != instance.EntityID || projection.Identity.ParentRoute.EntityID != instance.ParentEntityID {
				t.Fatalf("historical cancellation escaped current owner: %#v err=%v", projection, err)
			}
			mismatched := runtimeflowidentity.StoredRoute(route.ScopeKey, "wrong-instance", route.InstancePath)
			if _, err := pc.workflowStore.LoadRouteRecoveryProjection(ctx, testRunScopedWorkflowRoute(ctx, mismatched)); err == nil || !strings.Contains(err.Error(), "disagrees with requested route") {
				t.Fatalf("mismatched route err=%v, want exact-route refusal", err)
			}
			if counts := fixture.Transactions(); counts.Active != 0 {
				t.Fatalf("recovery leaked a selected transaction: %+v run=%s", counts, run)
			}
		})
		for _, fault := range []string{"missing fields", "ambiguous fields", "numeric flow_path", "terminated header", "missing header"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				fixture, pc, ctx, instance := nativeRouteRecoveryFixtureForTest(t, backend, open)
				run := runtimeRunID(ctx)
				var changed int64
				var err error
				want := "active flow instance not found"
				switch fault {
				case "missing fields":
					changed, err = fixture.MissingFields(ctx, run, instance.EntityID)
					want = "exactly one matching declared field row"
				case "ambiguous fields":
					changed, err = fixture.AmbiguousFields(ctx, run, instance.EntityID)
					want = "exactly one matching declared field row"
				case "numeric flow_path":
					changed, err = fixture.NumericFlowPath(ctx, run, instance.StorageRef)
					want = "flow_path must be a string"
				case "terminated header":
					changed, err = fixture.Terminated(ctx, run, instance.StorageRef, time.Now().UTC())
				case "missing header":
					changed, err = fixture.MissingHeader(ctx, run, instance.StorageRef)
				}
				if err != nil || changed != 1 {
					t.Fatalf("exact %s fault changed=%d err=%v", fault, changed, err)
				}
				if _, err := pc.workflowStore.LoadRouteRecoveryProjection(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("%s recovery err=%v, want %q", fault, err, want)
				}
				if counts := fixture.Transactions(); counts.Active != 0 {
					t.Fatalf("fault recovery leaked selected transaction: %+v", counts)
				}
			})
		}
	}
}

func VerifyNativeWorkflowInstanceStoreLoadRouteRecoveryProjectionRejectsTerminatedTimestampForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, instance := nativeRouteRecoveryFixtureForTest(t, backend, open)
			if changed, err := fixture.ActiveTerminatedTimestamp(ctx, runtimeRunID(ctx), instance.StorageRef, time.Now().UTC()); err != nil || changed != 1 {
				t.Fatalf("active header with terminal timestamp fault changed=%d err=%v", changed, err)
			}
			if _, err := pc.workflowStore.LoadRouteRecoveryProjection(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("terminated row err=%v, want active-row refusal", err)
			}
		})
	}
}

func nativeRouteRecoveryFixtureForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, WorkflowInstance) {
	t.Helper()
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":          "name: native-route-recovery\nstages:\n  active: {}\n",
		"entities.yaml":        "test_entity: {}\n",
		"review/schema.yaml":   "name: review\nstages:\n  active: {}\n",
		"review/entities.yaml": "test_entity: {}\n",
	})
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	instance := constructNativeRouteRecoveryInstanceForTest(t, fixture, pc, ctx)
	return fixture, pc, ctx, instance
}

func constructNativeRouteRecoveryInstanceForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context) WorkflowInstance {
	t.Helper()
	root := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")
	if err := fixture.Construct(ctx, root); err != nil {
		t.Fatal(err)
	}
	child := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, "review")
	if err := fixture.Construct(ctx, child); err != nil {
		t.Fatal(err)
	}
	return child
}
