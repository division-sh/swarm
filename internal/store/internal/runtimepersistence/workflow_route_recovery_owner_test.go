package runtimepersistence_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type activeWorkflowRouteReader interface {
	bus.FlowInstanceActivationCommitOwner
	LoadActiveWorkflowRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error)
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
}

func TestActiveWorkflowRouteRecoveryUsesExactRunOwnerOnBothStores(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) activeWorkflowRouteReader
	}{
		{
			name: "sqlite",
			open: func(t *testing.T) activeWorkflowRouteReader {
				return storetest.StartSQLiteRuntimeStore(t)
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) activeWorkflowRouteReader {
				return storetest.StartPostgresRuntimeStore(t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			ctx := testAuthorActivityContext()
			retiredRunID := uuid.NewString()
			currentRunID := uuid.NewString()
			ambiguousRunID := uuid.NewString()
			for _, fixture := range []storetest.RunFixture{
				{Origin: storetest.ScenarioSetupOrigin(), RunID: retiredRunID, State: runtimerunlifecycle.StateRunning},
				{Origin: storetest.ScenarioSetupOrigin(), RunID: currentRunID, State: runtimerunlifecycle.StateRunning},
				{Origin: storetest.ScenarioSetupOrigin(), RunID: ambiguousRunID, State: runtimerunlifecycle.StateRunning},
			} {
				storetest.RequireRun(t, ctx, selected, fixture)
			}

			instancePath := "review/" + uuid.NewString()
			retiredEntityID := uuid.NewString()
			currentEntityID := uuid.NewString()
			ambiguousEntityID := uuid.NewString()
			for _, owner := range []struct{ runID, entityID string }{{retiredRunID, retiredEntityID}, {currentRunID, currentEntityID}, {ambiguousRunID, ambiguousEntityID}} {
				constructWorkflowRecoveryComponent(t, ctx, selected, owner.runID, owner.entityID, instancePath)
			}
			if _, _, err := storetest.TerminalizeRun(ctx, selected, runtimerunlifecycle.TerminalRequest{RunID: retiredRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: ambiguousRunID, State: runtimerunlifecycle.StatePaused}); err != nil {
				t.Fatal(err)
			}
			currentIdentity := runtimeflowidentity.RunScopedFlowInstance{
				RunID: currentRunID,
				Route: runtimeflowidentity.RouteForInstancePath(instancePath),
			}
			record, err := selected.LoadActiveWorkflowRoute(ctx, currentIdentity)
			if err != nil {
				t.Fatalf("recover exact current owner with same-path siblings: %v", err)
			}
			if record.EntityID != currentEntityID {
				t.Fatalf("recovered entity_id = %q, want current owner %q instead of retired owner %q", record.EntityID, currentEntityID, retiredEntityID)
			}

			foreign, err := selected.LoadActiveWorkflowRoute(ctx, runtimeflowidentity.RunScopedFlowInstance{
				RunID: ambiguousRunID,
				Route: runtimeflowidentity.RouteForInstancePath(instancePath),
			})
			if err != nil || foreign.EntityID != ambiguousEntityID {
				t.Fatalf("recover same path from exact paused run: record=%#v err=%v", foreign, err)
			}

			if _, err := storetest.AddAmbiguousWorkflowProjectionFields(ctx, selected, currentRunID, currentEntityID); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.LoadActiveWorkflowRoute(ctx, currentIdentity); err == nil || !strings.Contains(err.Error(), "exactly one matching declared field row") {
				t.Fatalf("ambiguous exact-run owner error = %v", err)
			}

			if _, _, err := storetest.TerminalizeRun(ctx, selected, runtimerunlifecycle.TerminalRequest{
				RunID: currentRunID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("retire exact route owner run %s: %v", currentRunID, err)
			}
			if _, err := selected.LoadActiveWorkflowRoute(ctx, currentIdentity); err == nil || !strings.Contains(err.Error(), "active flow instance not found") {
				t.Fatalf("retired exact-run owner error = %v", err)
			}
		})
	}
}

func TestEntityStateSchemaRequiresExplicitNonblankEntityContractOnBothStores(t *testing.T) {
	tests := []struct {
		name string
		open func(*testing.T) activeWorkflowRouteReader
	}{
		{name: "sqlite", open: func(t *testing.T) activeWorkflowRouteReader {
			return storetest.StartSQLiteRuntimeStore(t)
		}},
		{name: "postgres", open: func(t *testing.T) activeWorkflowRouteReader {
			return storetest.StartPostgresRuntimeStore(t)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
				Origin: storetest.ScenarioSetupOrigin(), RunID: runID, State: runtimerunlifecycle.StateRunning,
			})
			if err := storetest.InsertDescriptorEntityStateWithoutType(ctx, selected, runID); err == nil {
				t.Fatal("fresh schema accepted omitted entity_type")
			}
			if err := storetest.InsertDescriptorEntityStateWithBlankType(ctx, selected, runID); err == nil {
				t.Fatal("fresh schema accepted blank entity_type")
			}
		})
	}
}

func constructWorkflowRecoveryComponent(t *testing.T, ctx context.Context, selected bus.FlowInstanceActivationCommitOwner, run, entity, path string) {
	t.Helper()
	at := time.Now().UTC()
	command, err := flowactivationfixture.Command(correlation.WithRunID(ctx, run), pipeline.WorkflowInstance{
		WorkflowName: "review", WorkflowVersion: "1", Mode: "template", EntityType: "review_entity",
		StorageRef: path, InstanceID: runtimeflowidentity.LogicalInstanceID(path), EntityID: entity,
		ParentFlowID: ".", ParentFlowInstance: run, ParentEntityID: run,
		CurrentState: "active", StageDefined: true, CreatedAt: at, EnteredStageAt: at, UpdatedAt: at,
	}, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.CommitFlowInstanceActivation(correlation.WithRunID(ctx, run), command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct exact recovery component: result=%+v err=%v", committed, err)
	}
}
