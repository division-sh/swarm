package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type standingServicePostCommitFaultOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	fault error
}

func (o *standingServicePostCommitFaultOwner) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	result, err := o.WorkflowPersistenceOwner.SuspendStandingService(ctx, operation)
	if result.DeliveryContinuationRequired {
		err = errors.Join(err, o.fault)
	}
	return result, err
}

func (o *standingServicePostCommitFaultOwner) ReconcileStandingServiceSet(ctx context.Context, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	results, err := o.WorkflowPersistenceOwner.ReconcileStandingServiceSet(ctx, candidates)
	for _, result := range results {
		if result.DeliveryContinuationRequired {
			return results, errors.Join(err, o.fault)
		}
	}
	return results, err
}

func TestStandingServiceAcknowledgedCleanupErrorStillSignalsContinuationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var (
				db       *sql.DB
				selected workflowTestSelectedStore
			)
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, selected = store.backend.ConstructionHandle(), store
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db, selected = opened, admitTestPostgresStore(t, opened)
			}

			cleanupFault := errors.New("standing postcommit cleanup failed")
			handoffFault := errors.New("standing postcommit handoff failed")
			owner := &standingServicePostCommitFaultOwner{
				WorkflowPersistenceOwner: selected,
				fault:                    errors.Join(cleanupFault, handoffFault),
			}
			workflow := newWorkflowTestCoordinator(t, db, runtimepipeline.NewWorkflowPersistence(owner), selected)
			ctx := testAuthorActivityRuntimeContext()
			artifact := storeTestSourceArtifact("standing-ack-continuation-" + backend)
			seedStoreTestPersistedArtifact(t, db, artifact)
			source := mustStoreTestSourceArtifactFact(artifact.BundleHash())
			candidate := func(path string) runtimepipeline.StandingServiceCandidate {
				return runtimepipeline.StandingServiceCandidate{
					ServiceID: runtimeflowidentity.StandingServiceID(path), FlowPath: path,
					InstanceID: uuid.NewString(), EntityID: uuid.NewString(), Source: source,
				}
			}
			suspendedCandidate := candidate("project/standing-ack-suspend")
			orphanedCandidate := candidate("project/standing-ack-orphan")
			created, err := workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{suspendedCandidate, orphanedCandidate})
			if err != nil || len(created) != 2 {
				t.Fatalf("create standing set = %+v, %v", created, err)
			}
			authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "standing-ack-proof", 1)
			if err != nil {
				t.Fatal(err)
			}
			var signals atomic.Int32
			registration, err := workflow.RegisterDeliveryContinuationSignal(authority, func() { signals.Add(1) })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Release)

			missing, err := workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: "project/missing", Actor: "test"})
			if err == nil || missing.DeliveryContinuationRequired || signals.Load() != 0 {
				t.Fatalf("unacknowledged suspend = %+v, err=%v, signals=%d", missing, err, signals.Load())
			}

			suspended, err := workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: suspendedCandidate.ServiceID, Actor: "test"})
			if !errors.Is(err, cleanupFault) || !errors.Is(err, handoffFault) || !suspended.DeliveryContinuationRequired || suspended.EffectiveState != "suspended" || signals.Load() != 1 {
				t.Fatalf("acknowledged suspend = %+v, err=%v, signals=%d", suspended, err, signals.Load())
			}

			results, err := workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{suspendedCandidate})
			acknowledged, orphaned := false, false
			for _, result := range results {
				acknowledged = acknowledged || result.DeliveryContinuationRequired
				orphaned = orphaned || result.ServiceID == string(orphanedCandidate.ServiceID) && result.EffectiveState == "orphaned"
			}
			if !errors.Is(err, cleanupFault) || !errors.Is(err, handoffFault) || !acknowledged || !orphaned || signals.Load() != 2 {
				t.Fatalf("acknowledged set = %+v, err=%v, signals=%d", results, err, signals.Load())
			}

			statuses, err := selected.ListStandingServiceStatuses(ctx)
			if err != nil || len(statuses) != 2 {
				t.Fatalf("durable standing statuses = %+v, %v", statuses, err)
			}
			for _, status := range statuses {
				switch status.ServiceID {
				case string(suspendedCandidate.ServiceID):
					if status.EffectiveState != "suspended" || !status.DeclarationPresent {
						t.Fatalf("suspended durable status = %+v", status)
					}
				case string(orphanedCandidate.ServiceID):
					if status.EffectiveState != "orphaned" || status.DeclarationPresent {
						t.Fatalf("orphaned durable status = %+v", status)
					}
				default:
					t.Fatalf("unexpected durable status = %+v", status)
				}
			}
		})
	}
}
