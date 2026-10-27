package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type flowRunMutation interface {
	Create(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	ReviseSource(context.Context, runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error)
}

func (s *RunLifecyclePostgresOwner) ReconcileFlowRun(ctx context.Context, scope runtimerunlifecycle.LiveRunScope, source runtimecorrelation.SourceArtifactFact) (runtimerunlifecycle.FlowRunReconciliation, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimerunlifecycle.FlowRunReconciliation, error) {
		return reconcileFlowRunTx(ctx, mutation.tx, mutation, scope, source)
	})
}

func (s *RunLifecycleSQLiteOwner) ReconcileFlowRun(ctx context.Context, scope runtimerunlifecycle.LiveRunScope, source runtimecorrelation.SourceArtifactFact) (runtimerunlifecycle.FlowRunReconciliation, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimerunlifecycle.FlowRunReconciliation, error) {
		return reconcileFlowRunTx(ctx, mutation.tx, mutation, scope, source)
	})
}

func reconcileFlowRunTx(ctx context.Context, tx *sql.Tx, mutation flowRunMutation, scope runtimerunlifecycle.LiveRunScope, source runtimecorrelation.SourceArtifactFact) (runtimerunlifecycle.FlowRunReconciliation, error) {
	if err := source.Validate(); err != nil {
		return runtimerunlifecycle.FlowRunReconciliation{}, err
	}
	current, found, err := loadLandingRun(ctx, tx, scope)
	if err != nil {
		return runtimerunlifecycle.FlowRunReconciliation{}, err
	}
	out := runtimerunlifecycle.FlowRunReconciliation{LandingRun: current, CommittedMutation: runtimerunlifecycle.MutationExactNoop}
	if found {
		if !current.Source.Matches(source) && current.State.Active() {
			out.CommittedMutation, err = mutation.ReviseSource(ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: current.RunID, Source: source})
			if err != nil {
				return runtimerunlifecycle.FlowRunReconciliation{}, err
			}
			out.Source = source
		}
		return out, nil
	}
	request := runtimerunlifecycle.CreateRequest{RunID: uuid.NewString(), Origin: runtimerunlifecycle.DeploymentRunOrigin(), Source: source, StartedAt: time.Now().UTC(), LiveScope: scope}
	out.CommittedMutation, err = mutation.Create(ctx, request)
	if err != nil {
		return runtimerunlifecycle.FlowRunReconciliation{}, err
	}
	out.LandingRun, found, err = loadLandingRun(ctx, tx, scope)
	if err != nil {
		return runtimerunlifecycle.FlowRunReconciliation{}, err
	}
	if !found || out.RunID != request.RunID {
		return runtimerunlifecycle.FlowRunReconciliation{}, fmt.Errorf("reconciled deployment run did not become its landing run")
	}
	out.Created = true
	return out, nil
}
