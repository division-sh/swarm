package runtime

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (rt *Runtime) WithParkedLiveRun(ctx context.Context, live runlifecycle.LiveRun, commit func() error) error {
	if rt == nil || rt.Manager == nil || rt.Bus == nil || commit == nil {
		return errors.New("live switch requires its loaded runtime, Manager and pipeline owner")
	}
	if !live.Source.Matches(rt.Options.SourceArtifactFact) || live.Scope.Validate() != nil {
		return errors.New("live predecessor differs from its loaded source or flow")
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err != nil {
		return err
	}
	ctx = startupownership.WithGenerationGrantEvidence(ctx, grant)
	ctx = correlation.WithSourceArtifactFact(ctx, live.Source)
	transition, err := rt.Bus.BeginRunStop(ctx, live.RunID)
	if err != nil {
		return err
	}
	defer transition.Done()
	return rt.Manager.WithParkedRun(ctx, live.RunID, commit)
}

func (rt *Runtime) ReconcileLiveRun(ctx context.Context, runID string) error {
	if rt == nil || rt.Manager == nil || rt.Bus == nil {
		return errors.New("live reconciliation requires its ordinary runtime")
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err != nil {
		return err
	}
	ctx = startupownership.WithGenerationGrantEvidence(ctx, grant)
	ctx = correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact)
	if err := rt.Manager.ReconcileLiveRun(ctx, runID); err != nil {
		return err
	}
	if rt.GenericSchedules != nil {
		if err := rt.GenericSchedules.ReconcileRunWakeups(ctx, runID); err != nil {
			return err
		}
	}
	if rt.Pipeline != nil {
		if err := rt.Pipeline.RestoreWorkflowTimers(ctx); err != nil {
			return err
		}
	}
	_, err = rt.Bus.ReleaseRunQueue(ctx, runID, 200)
	return err
}
