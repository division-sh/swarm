package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (rt *Runtime) WithParkedLiveRun(ctx context.Context, live runlifecycle.LiveRun, commit func(context.Context) error) error {
	if rt == nil || rt.Manager == nil || rt.Bus == nil || commit == nil {
		return errors.New("live switch requires its loaded runtime, Manager and pipeline owner")
	}
	parkCtx, release, err := rt.ParkLiveRun(ctx, live)
	if err != nil {
		return err
	}
	defer release()
	return commit(parkCtx)
}

func (rt *Runtime) ParkLiveRun(ctx context.Context, live runlifecycle.LiveRun) (context.Context, func(), error) {
	if rt == nil || rt.Manager == nil || rt.Bus == nil {
		return nil, nil, errors.New("live switch requires its loaded runtime, Manager and pipeline owner")
	}
	if !live.Source.Matches(rt.Options.SourceArtifactFact) || live.Scope.Validate() != nil {
		return nil, nil, errors.New("live predecessor differs from its loaded source or flow")
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err != nil {
		return nil, nil, err
	}
	ctx = startupownership.WithGenerationGrantEvidence(ctx, grant)
	ctx = correlation.WithSourceArtifactFact(ctx, live.Source)
	ctx = correlation.WithRuntimeInstanceID(ctx, rt.Options.RuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(rt.Options.RuntimeInstanceID, live.Source.BundleHash()))
	transition, err := rt.Bus.BeginRunStop(ctx, live.RunID)
	if err != nil {
		return nil, nil, err
	}
	release, err := rt.Manager.ParkLiveRun(ctx, live.RunID)
	if err != nil {
		transition.Done()
		return nil, nil, err
	}
	return ctx, sync.OnceFunc(func() { release(); transition.Done() }), nil
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
	ctx = correlation.WithRunID(ctx, runID)
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
