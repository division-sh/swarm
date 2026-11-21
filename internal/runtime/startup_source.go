package runtime

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// StartupSource carries one runtime's checked source through composed startup.
// It does not admit execution or replace any selected-store authority check.
type StartupSource struct {
	runtime *Runtime
	ctx     context.Context
	root    sourceartifact.RootAdmission
	grant   startupownership.LiveGenerationGrant
}

func (rt *Runtime) admitStartupSource(ctx context.Context) (*StartupSource, error) {
	if rt == nil {
		return nil, errors.New("runtime is required")
	}
	rt.lifecycleMu.Lock()
	grant := rt.startupGrant
	rt.lifecycleMu.Unlock()
	checked, root, err := rt.checkCompiledSource(ctx)
	if err != nil {
		return nil, err
	}
	return &StartupSource{runtime: rt, ctx: checked, root: root, grant: grant}, nil
}

// PreflightStartupSource is the first, read-only entrance for a composed start.
// The caller preflights all runtimes before preparing any of them.
func (rt *Runtime) PreflightStartupSource(ctx context.Context) (*StartupSource, error) {
	source, err := rt.admitStartupSource(ctx)
	if err != nil {
		return nil, err
	}
	if err := source.requireCurrent(); err != nil {
		return nil, err
	}
	if err := rt.preflightDynamicTopologyStartupChecked(source.ctx); err != nil {
		return nil, err
	}
	return source, nil
}

func (s *StartupSource) requireCurrent() error {
	if s == nil || s.runtime == nil {
		return errors.New("checked startup source is required")
	}
	rt := s.runtime
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.startupGrant != s.grant {
		return errors.New("runtime generation changed during startup preparation")
	}
	return nil
}

func (s *StartupSource) PrepareLifecycle() error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	rt := s.runtime
	rt.startupPrepareMu.Lock()
	defer rt.startupPrepareMu.Unlock()
	return rt.prepareStartupLifecycleLocked(s.ctx, s)
}

func (s *StartupSource) PrepareStart() (*PreparedStartup, error) {
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	rt := s.runtime
	rt.startupPrepareMu.Lock()
	defer rt.startupPrepareMu.Unlock()
	return rt.prepareStartLocked(s.ctx, s)
}

func (s *StartupSource) RootAdmission() (sourceartifact.RootAdmission, error) {
	if err := s.requireCurrent(); err != nil {
		return sourceartifact.RootAdmission{}, err
	}
	return s.root, nil
}

func (s *StartupSource) PrepareStandingTargets() ([]StandingTarget, []StandingActivation, func() error, error) {
	if err := s.requireCurrent(); err != nil {
		return nil, nil, nil, err
	}
	return s.runtime.standingTargetsMutation(s.ctx, "", true, s)
}

func (s *StartupSource) CompleteLiveFlowStartup() error {
	if err := s.requireCurrent(); err != nil {
		return err
	}
	return s.runtime.completeLiveFlowStartup(s.ctx, s)
}
