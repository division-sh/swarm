package runlifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

// LiveRunScope is relative to the selected store, which owns the deployment.
type LiveRunScope struct {
	FlowPath string `json:"flow_path"`
}

func (s LiveRunScope) Empty() bool { return s == (LiveRunScope{}) }

func (s LiveRunScope) Validate() error {
	if _, err := identity.AdmitFlowIdentity(s.FlowPath); err != nil {
		return fmt.Errorf("live selection flow: %w", err)
	}
	return nil
}

type LiveRun struct {
	RunID  string
	Scope  LiveRunScope
	Source runtimecorrelation.SourceArtifactFact
}

type FlowScopeReader interface {
	LoadRunLiveScope(context.Context, string) (LiveRunScope, error)
}

type LiveRunReader interface {
	FlowScopeReader
	LoadLiveRun(context.Context, LiveRunScope) (LiveRun, bool, error)
}

var ErrLiveRunChanged = errors.New("live predecessor changed before its handoff fence")

// LandingRun remains selected during pause; execution still uses LiveRunReader.
type LandingRun struct {
	LiveRun
	State State
}

type LandingRunReader interface {
	LoadLandingRun(context.Context, LiveRunScope) (LandingRun, bool, error)
}

type FlowRunReconciliation struct {
	LandingRun
	Created           bool
	CommittedMutation MutationDisposition
}

type FlowRunOwner interface {
	ReconcileFlowRun(context.Context, LiveRunScope, runtimecorrelation.SourceArtifactFact) (FlowRunReconciliation, error)
}

type LiveRunConflictError struct {
	Scope     LiveRunScope
	LiveRunID string
}

func (e *LiveRunConflictError) Error() string {
	return fmt.Sprintf("flow %s already has live run %s; pause it before continuing another run", e.Scope.FlowPath, e.LiveRunID)
}
