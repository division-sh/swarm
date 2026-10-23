package runlifecycle

import (
	"context"
	"fmt"
	"path"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

// LiveRunScope is the deployment coordinate, not a bundle or execution identity.
type LiveRunScope struct {
	Deployment string `json:"deployment"`
	FlowPath   string `json:"flow_path"`
}

func (s LiveRunScope) Empty() bool { return s == (LiveRunScope{}) }

func (s LiveRunScope) Validate() error {
	if s.Deployment == "" || strings.TrimSpace(s.Deployment) != s.Deployment {
		return fmt.Errorf("live selection requires a canonical deployment")
	}
	if s.FlowPath == "" || path.Clean(s.FlowPath) != s.FlowPath || strings.HasPrefix(s.FlowPath, "/") ||
		s.FlowPath == ".." || strings.HasPrefix(s.FlowPath, "../") {
		return fmt.Errorf("live selection requires a canonical deployment-relative flow path")
	}
	return nil
}

type LiveRun struct {
	RunID  string
	Scope  LiveRunScope
	Source runtimecorrelation.SourceArtifactFact
}

type LiveRunReader interface {
	LoadLiveRun(context.Context, LiveRunScope) (LiveRun, bool, error)
}

type LiveRunConflictError struct {
	Scope     LiveRunScope
	LiveRunID string
}

func (e *LiveRunConflictError) Error() string {
	return fmt.Sprintf("flow %s already has live run %s; pause it before continuing another run", e.Scope.FlowPath, e.LiveRunID)
}
