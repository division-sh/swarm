package agentcontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	StatusIdle       = "idle"
	StatusRunning    = "running"
	StatusPaused     = "paused"
	StatusFailed     = "failed"
	StatusTerminated = "terminated"
)

var (
	ErrAgentNotFound   = errors.New("agent not found")
	ErrAgentNotRunning = errors.New("agent not running")
)

type StateError struct {
	Err           error
	AgentID       string
	CurrentStatus string
}

func (e *StateError) Error() string {
	if e == nil {
		return ""
	}
	agentID := strings.TrimSpace(e.AgentID)
	status := strings.TrimSpace(e.CurrentStatus)
	switch {
	case errors.Is(e.Err, ErrAgentNotFound) && agentID != "":
		return fmt.Sprintf("agent not found: %s", agentID)
	case errors.Is(e.Err, ErrAgentNotRunning) && agentID != "" && status != "":
		return fmt.Sprintf("agent not running: %s current_status=%s", agentID, status)
	case e.Err != nil:
		return e.Err.Error()
	default:
		return "agent control state error"
	}
}

func (e *StateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type SendDirectiveRequest struct {
	AgentID      string
	Directive    string
	KillPrevious bool
}

type SendDirectiveResult struct {
	AgentID  string
	Response string
}

type RestartRequest struct {
	AgentID string
}

type RestartResult struct {
	AgentID string
}

type ReplayBacklogRequest struct {
	AgentID string
}

type ReplayBacklogResult struct {
	AgentID       string
	ReplayedCount int
}

type Controller interface {
	SendDirective(context.Context, SendDirectiveRequest) (SendDirectiveResult, error)
	Restart(context.Context, RestartRequest) (RestartResult, error)
	ReplayBacklog(context.Context, ReplayBacklogRequest) (ReplayBacklogResult, error)
}
