package agentcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
)

const (
	StatusIdle       = "idle"
	StatusRunning    = "running"
	StatusPaused     = "paused"
	StatusFailed     = "failed"
	StatusTerminated = "terminated"
)

var (
	ErrAgentNotFound      = errors.New("agent not found")
	ErrAgentNotRunning    = errors.New("agent not running")
	ErrRunNotFound        = errors.New("run not found")
	ErrRunAlreadyTerminal = errors.New("run already terminal")
	ErrAmbiguousRunTarget = errors.New("ambiguous run target")
)

type StateError struct {
	Err            error
	AgentID        string
	RunID          string
	CurrentStatus  string
	ActiveSessions []ActiveSessionTarget
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
	case errors.Is(e.Err, ErrRunNotFound) && strings.TrimSpace(e.RunID) != "":
		return fmt.Sprintf("run not found: %s", strings.TrimSpace(e.RunID))
	case errors.Is(e.Err, ErrRunAlreadyTerminal) && strings.TrimSpace(e.RunID) != "":
		if status != "" {
			return fmt.Sprintf("run already terminal: %s current_status=%s", strings.TrimSpace(e.RunID), status)
		}
		return fmt.Sprintf("run already terminal: %s", strings.TrimSpace(e.RunID))
	case errors.Is(e.Err, ErrAmbiguousRunTarget) && agentID != "":
		return fmt.Sprintf("ambiguous run target for agent %s", agentID)
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

const (
	DirectiveEventType = "platform.agent_directive"
	DirectiveEventMode = "directive"

	RunResolutionSpecified         = "specified"
	RunResolutionActiveSession     = "inferred_from_active_session"
	RunResolutionNewRunAllocated   = "new_run_allocated"
	DirectiveSourceV1RPC           = "v1_rpc"
	DirectiveSourceDashboardLegacy = "dashboard_legacy_adapter"
	DirectiveSourceBuilderRuntime  = "builder_runtime_adapter"
)

type ActiveSessionTarget struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id,omitempty"`
}

type RunTargetResolution struct {
	RunID          string
	Mode           string
	ActiveSessions []ActiveSessionTarget
}

func (r RunTargetResolution) Normalized() RunTargetResolution {
	r.RunID = strings.TrimSpace(r.RunID)
	r.Mode = strings.TrimSpace(r.Mode)
	if r.Mode == "" {
		r.Mode = RunResolutionNewRunAllocated
	}
	out := make([]ActiveSessionTarget, 0, len(r.ActiveSessions))
	for _, session := range r.ActiveSessions {
		session.SessionID = strings.TrimSpace(session.SessionID)
		session.RunID = strings.TrimSpace(session.RunID)
		if session.SessionID == "" && session.RunID == "" {
			continue
		}
		out = append(out, session)
	}
	r.ActiveSessions = out
	return r
}

type SendDirectiveRequest struct {
	AgentID    string
	Directive  string
	RunID      string
	Source     string
	OperatorID string
}

type SendDirectiveResult struct {
	AgentID            string
	Response           string
	RunID              string
	RunIDResolution    string
	DirectiveEventID   string
	DirectiveEventType string
}

type BoardDirective struct {
	Directive       string
	Event           events.Event
	RunIDResolution string
	OperatorID      string
	Source          string
}

func NewDirectiveEvent(req SendDirectiveRequest, target RunTargetResolution, now time.Time) (events.Event, error) {
	agentID := strings.TrimSpace(req.AgentID)
	directive := strings.TrimSpace(req.Directive)
	target = target.Normalized()
	if agentID == "" {
		return events.Event{}, errors.New("agent id is required")
	}
	if directive == "" {
		return events.Event{}, errors.New("directive is required")
	}
	if target.RunID == "" {
		return events.Event{}, errors.New("run_id is required")
	}
	if _, err := uuid.Parse(target.RunID); err != nil {
		return events.Event{}, fmt.Errorf("run_id must be a UUID: %w", err)
	}
	source := strings.TrimSpace(req.Source)
	if source == "" {
		source = DirectiveSourceBuilderRuntime
	}
	operatorID := strings.TrimSpace(req.OperatorID)
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	payload := map[string]any{
		"agent_id":          agentID,
		"directive_text":    directive,
		"mode":              DirectiveEventMode,
		"run_id":            target.RunID,
		"run_id_resolution": target.Mode,
		"source":            source,
		"timestamp":         now.Format(time.RFC3339Nano),
	}
	if operatorID != "" {
		payload["operator_id"] = operatorID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, err
	}
	return events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(DirectiveEventType),
		SourceAgent: "runtime",
		RunID:       target.RunID,
		Payload:     raw,
		CreatedAt:   now,
	}, nil
}

func ValidateBoardDirective(d BoardDirective) error {
	if strings.TrimSpace(d.Directive) == "" {
		return errors.New("directive is required")
	}
	evt := d.Event
	if strings.TrimSpace(evt.ID) == "" {
		return errors.New("directive event id is required")
	}
	if strings.TrimSpace(string(evt.Type)) != DirectiveEventType {
		return fmt.Errorf("directive event type = %q, want %s", strings.TrimSpace(string(evt.Type)), DirectiveEventType)
	}
	if strings.TrimSpace(evt.RunID) == "" {
		return errors.New("directive event run_id is required")
	}
	return nil
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
