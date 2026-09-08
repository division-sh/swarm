package agentcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
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
)

type StateError struct {
	Err           error
	AgentID       string
	FlowInstance  string
	RunID         string
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
	case errors.Is(e.Err, ErrRunNotFound) && strings.TrimSpace(e.RunID) != "":
		return fmt.Sprintf("run not found: %s", strings.TrimSpace(e.RunID))
	case errors.Is(e.Err, ErrRunAlreadyTerminal) && strings.TrimSpace(e.RunID) != "":
		if status != "" {
			return fmt.Sprintf("run already terminal: %s current_status=%s", strings.TrimSpace(e.RunID), status)
		}
		return fmt.Sprintf("run already terminal: %s", strings.TrimSpace(e.RunID))
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
	DirectiveSourceV1RPC           = "v1_rpc"
	DirectiveSourceDashboardLegacy = "dashboard_legacy_adapter"
	DirectiveSourceInternalRuntime = "internal_runtime"
)

type RunTargetResolution struct {
	RunID string
	Mode  string
}

func (r RunTargetResolution) Normalized() RunTargetResolution {
	r.RunID = strings.TrimSpace(r.RunID)
	r.Mode = strings.TrimSpace(r.Mode)
	if r.Mode == "" {
		r.Mode = RunResolutionSpecified
	}
	return r
}

type SendDirectiveRequest struct {
	AgentID        string
	FlowInstance   string
	Directive      string
	RunID          string
	Source         string
	OperatorID     string
	ActorTokenID   string
	IdempotencyKey string
	RequestHash    string
}

type SendDirectiveResult struct {
	OK                 bool   `json:"ok"`
	AgentID            string `json:"-"`
	FlowInstance       string `json:"flow_instance,omitempty"`
	OperationID        string `json:"operation_id"`
	Response           string `json:"response,omitempty"`
	RunID              string `json:"run_id"`
	RunIDResolution    string `json:"run_id_resolution"`
	DirectiveEventID   string `json:"directive_event_id"`
	DirectiveEventType string `json:"directive_event_type"`
}

type BoardDirective struct {
	Directive       string
	Event           events.Event
	RunIDResolution string
	OperatorID      string
	Source          string
}

func NewDirectiveEvent(req SendDirectiveRequest, target RunTargetResolution, operationID, eventID string, now time.Time, posture executionposture.Posture) (events.Event, error) {
	var none events.Event
	agentID := strings.TrimSpace(req.AgentID)
	directive := strings.TrimSpace(req.Directive)
	target = target.Normalized()
	if agentID == "" {
		return none, errors.New("agent id is required")
	}
	if directive == "" {
		return none, errors.New("directive is required")
	}
	if target.RunID == "" {
		return none, errors.New("run_id is required")
	}
	if target.Mode != RunResolutionSpecified {
		return none, fmt.Errorf("unsupported run_id resolution %q", target.Mode)
	}
	if requestRunID := strings.TrimSpace(req.RunID); requestRunID == "" || requestRunID != target.RunID {
		return none, fmt.Errorf("directive request run_id %q does not match exact target run %q", requestRunID, target.RunID)
	}
	if !posture.Valid() {
		return none, errors.New("runtime execution posture is required")
	}
	operationID = strings.TrimSpace(operationID)
	if _, err := uuid.Parse(operationID); err != nil {
		return none, fmt.Errorf("operation_id must be a UUID: %w", err)
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return none, fmt.Errorf("directive_event_id must be a UUID: %w", err)
	}
	if _, err := uuid.Parse(target.RunID); err != nil {
		return none, fmt.Errorf("run_id must be a UUID: %w", err)
	}
	source := strings.TrimSpace(req.Source)
	if source == "" {
		source = DirectiveSourceInternalRuntime
	}
	operatorID := strings.TrimSpace(req.OperatorID)
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	payload := map[string]any{
		"operation_id":      operationID,
		"agent_id":          agentID,
		"directive_text":    directive,
		"mode":              DirectiveEventMode,
		"run_id":            target.RunID,
		"run_id_resolution": target.Mode,
		"source":            source,
		"timestamp":         now.Format(time.RFC3339Nano),
	}
	if flowInstance := strings.Trim(strings.TrimSpace(req.FlowInstance), "/"); flowInstance != "" {
		payload["flow_instance"] = flowInstance
	}
	if operatorID != "" {
		payload["operator_id"] = operatorID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return none, err
	}
	facts := events.EventFacts{
		ID: eventID, Type: events.EventType(DirectiveEventType),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload:  raw, ExecutionMode: posture.RootMode(), CreatedAt: now,
	}
	return events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: target.RunID})
}

func ValidateBoardDirective(d BoardDirective) error {
	if strings.TrimSpace(d.Directive) == "" {
		return errors.New("directive is required")
	}
	evt := d.Event
	if strings.TrimSpace(evt.ID()) == "" {
		return errors.New("directive event id is required")
	}
	if strings.TrimSpace(string(evt.Type())) != DirectiveEventType {
		return fmt.Errorf("directive event type = %q, want %s", strings.TrimSpace(string(evt.Type())), DirectiveEventType)
	}
	if strings.TrimSpace(evt.RunID()) == "" {
		return errors.New("directive event run_id is required")
	}
	return nil
}

type RestartRequest struct {
	RunID        string
	AgentID      string
	FlowInstance string
	OperationID  string
}

type RestartResult struct {
	RunID        string `json:"run_id"`
	AgentID      string `json:"agent_id"`
	FlowInstance string `json:"flow_instance,omitempty"`
	OperationID  string `json:"operation_id,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
}

type Controller interface {
	SendDirective(context.Context, SendDirectiveRequest) (SendDirectiveResult, error)
	Restart(context.Context, RestartRequest) (RestartResult, error)
}
