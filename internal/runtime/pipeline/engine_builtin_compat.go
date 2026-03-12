package pipeline

import (
	"context"
	"strings"
	"sync"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"

	"github.com/google/uuid"
)

type handlerEngineContext struct {
	coordinator *FactoryPipelineCoordinator
	nodeID      string
	preview     bool
}

type handlerEngineAccumulator struct {
	Expected       []string         `json:"expected,omitempty"`
	ExpectedCount  int              `json:"expected_count,omitempty"`
	Received       map[string]bool  `json:"received,omitempty"`
	Items          []map[string]any `json:"items,omitempty"`
	LastEventID    string           `json:"last_event_id,omitempty"`
	LastEventType  string           `json:"last_event_type,omitempty"`
	LastSource     string           `json:"last_source,omitempty"`
	LastReceivedAt string           `json:"last_received_at,omitempty"`
}

type handlerEngineExecution struct {
	ctx         context.Context
	scope       *handlerEngineContext
	state       *WorkflowState
	handler     runtimecontracts.SystemNodeEventHandler
	event       events.Event
	payload     map[string]any
	entityID    string
	policy      map[string]any
	accumulated map[string]any
	fanOut      map[string]any
	transformed map[string]any
	outcome     handlerExecutionOutcome
	ruleApplied bool
}

func (e *handlerEngineExecution) coordinator() *FactoryPipelineCoordinator {
	if e == nil || e.scope == nil {
		return nil
	}
	return e.scope.coordinator
}

func (pc *FactoryPipelineCoordinator) lockWorkflowEntity(entityID string) func() {
	if pc == nil {
		return func() {}
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return func() {}
	}
	pc.entityLockMu.Lock()
	mu := pc.entityLocks[entityID]
	if mu == nil {
		mu = &sync.Mutex{}
		pc.entityLocks[entityID] = mu
	}
	pc.entityLockMu.Unlock()
	mu.Lock()
	return func() {
		mu.Unlock()
	}
}

func hasValidUUID(text string) bool {
	_, err := uuid.Parse(strings.TrimSpace(text))
	return err == nil
}

func normalizeHandlerStateField(field string) string {
	field = strings.TrimSpace(field)
	switch {
	case strings.HasPrefix(field, "entity."):
		return strings.TrimSpace(strings.TrimPrefix(field, "entity."))
	case strings.HasPrefix(field, "metadata."):
		return strings.TrimSpace(strings.TrimPrefix(field, "metadata."))
	default:
		return field
	}
}

func handlerGuardOnFail(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.OnFail)
}

func normalizeWorkflowGuardFailureAction(action string) string {
	action = strings.TrimSpace(strings.ToLower(action))
	switch action {
	case "":
		return ""
	case "block":
		return "blocked"
	default:
		return action
	}
}
