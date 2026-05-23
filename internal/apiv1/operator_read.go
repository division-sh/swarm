package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

type Pinger interface {
	Ping(context.Context) error
}

type RunReadStore interface {
	LoadRunHeader(context.Context, string) (store.RunHeader, error)
	ListRunHeaders(context.Context, store.RunHeaderListOptions) ([]store.RunHeader, string, error)
	LoadRunDebugReport(context.Context, string, store.RunDebugQueryOptions) (store.RunDebugReport, error)
}

type ObservabilityReadStore interface {
	LoadRunDebugTracePage(context.Context, string, store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, string, error)
	ListOperatorEvents(context.Context, store.OperatorEventListOptions) (store.OperatorEventListResult, error)
	LoadOperatorEvent(context.Context, string) (store.OperatorEventFull, error)
	ListOperatorRuntimeLogs(context.Context, store.OperatorRuntimeLogListOptions) (store.OperatorRuntimeLogListResult, error)
	ListOperatorRuntimeIncidents(context.Context, store.OperatorRuntimeIncidentListOptions) (store.OperatorRuntimeIncidentListResult, error)
}

type EntityReadStore interface {
	ListOperatorEntities(context.Context, store.OperatorEntityListOptions) (store.OperatorEntityListResult, error)
	LoadOperatorEntity(context.Context, string, string) (store.OperatorEntityFull, error)
	AggregateOperatorEntities(context.Context, store.OperatorEntityAggregateOptions) (store.OperatorEntityAggregateResult, error)
}

type AgentConversationReadStore interface {
	ListOperatorAgents(context.Context, store.OperatorAgentListOptions) (store.OperatorAgentListResult, error)
	LoadOperatorAgent(context.Context, string) (store.OperatorAgentDetail, error)
	LoadOperatorAgentDiagnosis(context.Context, string, store.OperatorAgentDiagnosisOptions) (store.OperatorAgentDiagnosis, error)
	ListOperatorConversations(context.Context, store.OperatorConversationListOptions) (store.OperatorConversationListResult, error)
	LoadOperatorConversation(context.Context, string) (store.OperatorConversationDetail, error)
	LoadOperatorConversationTurn(context.Context, string, int) (store.OperatorConversationTurnDetail, error)
	LoadCurrentOperatorConversationForAgent(context.Context, string) (*store.OperatorConversationDetail, error)
}

type OperatorReadOptions struct {
	Now                   func() time.Time
	Ready                 func() bool
	Database              Pinger
	Runs                  RunReadStore
	Observability         ObservabilityReadStore
	Entities              EntityReadStore
	AgentConversations    AgentConversationReadStore
	AgentControl          AgentControlController
	Mailbox               MailboxAPIStore
	Idempotency           APIIdempotencyStore
	Events                EventPublisher
	RunControl            RunControlController
	RuntimeIngress        RuntimeIngressController
	ResetCoordinator      DestructiveResetCoordinator
	ResetQuiescer         DestructiveResetQuiescer
	ResetCleaner          DestructiveResetCleaner
	ResetContainers       DestructiveResetContainerStopper
	Source                semanticview.Source
	MailboxApprovalRoutes map[string]string
	Bundle                runtimecontracts.BundleIdentity
}

type healthPingResult struct {
	OK bool   `json:"ok"`
	TS string `json:"ts"`
}

type healthCheckResult struct {
	Alive     bool                            `json:"alive"`
	Ready     bool                            `json:"ready"`
	DBOK      bool                            `json:"db_ok"`
	RuntimeOK bool                            `json:"runtime_ok"`
	Bundle    runtimecontracts.BundleIdentity `json:"bundle"`
}

type runGetResult struct {
	Run store.RunHeader `json:"run"`
}

type runListResult struct {
	Runs       []store.RunHeader `json:"runs"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type runTraceListResult struct {
	Trace      []store.RunDebugTraceRow `json:"trace"`
	NextCursor string                   `json:"next_cursor,omitempty"`
}

type runDiagnosis struct {
	Run              store.RunHeader `json:"run"`
	OperationalState string          `json:"operational_state"`
	BlockingLayer    string          `json:"blocking_layer"`
	BlockingReason   string          `json:"blocking_reason"`
	Heuristics       []string        `json:"heuristics"`
}

var runListStatuses = map[string]struct{}{
	"running":   {},
	"paused":    {},
	"completed": {},
	"failed":    {},
	"cancelled": {},
	"forked":    {},
}

func OperatorReadHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	ready := opts.Ready
	if ready == nil {
		ready = func() bool { return false }
	}
	handlers := map[string]MethodHandler{
		"health.ping": func(context.Context, Request) (any, error) {
			return healthPingResult{OK: true, TS: now().UTC().Format(time.RFC3339Nano)}, nil
		},
		"health.check": func(ctx context.Context, _ Request) (any, error) {
			return operatorHealthSnapshot(ctx, ready, opts.Database, opts.Bundle), nil
		},
		"run.get": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			header, err := runs.LoadRunHeader(ctx, runID)
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			return runGetResult{Run: header}, nil
		},
		"run.list": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			listOpts, err := runHeaderListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			headers, nextCursor, err := runs.ListRunHeaders(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidRunListCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid run list cursor"})
			}
			if err != nil {
				return nil, err
			}
			if headers == nil {
				headers = []store.RunHeader{}
			}
			return runListResult{Runs: headers, NextCursor: nextCursor}, nil
		},
		"run.diagnose": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			header, err := runs.LoadRunHeader(ctx, runID)
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			report, err := runs.LoadRunDebugReport(ctx, runID, store.RunDebugQueryOptions{})
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			status := store.ProjectRunOperationalStatus(report)
			return runDiagnosis{
				Run:              header,
				OperationalState: strings.TrimSpace(status.State),
				BlockingLayer:    strings.TrimSpace(status.BlockingLayer),
				BlockingReason:   strings.TrimSpace(status.BlockingReason),
				Heuristics:       status.Heuristics,
			}, nil
		},
	}
	for name, handler := range OperatorMailboxHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorRunStartHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorEventPublishHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorEventReplayHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorRunControlHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorRuntimeControlHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorRuntimeNukeHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorObservabilityHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorEntityHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorAgentConversationHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorAgentControlHandlers(opts) {
		handlers[name] = handler
	}
	return handlers
}

func requireRunReadStore(runs RunReadStore) (RunReadStore, error) {
	if runs == nil {
		return nil, fmt.Errorf("run read store is required")
	}
	return runs, nil
}

func requireObservabilityReadStore(reads ObservabilityReadStore) (ObservabilityReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("observability read store is required")
	}
	return reads, nil
}

func requireEntityReadStore(reads EntityReadStore) (EntityReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("entity read store is required")
	}
	return reads, nil
}

func requireAgentConversationReadStore(reads AgentConversationReadStore) (AgentConversationReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("agent/conversation read store is required")
	}
	return reads, nil
}

func OperatorAgentConversationHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.AgentConversations == nil {
		return nil
	}
	return map[string]MethodHandler{
		"agent.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorAgentListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorAgents(ctx, listOpts)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"agent.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			agentID, err := requiredStringParam(req.Params, "agent_id")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadOperatorAgent(ctx, agentID)
			if errors.Is(err, store.ErrAgentNotFound) {
				return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"agent.diagnose": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			agentID, err := requiredStringParam(req.Params, "agent_id")
			if err != nil {
				return nil, err
			}
			queueLimit, err := boundedIntegerParam(req.Params, "queue_limit", 1, store.MaxAgentDiagnosisQueueLimit)
			if err != nil {
				return nil, err
			}
			queueCursor, _, err := optionalStringParam(req.Params, "queue_cursor")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadOperatorAgentDiagnosis(ctx, agentID, store.OperatorAgentDiagnosisOptions{
				QueueLimit:  queueLimit,
				QueueCursor: queueCursor,
			})
			if errors.Is(err, store.ErrAgentNotFound) {
				return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
			}
			if errors.Is(err, store.ErrInvalidPendingAgentDeliveryCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "queue_cursor", "reason": "invalid agent.diagnose queue cursor"})
			}
			if err != nil {
				return nil, err
			}
			if err := validateAgentDiagnosisResult(result); err != nil {
				return nil, err
			}
			return result, nil
		},
		"conversation.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorConversationListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorConversations(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidConversationCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid conversation list cursor"})
			}
			if paramErr := entityReadParamError(err); paramErr != nil {
				return nil, NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"conversation.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			sessionID, err := requiredStringParam(req.Params, "session_id")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadOperatorConversation(ctx, sessionID)
			if errors.Is(err, store.ErrSessionNotFound) {
				return nil, NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": sessionID})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"conversation.get_turn": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			sessionID, err := requiredStringParam(req.Params, "session_id")
			if err != nil {
				return nil, err
			}
			turnIndex, err := requiredBoundedIntegerParam(req.Params, "turn_index", 1, 1000000)
			if err != nil {
				return nil, err
			}
			includeLogs, err := optionalBoolParam(req.Params, "include_logs", true)
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadOperatorConversationTurn(ctx, sessionID, turnIndex)
			if errors.Is(err, store.ErrSessionNotFound) {
				return nil, NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": sessionID})
			}
			if errors.Is(err, store.ErrTurnNotFound) {
				return nil, NewApplicationError(TurnNotFoundCode, false, map[string]any{"session_id": sessionID, "turn_index": turnIndex})
			}
			if err != nil {
				return nil, err
			}
			if includeLogs {
				observability, err := requireObservabilityReadStore(opts.Observability)
				if err != nil {
					return nil, err
				}
				logOpts := store.OperatorRuntimeLogListOptions{
					SessionID: result.Session.SessionID,
					Since:     &result.RuntimeLogWindowStart,
					Until:     result.RuntimeLogWindowEnd,
					Limit:     1000,
					Order:     "asc",
				}
				logs, err := observability.ListOperatorRuntimeLogs(ctx, logOpts)
				if errors.Is(err, store.ErrInvalidObservabilityCursor) {
					return nil, NewInvalidParamsError(map[string]any{"field": "runtime_log_entries", "reason": "invalid runtime log cursor"})
				}
				if err != nil {
					return nil, err
				}
				if logs.Logs == nil {
					logs.Logs = []store.OperatorRuntimeLogEntry{}
				}
				result.Turn.RuntimeLogEntries = logs.Logs
			}
			return result, nil
		},
		"conversation.current_for_agent": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireAgentConversationReadStore(opts.AgentConversations)
			if err != nil {
				return nil, err
			}
			agentID, err := requiredStringParam(req.Params, "agent_id")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadCurrentOperatorConversationForAgent(ctx, agentID)
			if errors.Is(err, store.ErrAgentNotFound) {
				return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func validateAgentDiagnosisResult(item store.OperatorAgentDiagnosis) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: agent_id is required")
	}
	if !validAgentDiagnosisStatus(item.Status) {
		return fmt.Errorf("agent.diagnose owner returned malformed result: status=%q is not a valid AgentStatus", item.Status)
	}
	if item.Queue.PendingCount < 0 {
		return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_count must be non-negative")
	}
	if item.Queue.OldestPendingAgeSeconds < 0 {
		return fmt.Errorf("agent.diagnose owner returned malformed result: queue.oldest_pending_age_seconds must be non-negative")
	}
	if item.Queue.PendingDeliveries == nil {
		return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries must be an array")
	}
	for i, detail := range item.Queue.PendingDeliveries {
		if strings.TrimSpace(detail.EventID) == "" {
			return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries[%d].event_id is required", i)
		}
		if strings.TrimSpace(detail.EventName) == "" {
			return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries[%d].event_name is required", i)
		}
		if detail.EnqueuedAt.IsZero() {
			return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries[%d].enqueued_at is required", i)
		}
		if detail.Attempts < 0 {
			return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries[%d].attempts must be non-negative", i)
		}
	}
	if item.DeliveryLifecycle != nil {
		if !validAgentDeliveryLifecycleState(item.DeliveryLifecycle.State) {
			return fmt.Errorf("agent.diagnose owner returned malformed result: delivery_lifecycle.state=%q is not valid", item.DeliveryLifecycle.State)
		}
		if strings.TrimSpace(item.DeliveryLifecycle.BlockingLayer) == "" {
			return fmt.Errorf("agent.diagnose owner returned malformed result: delivery_lifecycle.blocking_layer is required")
		}
	}
	if err := validateAgentDiagnosisActiveResult(item.Active); err != nil {
		return err
	}
	if err := validateAgentDiagnosisRuntimeStateResult(item.RuntimeState); err != nil {
		return err
	}
	if err := validateAgentDiagnosisLastToolOutcomeResult(item.LastToolOutcome); err != nil {
		return err
	}
	if item.LastToolOutcome != nil {
		if item.Active == nil {
			return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome requires active selected-turn evidence")
		}
		activeTurnID := strings.TrimSpace(item.Active.TurnID)
		lastToolTurnID := strings.TrimSpace(item.LastToolOutcome.TurnID)
		if activeTurnID != lastToolTurnID {
			return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.turn_id %q must match active.turn_id %q", lastToolTurnID, activeTurnID)
		}
	}
	return nil
}

func validateAgentDiagnosisActiveResult(item *store.OperatorAgentDiagnosisActive) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: active.turn_id is required")
	}
	return nil
}

func validateAgentDiagnosisRuntimeStateResult(item *store.OperatorAgentDiagnosisRuntimeState) error {
	if item == nil {
		return nil
	}
	if item.Watchdog == nil {
		return fmt.Errorf("agent.diagnose owner returned malformed result: runtime_state.watchdog is required")
	}
	watchdog := store.ConversationRuntimeWatchdogDescriptor{
		State:         strings.TrimSpace(item.Watchdog.State),
		BlockingLayer: strings.TrimSpace(item.Watchdog.BlockingLayer),
		Action:        strings.TrimSpace(item.Watchdog.Action),
		Outcome:       strings.TrimSpace(item.Watchdog.Outcome),
		LastOutputAt:  strings.TrimSpace(item.Watchdog.LastOutputAt),
		RecordedAt:    strings.TrimSpace(item.Watchdog.RecordedAt),
	}
	if err := store.ValidateConversationRuntimeWatchdogDescriptor(watchdog); err != nil {
		return fmt.Errorf("agent.diagnose owner returned malformed result: runtime_state.watchdog is invalid: %w", err)
	}
	return nil
}

func validateAgentDiagnosisLastToolOutcomeResult(item *store.OperatorAgentLastToolOutcome) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.turn_id is required")
	}
	if strings.TrimSpace(item.ToolName) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.tool_name is required")
	}
	if item.Result != nil {
		trimmed := bytes.TrimSpace(item.Result)
		if len(trimmed) == 0 {
			return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.result is empty")
		}
		var obj map[string]any
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.result must be a JSON object: %w", err)
		}
		if obj == nil {
			return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.result must be a JSON object")
		}
	}
	return nil
}

func validAgentDiagnosisStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "idle", "running", "paused", "failed", "terminated":
		return true
	default:
		return false
	}
}

func validAgentDeliveryLifecycleState(state string) bool {
	switch strings.TrimSpace(state) {
	case "queued", "launching", "active", "retrying", "exhausted":
		return true
	default:
		return false
	}
}

func OperatorEntityHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.Entities == nil {
		return nil
	}
	return map[string]MethodHandler{
		"entity.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireEntityReadStore(opts.Entities)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorEntityListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorEntities(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidEntityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid entity list cursor"})
			}
			if paramErr := entityReadParamError(err); paramErr != nil {
				return nil, NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"entity.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireEntityReadStore(opts.Entities)
			if err != nil {
				return nil, err
			}
			entityID := stringParam(req.Params, "entity_id")
			runID, _, err := optionalStringParam(req.Params, "run_id")
			if err != nil {
				return nil, err
			}
			entity, err := reads.LoadOperatorEntity(ctx, entityID, runID)
			if errors.Is(err, store.ErrEntityNotFound) {
				return nil, NewApplicationError(EntityNotFoundCode, false, map[string]any{"entity_id": entityID})
			}
			if errors.Is(err, store.ErrAmbiguousEntityRunID) {
				return nil, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "required when entity_id exists in multiple runs"})
			}
			if paramErr := entityReadParamError(err); paramErr != nil {
				return nil, NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
			}
			if err != nil {
				return nil, err
			}
			return entity, nil
		},
		"entity.aggregate": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireEntityReadStore(opts.Entities)
			if err != nil {
				return nil, err
			}
			aggregateOpts, err := operatorEntityAggregateOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.AggregateOperatorEntities(ctx, aggregateOpts)
			if paramErr := entityReadParamError(err); paramErr != nil {
				return nil, NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func OperatorObservabilityHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.Observability == nil {
		return nil
	}
	return map[string]MethodHandler{
		"run.trace": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireObservabilityReadStore(opts.Observability)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			limit, err := boundedIntegerParam(req.Params, "limit", 1, 2000)
			if err != nil {
				return nil, err
			}
			cursor, _, err := optionalStringParam(req.Params, "cursor")
			if err != nil {
				return nil, err
			}
			since, err := timestampParam(req.Params, "since")
			if err != nil {
				return nil, err
			}
			rows, nextCursor, err := reads.LoadRunDebugTracePage(ctx, runID, store.RunDebugTraceQueryOptions{Limit: limit, Cursor: cursor, Since: since})
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if errors.Is(err, store.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid run trace cursor"})
			}
			if err != nil {
				return nil, err
			}
			if rows == nil {
				rows = []store.RunDebugTraceRow{}
			}
			return runTraceListResult{Trace: rows, NextCursor: nextCursor}, nil
		},
		"event.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireObservabilityReadStore(opts.Observability)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorEventListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorEvents(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid event list cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"event.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireObservabilityReadStore(opts.Observability)
			if err != nil {
				return nil, err
			}
			eventID := stringParam(req.Params, "event_id")
			event, err := reads.LoadOperatorEvent(ctx, eventID)
			if errors.Is(err, store.ErrEventNotFound) {
				return nil, NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": eventID})
			}
			if err != nil {
				return nil, err
			}
			return event, nil
		},
		"runtime.logs": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireObservabilityReadStore(opts.Observability)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorRuntimeLogListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorRuntimeLogs(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid runtime log cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"runtime.incidents": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireObservabilityReadStore(opts.Observability)
			if err != nil {
				return nil, err
			}
			listOpts, err := operatorRuntimeIncidentListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListOperatorRuntimeIncidents(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid runtime incident cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func operatorEntityListOptionsFromParams(params map[string]any) (store.OperatorEntityListOptions, error) {
	out := store.OperatorEntityListOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return store.OperatorEntityListOptions{}, err
	}
	if out.Flow, _, err = optionalStringParam(params, "flow"); err != nil {
		return store.OperatorEntityListOptions{}, err
	}
	if out.Type, _, err = optionalStringParam(params, "type"); err != nil {
		return store.OperatorEntityListOptions{}, err
	}
	if out.CurrentState, _, err = optionalStringParam(params, "current_state"); err != nil {
		return store.OperatorEntityListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return store.OperatorEntityListOptions{}, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return store.OperatorEntityListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	return out, nil
}

func operatorEntityAggregateOptionsFromParams(params map[string]any) (store.OperatorEntityAggregateOptions, error) {
	out := store.OperatorEntityAggregateOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return store.OperatorEntityAggregateOptions{}, err
	}
	if out.GroupBy, _, err = optionalStringParam(params, "group_by"); err != nil {
		return store.OperatorEntityAggregateOptions{}, err
	}
	if out.Type, _, err = optionalStringParam(params, "type"); err != nil {
		return store.OperatorEntityAggregateOptions{}, err
	}
	return out, nil
}

func operatorAgentListOptionsFromParams(params map[string]any) (store.OperatorAgentListOptions, error) {
	out := store.OperatorAgentListOptions{}
	var err error
	if out.Flow, _, err = optionalStringParam(params, "flow"); err != nil {
		return store.OperatorAgentListOptions{}, err
	}
	if out.Role, _, err = optionalStringParam(params, "role"); err != nil {
		return store.OperatorAgentListOptions{}, err
	}
	return out, nil
}

func operatorConversationListOptionsFromParams(params map[string]any) (store.OperatorConversationListOptions, error) {
	out := store.OperatorConversationListOptions{}
	var err error
	if out.AgentID, _, err = optionalStringParam(params, "agent_id"); err != nil {
		return store.OperatorConversationListOptions{}, err
	}
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return store.OperatorConversationListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return store.OperatorConversationListOptions{}, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return store.OperatorConversationListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	return out, nil
}

func entityReadParamError(err error) *store.EntityReadParamError {
	if err == nil {
		return nil
	}
	var paramErr *store.EntityReadParamError
	if errors.As(err, &paramErr) {
		return paramErr
	}
	return nil
}

func operatorEventListOptionsFromParams(params map[string]any) (store.OperatorEventListOptions, error) {
	out := store.OperatorEventListOptions{}
	filter, err := eventListFilterParam(params)
	if err != nil {
		return store.OperatorEventListOptions{}, err
	}
	out.Filter = filter
	limit, err := boundedIntegerParam(params, "limit", 1, 1000)
	if err != nil {
		return store.OperatorEventListOptions{}, err
	}
	out.Limit = limit
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return store.OperatorEventListOptions{}, err
	}
	out.Cursor = cursor
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return store.OperatorEventListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return store.OperatorEventListOptions{}, err
	}
	return out, nil
}

func eventListFilterParam(params map[string]any) (store.OperatorEventListFilter, error) {
	raw, ok := params["filter"]
	if !ok || isEmptyParam(raw) {
		return store.OperatorEventListFilter{}, nil
	}
	filter, ok := raw.(map[string]any)
	if !ok {
		return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter", "reason": "must be an object"})
	}
	for name := range filter {
		if _, ok := eventListFilterFields[name]; !ok {
			return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "unknown parameter"})
		}
	}
	out := store.OperatorEventListFilter{}
	var err error
	if out.RunID, _, err = optionalStringParam(filter, "run_id"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.RunID != "" && !opaqueIDPattern.MatchString(out.RunID) {
		return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.run_id", "reason": "must match OpaqueId pattern"})
	}
	if out.EntityID, _, err = optionalStringParam(filter, "entity_id"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.EntityID != "" && !opaqueIDPattern.MatchString(out.EntityID) {
		return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.entity_id", "reason": "must match OpaqueId pattern"})
	}
	if out.EventName, _, err = optionalStringParam(filter, "event_name"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.DeliveryStatus, _, err = optionalStringParam(filter, "delivery_status"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.DeliveryStatus != "" {
		if _, ok := eventListDeliveryStatuses[out.DeliveryStatus]; !ok {
			return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.delivery_status", "reason": "must be a valid DeliveryStatus"})
		}
	}
	if out.SubscriberID, _, err = optionalStringParam(filter, "subscriber_id"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.SubscriberType, _, err = optionalStringParam(filter, "subscriber_type"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if out.SubscriberType != "" {
		if _, ok := eventListSubscriberTypes[out.SubscriberType]; !ok {
			return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.subscriber_type", "reason": "must be a valid SubscriberType"})
		}
	}
	if out.ReasonCode, _, err = optionalStringParam(filter, "reason_code"); err != nil {
		return store.OperatorEventListFilter{}, err
	}
	if rawBool, ok := filter["has_dead_letter"]; ok && !isEmptyParam(rawBool) {
		value, ok := rawBool.(bool)
		if !ok {
			return store.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.has_dead_letter", "reason": "must be a boolean"})
		}
		out.HasDeadLetter = &value
	}
	return out, nil
}

var eventListFilterFields = map[string]struct{}{
	"run_id":          {},
	"entity_id":       {},
	"event_name":      {},
	"delivery_status": {},
	"subscriber_id":   {},
	"subscriber_type": {},
	"reason_code":     {},
	"has_dead_letter": {},
}

var eventListDeliveryStatuses = map[string]struct{}{
	"pending":     {},
	"in_progress": {},
	"delivered":   {},
	"failed":      {},
	"dead_letter": {},
}

var eventListSubscriberTypes = map[string]struct{}{
	"node":  {},
	"agent": {},
}

func operatorRuntimeLogListOptionsFromParams(params map[string]any) (store.OperatorRuntimeLogListOptions, error) {
	out := store.OperatorRuntimeLogListOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.EntityID, _, err = optionalStringParam(params, "entity_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.SessionID, _, err = optionalStringParam(params, "session_id"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Component, _, err = optionalStringParam(params, "component"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Level, _, err = optionalStringParam(params, "level"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.ErrorCode, _, err = optionalStringParam(params, "error_code"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Source, _, err = optionalStringParam(params, "source"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Order, _, err = optionalStringParam(params, "order"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	if out.Since != nil && out.Until != nil && out.Since.After(*out.Until) {
		return store.OperatorRuntimeLogListOptions{}, NewInvalidParamsError(map[string]any{"field": "until", "reason": "must be at or after since"})
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 1000); err != nil {
		return store.OperatorRuntimeLogListOptions{}, err
	}
	return out, nil
}

func operatorRuntimeIncidentListOptionsFromParams(params map[string]any) (store.OperatorRuntimeIncidentListOptions, error) {
	out := store.OperatorRuntimeIncidentListOptions{}
	var err error
	if out.Component, _, err = optionalStringParam(params, "component"); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Level, _, err = optionalStringParam(params, "level"); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
	if rawBool, ok := params["mcp_only"]; ok && !isEmptyParam(rawBool) {
		value, ok := rawBool.(bool)
		if !ok {
			return store.OperatorRuntimeIncidentListOptions{}, NewInvalidParamsError(map[string]any{"field": "mcp_only", "reason": "must be a boolean"})
		}
		out.MCPOnly = value
	}
	if out.SinceHours, err = boundedIntegerParam(params, "since_hours", 1, 720); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 500); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
	return out, nil
}

func runHeaderListOptionsFromParams(params map[string]any) (store.RunHeaderListOptions, error) {
	out := store.RunHeaderListOptions{}
	status, _, err := optionalStringParam(params, "status")
	if err != nil {
		return store.RunHeaderListOptions{}, err
	}
	status = strings.ToLower(status)
	if status != "" {
		if _, ok := runListStatuses[status]; !ok {
			return store.RunHeaderListOptions{}, NewInvalidParamsError(map[string]any{"field": "status", "reason": "must be a valid RunStatus"})
		}
		out.Status = status
	}
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return store.RunHeaderListOptions{}, err
	}
	out.Cursor = cursor
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return store.RunHeaderListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return store.RunHeaderListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return store.RunHeaderListOptions{}, err
	}
	return out, nil
}

func timestampParam(params map[string]any, name string) (*time.Time, error) {
	raw, present, err := optionalStringParam(params, name)
	if err != nil {
		return nil, err
	}
	if !present || raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be RFC3339 timestamp"})
	}
	value := parsed.UTC()
	return &value, nil
}

func optionalStringParam(params map[string]any, name string) (string, bool, error) {
	if params == nil {
		return "", false, nil
	}
	value, ok := params[name]
	if !ok || isEmptyParam(value) {
		return "", ok, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", true, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a string"})
	}
	return strings.TrimSpace(text), true, nil
}

func requiredStringParam(params map[string]any, name string) (string, error) {
	value, present, err := optionalStringParam(params, name)
	if err != nil {
		return "", err
	}
	if !present || value == "" {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
	}
	return value, nil
}

func optionalBoolParam(params map[string]any, name string, defaultValue bool) (bool, error) {
	if params == nil {
		return defaultValue, nil
	}
	value, ok := params[name]
	if !ok || isEmptyParam(value) {
		return defaultValue, nil
	}
	boolValue, ok := value.(bool)
	if !ok {
		return false, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a boolean"})
	}
	return boolValue, nil
}

func stringParam(params map[string]any, name string) string {
	if params == nil {
		return ""
	}
	value, _ := params[name].(string)
	return strings.TrimSpace(value)
}

func integerParam(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		if math.Trunc(typed) != typed {
			return 0, false
		}
		return int(typed), true
	default:
		return 0, false
	}
}

func requiredBoundedIntegerParam(params map[string]any, name string, minValue, maxValue int) (int, error) {
	if params == nil {
		return 0, NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
	}
	raw, ok := params[name]
	if !ok || isEmptyParam(raw) {
		return 0, NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
	}
	value, ok := integerParam(raw)
	if !ok || value < minValue || value > maxValue {
		return 0, NewInvalidParamsError(map[string]any{
			"field":  name,
			"reason": fmt.Sprintf("must be an integer from %d to %d", minValue, maxValue),
		})
	}
	return value, nil
}

func boundedIntegerParam(params map[string]any, name string, minValue, maxValue int) (int, error) {
	if params == nil {
		return 0, nil
	}
	raw, ok := params[name]
	if !ok || isEmptyParam(raw) {
		return 0, nil
	}
	value, ok := integerParam(raw)
	if !ok || value < minValue || value > maxValue {
		return 0, NewInvalidParamsError(map[string]any{
			"field":  name,
			"reason": fmt.Sprintf("must be an integer from %d to %d", minValue, maxValue),
		})
	}
	return value, nil
}
