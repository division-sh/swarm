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

	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
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
	LoadOperatorAgentDeliveryDiagnostics(context.Context, string, store.OperatorAgentDeliveryDiagnosticsOptions) (store.OperatorAgentDeliveryDiagnostics, error)
	ListOperatorConversations(context.Context, store.OperatorConversationListOptions) (store.OperatorConversationListResult, error)
	LoadOperatorConversation(context.Context, string) (store.OperatorConversationDetail, error)
	LoadOperatorConversationTurn(context.Context, string, int) (store.OperatorConversationTurnDetail, error)
	LoadCurrentOperatorConversationForAgent(context.Context, string) (*store.OperatorConversationDetail, error)
}

type AgentDeliveryLifecycleReadStore interface {
	LoadOperatorAgentDeliveryLifecycle(context.Context, string, store.OperatorAgentDeliveryLifecycleOptions) (store.OperatorAgentDeliveryLifecycleList, error)
}

type AgentUsageReadStore interface {
	LoadOperatorAgentUsage(context.Context, string, store.OperatorAgentUsageOptions) (store.OperatorAgentUsage, error)
}

type BundleCatalogReadStore interface {
	ListBundleCatalog(context.Context, store.BundleCatalogListOptions) (store.BundleCatalogListResult, error)
	LoadBundleCatalog(context.Context, string) (store.BundleCatalogDetail, error)
	ListBundleCatalogAgents(context.Context, string) (store.BundleCatalogAgentsResult, error)
}

type BundleDeleteExecutor interface {
	Execute(context.Context, bundledelete.Request) (bundledelete.Result, error)
}

type TestSetupStore interface {
	SetupScenarioEntities(context.Context, store.ScenarioSetupRequest) (store.ScenarioSetupResult, error)
}

type OperatorReadOptions struct {
	Now                       func() time.Time
	Ready                     func() bool
	RepoRoot                  string
	PlatformSpecPath          string
	Database                  Pinger
	Runs                      RunReadStore
	Observability             ObservabilityReadStore
	Entities                  EntityReadStore
	AgentConversations        AgentConversationReadStore
	AgentDeliveryLifecycle    AgentDeliveryLifecycleReadStore
	AgentUsage                AgentUsageReadStore
	BundleCatalog             BundleCatalogReadStore
	BundleDelete              BundleDeleteExecutor
	ConversationForks         ConversationForkReadStore
	ConversationForkLifecycle ConversationForkLifecycleStore
	ForkChatExecutor          ForkChatExecutor
	RunBundleContext          RunBundleContextStore
	RunForkAvailability       RunForkAvailabilityStore
	RunFork                   RunForkExecutor
	AgentControl              AgentControlController
	Mailbox                   MailboxAPIStore
	TestSetup                 TestSetupStore
	Idempotency               APIIdempotencyStore
	Events                    EventPublisher
	RunControl                RunControlController
	RuntimeIngress            RuntimeIngressController
	RuntimeContexts           *swruntime.RuntimeContextManager
	ResetCoordinator          DestructiveResetCoordinator
	ResetQuiescer             DestructiveResetQuiescer
	ResetCleaner              DestructiveResetCleaner
	ResetContainers           DestructiveResetContainerStopper
	Source                    semanticview.Source
	MailboxDecisionRoutes     map[string]MailboxDecisionEventRoute
	Bundle                    runtimecontracts.BundleIdentity
	RuntimeIdentity           RuntimeIdentityResult
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

type RuntimeIdentityResult struct {
	RuntimeInstanceID   string   `json:"runtime_instance_id"`
	StartedAt           string   `json:"started_at"`
	APIVersion          string   `json:"api_version"`
	SupportedTransports []string `json:"supported_transports"`
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
	Run              store.RunHeader                 `json:"run"`
	OperationalState string                          `json:"operational_state"`
	BlockingLayer    string                          `json:"blocking_layer"`
	BlockingReason   string                          `json:"blocking_reason"`
	Heuristics       []string                        `json:"heuristics"`
	FailedDeliveries []store.RunDebugFailureDelivery `json:"failed_deliveries"`
	TestQuiescence   store.RunTestQuiescence         `json:"test_quiescence"`
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
		"runtime.identity": func(context.Context, Request) (any, error) {
			identity := opts.RuntimeIdentity
			if strings.TrimSpace(identity.RuntimeInstanceID) == "" {
				return nil, fmt.Errorf("runtime identity is not configured")
			}
			if identity.SupportedTransports == nil {
				identity.SupportedTransports = []string{}
			}
			return identity, nil
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
			failedDeliveries := report.FailedDeliveries
			if failedDeliveries == nil {
				failedDeliveries = []store.RunDebugFailureDelivery{}
			}
			return runDiagnosis{
				Run:              header,
				OperationalState: strings.TrimSpace(status.State),
				BlockingLayer:    strings.TrimSpace(status.BlockingLayer),
				BlockingReason:   strings.TrimSpace(status.BlockingReason),
				Heuristics:       status.Heuristics,
				FailedDeliveries: failedDeliveries,
				TestQuiescence:   normalizeRunTestQuiescence(report.TestQuiescence),
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
	for name, handler := range OperatorTestSetupHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorEventReplayHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorRunForkHandlers(opts) {
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
	for name, handler := range OperatorBundleCatalogHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorBundleRegisterHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorBundleDeleteHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorConversationForkHandlers(opts) {
		handlers[name] = handler
	}
	for name, handler := range OperatorAgentControlHandlers(opts) {
		handlers[name] = handler
	}
	return handlers
}

func normalizeRunTestQuiescence(value store.RunTestQuiescence) store.RunTestQuiescence {
	value.Ready = value.ActiveDeliveries == 0 &&
		value.UnsettledPipelineEvents == 0 &&
		value.DueTimers == 0 &&
		value.ActiveSessionLeases == 0
	return value
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

func requireAgentDeliveryLifecycleReadStore(reads AgentDeliveryLifecycleReadStore) (AgentDeliveryLifecycleReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("agent delivery lifecycle read store is required")
	}
	return reads, nil
}

func requireAgentUsageReadStore(reads AgentUsageReadStore) (AgentUsageReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("agent usage read store is required")
	}
	return reads, nil
}

func requireBundleCatalogReadStore(reads BundleCatalogReadStore) (BundleCatalogReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("bundle catalog read store is required")
	}
	return reads, nil
}

func OperatorBundleCatalogHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.BundleCatalog == nil {
		return nil
	}
	return map[string]MethodHandler{
		"bundle.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.BundleCatalog)
			if err != nil {
				return nil, err
			}
			listOpts, err := bundleCatalogListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListBundleCatalog(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidBundleCatalogCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid bundle catalog cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"bundle.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.BundleCatalog)
			if err != nil {
				return nil, err
			}
			bundleHash, err := requiredBundleHashParam(req.Params, "bundle_hash")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadBundleCatalog(ctx, bundleHash)
			if errors.Is(err, store.ErrBundleNotFound) {
				return nil, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"bundle.agents": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.BundleCatalog)
			if err != nil {
				return nil, err
			}
			bundleHash, err := requiredBundleHashParam(req.Params, "bundle_hash")
			if err != nil {
				return nil, err
			}
			result, err := reads.ListBundleCatalogAgents(ctx, bundleHash)
			if errors.Is(err, store.ErrBundleNotFound) {
				return nil, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func OperatorAgentConversationHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	handlers := map[string]MethodHandler{}
	usageHandler := func(ctx context.Context, req Request) (any, error) {
		reads, err := requireAgentUsageReadStore(opts.AgentUsage)
		if err != nil {
			return nil, err
		}
		agentID, err := requiredStringParam(req.Params, "agent_id")
		if err != nil {
			return nil, err
		}
		usageOpts, err := operatorAgentUsageOptionsFromParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := reads.LoadOperatorAgentUsage(ctx, agentID, usageOpts)
		if errors.Is(err, store.ErrAgentNotFound) {
			return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
		}
		if err != nil {
			return nil, err
		}
		if err := validateAgentUsageResult(result); err != nil {
			return nil, err
		}
		return result, nil
	}
	lifecycleHandler := func(ctx context.Context, req Request) (any, error) {
		reads, err := requireAgentDeliveryLifecycleReadStore(opts.AgentDeliveryLifecycle)
		if err != nil {
			return nil, err
		}
		agentID, err := requiredStringParam(req.Params, "agent_id")
		if err != nil {
			return nil, err
		}
		runID, _, err := optionalStringParam(req.Params, "run_id")
		if err != nil {
			return nil, err
		}
		if runID != "" && !opaqueIDPattern.MatchString(runID) {
			return nil, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must match OpaqueId pattern"})
		}
		statuses, _, err := optionalStringListParam(req.Params, "delivery_status")
		if err != nil {
			return nil, err
		}
		for _, status := range statuses {
			if _, ok := eventListDeliveryStatuses[status]; !ok {
				return nil, NewInvalidParamsError(map[string]any{"field": "delivery_status", "reason": "must contain only valid DeliveryStatus values"})
			}
		}
		limit, err := boundedIntegerParam(req.Params, "limit", 1, store.MaxAgentDeliveryLifecycleLimit)
		if err != nil {
			return nil, err
		}
		cursor, _, err := optionalStringParam(req.Params, "cursor")
		if err != nil {
			return nil, err
		}
		result, err := reads.LoadOperatorAgentDeliveryLifecycle(ctx, agentID, store.OperatorAgentDeliveryLifecycleOptions{
			RunID:    runID,
			Statuses: statuses,
			Limit:    limit,
			Cursor:   cursor,
		})
		if errors.Is(err, store.ErrAgentNotFound) {
			return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
		}
		if errors.Is(err, store.ErrInvalidAgentDeliveryLifecycleCursor) {
			return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid agent.delivery_lifecycle cursor"})
		}
		if errors.Is(err, store.ErrInvalidAgentDeliveryLifecycleStatus) {
			return nil, NewInvalidParamsError(map[string]any{"field": "delivery_status", "reason": "must contain only valid DeliveryStatus values"})
		}
		if err != nil {
			return nil, err
		}
		if err := validateAgentDeliveryLifecycleListResult(result); err != nil {
			return nil, err
		}
		return result, nil
	}
	if opts.AgentDeliveryLifecycle != nil {
		handlers["agent.delivery_lifecycle"] = lifecycleHandler
	}
	if opts.AgentConversations != nil {
		for name, handler := range map[string]MethodHandler{
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
			"agent.delivery_diagnostics": func(ctx context.Context, req Request) (any, error) {
				reads, err := requireAgentConversationReadStore(opts.AgentConversations)
				if err != nil {
					return nil, err
				}
				agentID, err := requiredStringParam(req.Params, "agent_id")
				if err != nil {
					return nil, err
				}
				failureLimit, err := boundedIntegerParam(req.Params, "failure_limit", 1, store.MaxAgentDeliveryDiagnosticsLimit)
				if err != nil {
					return nil, err
				}
				deadLetterLimit, err := boundedIntegerParam(req.Params, "dead_letter_limit", 1, store.MaxAgentDeliveryDiagnosticsLimit)
				if err != nil {
					return nil, err
				}
				failureCursor, _, err := optionalStringParam(req.Params, "failure_cursor")
				if err != nil {
					return nil, err
				}
				deadLetterCursor, _, err := optionalStringParam(req.Params, "dead_letter_cursor")
				if err != nil {
					return nil, err
				}
				result, err := reads.LoadOperatorAgentDeliveryDiagnostics(ctx, agentID, store.OperatorAgentDeliveryDiagnosticsOptions{
					FailureLimit:     failureLimit,
					FailureCursor:    failureCursor,
					DeadLetterLimit:  deadLetterLimit,
					DeadLetterCursor: deadLetterCursor,
				})
				if errors.Is(err, store.ErrAgentNotFound) {
					return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
				}
				var cursorErr store.AgentDeliveryDiagnosticsCursorError
				if errors.As(err, &cursorErr) {
					field := strings.TrimSpace(cursorErr.Field)
					if field == "" {
						field = "cursor"
					}
					return nil, NewInvalidParamsError(map[string]any{"field": field, "reason": "invalid agent.delivery_diagnostics cursor"})
				}
				if err != nil {
					return nil, err
				}
				if err := validateAgentDeliveryDiagnosticsResult(result); err != nil {
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
		} {
			handlers[name] = handler
		}
	}
	if opts.AgentUsage != nil {
		handlers["agent.usage"] = usageHandler
	}
	if len(handlers) == 0 {
		return nil
	}
	return handlers
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

func validateAgentDeliveryDiagnosticsResult(item store.OperatorAgentDeliveryDiagnostics) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent.delivery_diagnostics owner returned malformed result: agent_id is required")
	}
	if item.Summary.Failures24h < 0 {
		return fmt.Errorf("agent.delivery_diagnostics owner returned malformed result: summary.failures_24h must be non-negative")
	}
	if item.Summary.DeadLetters24h < 0 {
		return fmt.Errorf("agent.delivery_diagnostics owner returned malformed result: summary.dead_letters_24h must be non-negative")
	}
	if item.Failures == nil {
		return fmt.Errorf("agent.delivery_diagnostics owner returned malformed result: failures must be an array")
	}
	for i, failure := range item.Failures {
		if err := validateAgentDeliveryFailureResult(failure, i); err != nil {
			return err
		}
	}
	if item.DeadLetters == nil {
		return fmt.Errorf("agent.delivery_diagnostics owner returned malformed result: dead_letters must be an array")
	}
	for i, deadLetter := range item.DeadLetters {
		if err := validateAgentDeadLetterDeliveryResult(deadLetter, i); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentDeliveryLifecycleListResult(item store.OperatorAgentDeliveryLifecycleList) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent.delivery_lifecycle owner returned malformed result: agent_id is required")
	}
	if item.Deliveries == nil {
		return fmt.Errorf("agent.delivery_lifecycle owner returned malformed result: deliveries must be an array")
	}
	for i, delivery := range item.Deliveries {
		if err := validateAgentDeliveryLifecycleRowResult(delivery, i); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentDeliveryLifecycleRowResult(item store.OperatorAgentDeliveryLifecycleRow, index int) error {
	prefix := fmt.Sprintf("agent.delivery_lifecycle owner returned malformed result: deliveries[%d]", index)
	if strings.TrimSpace(item.DeliveryID) == "" {
		return fmt.Errorf("%s.delivery_id is required", prefix)
	}
	if strings.TrimSpace(item.EventID) == "" {
		return fmt.Errorf("%s.event_id is required", prefix)
	}
	if strings.TrimSpace(item.EventName) == "" {
		return fmt.Errorf("%s.event_name is required", prefix)
	}
	if _, ok := eventListDeliveryStatuses[strings.TrimSpace(item.Status)]; !ok {
		return fmt.Errorf("%s.status=%q is not a valid DeliveryStatus", prefix, item.Status)
	}
	if item.RetryCount < 0 {
		return fmt.Errorf("%s.retry_count must be non-negative", prefix)
	}
	if item.DeliveryCreatedAt.IsZero() {
		return fmt.Errorf("%s.delivery_created_at is required", prefix)
	}
	return nil
}

func validateAgentUsageResult(item store.OperatorAgentUsage) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent.usage owner returned malformed result: agent_id is required")
	}
	if item.Window.Since != nil && item.Window.Until != nil && !item.Window.Since.Before(*item.Window.Until) {
		return fmt.Errorf("agent.usage owner returned malformed result: window.until must be after window.since")
	}
	if err := validateAgentUsageTotals("usage.exact", item.Usage.Exact); err != nil {
		return err
	}
	if err := validateAgentUsageTotals("usage.estimated", item.Usage.Estimated); err != nil {
		return err
	}
	if item.Breakdown == nil {
		return fmt.Errorf("agent.usage owner returned malformed result: breakdown must be an array")
	}
	for i, row := range item.Breakdown {
		prefix := fmt.Sprintf("breakdown[%d]", i)
		switch row.UsageAccounting {
		case store.AgentUsageAccountingExact, store.AgentUsageAccountingEstimated:
		default:
			return fmt.Errorf("agent.usage owner returned malformed result: %s.usage_accounting=%q is invalid", prefix, row.UsageAccounting)
		}
		if strings.TrimSpace(row.InvocationType) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.invocation_type is required", prefix)
		}
		if strings.TrimSpace(row.Model) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.model is required", prefix)
		}
		if strings.TrimSpace(row.ModelAlias) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.model_alias is required", prefix)
		}
		if strings.TrimSpace(row.BackendProfile) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.backend_profile is required", prefix)
		}
		if strings.TrimSpace(row.Provider) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.provider is required", prefix)
		}
		if strings.TrimSpace(row.Transport) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.transport is required", prefix)
		}
		if strings.TrimSpace(row.ResolvedModel) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.resolved_model is required", prefix)
		}
		if err := validateAgentUsageTotals(prefix+".totals", row.Totals); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentUsageTotals(path string, totals store.OperatorAgentUsageTotals) error {
	if totals.LedgerEntries < 0 {
		return fmt.Errorf("agent.usage owner returned malformed result: %s.ledger_entries must be non-negative", path)
	}
	if totals.InputTokens < 0 {
		return fmt.Errorf("agent.usage owner returned malformed result: %s.input_tokens must be non-negative", path)
	}
	if totals.OutputTokens < 0 {
		return fmt.Errorf("agent.usage owner returned malformed result: %s.output_tokens must be non-negative", path)
	}
	if totals.EstimatedCostUSD < 0 {
		return fmt.Errorf("agent.usage owner returned malformed result: %s.estimated_cost_usd must be non-negative", path)
	}
	return nil
}

func validateAgentDeliveryFailureResult(item store.OperatorAgentDeliveryFailure, index int) error {
	prefix := fmt.Sprintf("agent.delivery_diagnostics owner returned malformed result: failures[%d]", index)
	if strings.TrimSpace(item.DeliveryID) == "" {
		return fmt.Errorf("%s.delivery_id is required", prefix)
	}
	if strings.TrimSpace(item.EventID) == "" {
		return fmt.Errorf("%s.event_id is required", prefix)
	}
	if strings.TrimSpace(item.EventName) == "" {
		return fmt.Errorf("%s.event_name is required", prefix)
	}
	if strings.TrimSpace(item.Status) != "failed" {
		return fmt.Errorf("%s.status must be failed", prefix)
	}
	if item.RetryCount < 0 {
		return fmt.Errorf("%s.retry_count must be non-negative", prefix)
	}
	if item.OccurredAt.IsZero() {
		return fmt.Errorf("%s.occurred_at is required", prefix)
	}
	return nil
}

func validateAgentDeadLetterDeliveryResult(item store.OperatorAgentDeadLetterDelivery, index int) error {
	prefix := fmt.Sprintf("agent.delivery_diagnostics owner returned malformed result: dead_letters[%d]", index)
	if strings.TrimSpace(item.DeliveryID) == "" {
		return fmt.Errorf("%s.delivery_id is required", prefix)
	}
	if strings.TrimSpace(item.EventID) == "" {
		return fmt.Errorf("%s.event_id is required", prefix)
	}
	if strings.TrimSpace(item.EventName) == "" {
		return fmt.Errorf("%s.event_name is required", prefix)
	}
	if strings.TrimSpace(item.Status) != "dead_letter" {
		return fmt.Errorf("%s.status must be dead_letter", prefix)
	}
	if item.RetryCount < 0 {
		return fmt.Errorf("%s.retry_count must be non-negative", prefix)
	}
	if item.OccurredAt.IsZero() {
		return fmt.Errorf("%s.occurred_at is required", prefix)
	}
	if len(item.DeadLetterRecords) == 0 {
		return fmt.Errorf("%s.dead_letter_records must contain at least one record", prefix)
	}
	for i, record := range item.DeadLetterRecords {
		recordPrefix := fmt.Sprintf("%s.dead_letter_records[%d]", prefix, i)
		if strings.TrimSpace(record.DeadLetterID) == "" {
			return fmt.Errorf("%s.dead_letter_id is required", recordPrefix)
		}
		if strings.TrimSpace(record.FailureType) == "" {
			return fmt.Errorf("%s.failure_type is required", recordPrefix)
		}
		if record.RetryCount < 0 {
			return fmt.Errorf("%s.retry_count must be non-negative", recordPrefix)
		}
		if record.ChainDepth < 0 {
			return fmt.Errorf("%s.chain_depth must be non-negative", recordPrefix)
		}
		if record.CreatedAt.IsZero() {
			return fmt.Errorf("%s.created_at is required", recordPrefix)
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
			until, err := timestampParam(req.Params, "until")
			if err != nil {
				return nil, err
			}
			if since != nil && until != nil && since.After(*until) {
				return nil, NewInvalidParamsError(map[string]any{"field": "until", "reason": "must be at or after since"})
			}
			filter, err := runTraceFilterParam(req.Params)
			if err != nil {
				return nil, err
			}
			includeInternal, err := optionalBoolParam(req.Params, "include_internal", false)
			if err != nil {
				return nil, err
			}
			rows, nextCursor, err := reads.LoadRunDebugTracePage(ctx, runID, store.RunDebugTraceQueryOptions{
				Limit:              limit,
				Cursor:             cursor,
				Since:              since,
				Until:              until,
				Filter:             filter,
				ExcludeRuntimeLogs: !includeInternal,
			})
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

func operatorAgentUsageOptionsFromParams(params map[string]any) (store.OperatorAgentUsageOptions, error) {
	out := store.OperatorAgentUsageOptions{}
	var err error
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return store.OperatorAgentUsageOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return store.OperatorAgentUsageOptions{}, err
	}
	if out.Since != nil && out.Until != nil && !out.Since.Before(*out.Until) {
		return store.OperatorAgentUsageOptions{}, NewInvalidParamsError(map[string]any{"field": "until", "reason": "must be after since"})
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
	if err := requireEventListRunScope(filter); err != nil {
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

func requireEventListRunScope(filter store.OperatorEventListFilter) error {
	if strings.TrimSpace(filter.RunID) == "" {
		return NewInvalidParamsError(map[string]any{"field": "filter.run_id", "reason": "required run scope is missing"})
	}
	return nil
}

func runTraceFilterParam(params map[string]any) (store.RunDebugTraceFilter, error) {
	raw, ok := params["filter"]
	if !ok || isEmptyParam(raw) {
		return store.RunDebugTraceFilter{}, nil
	}
	filter, ok := raw.(map[string]any)
	if !ok {
		return store.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter", "reason": "must be an object"})
	}
	for name := range filter {
		if _, ok := runTraceFilterFields[name]; !ok {
			return store.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "unknown parameter"})
		}
	}
	out := store.RunDebugTraceFilter{}
	var err error
	if out.EventNames, err = requiredRunTraceStringListFilter(filter, "event_name"); err != nil {
		return store.RunDebugTraceFilter{}, err
	}
	if out.EntityIDs, err = requiredRunTraceStringListFilter(filter, "entity_id"); err != nil {
		return store.RunDebugTraceFilter{}, err
	}
	for _, entityID := range out.EntityIDs {
		if !opaqueIDPattern.MatchString(entityID) {
			return store.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.entity_id", "reason": "must contain only OpaqueId values"})
		}
	}
	if out.DeliveryStatuses, err = requiredRunTraceStringListFilter(filter, "delivery_status"); err != nil {
		return store.RunDebugTraceFilter{}, err
	}
	for _, status := range out.DeliveryStatuses {
		if _, ok := eventListDeliveryStatuses[status]; !ok {
			return store.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.delivery_status", "reason": "must contain only valid DeliveryStatus values"})
		}
	}
	if out.SubscriberIDs, err = requiredRunTraceStringListFilter(filter, "subscriber_id"); err != nil {
		return store.RunDebugTraceFilter{}, err
	}
	if out.SubscriberTypes, err = requiredRunTraceStringListFilter(filter, "subscriber_type"); err != nil {
		return store.RunDebugTraceFilter{}, err
	}
	for _, subscriberType := range out.SubscriberTypes {
		if _, ok := eventListSubscriberTypes[subscriberType]; !ok {
			return store.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.subscriber_type", "reason": "must contain only valid SubscriberType values"})
		}
	}
	return out, nil
}

func requiredRunTraceStringListFilter(filter map[string]any, name string) ([]string, error) {
	values, present, err := optionalStringListParam(filter, name)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "must be a non-empty array of strings"})
	}
	if present && len(values) == 0 {
		return nil, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "must be a non-empty array of strings"})
	}
	return values, nil
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

var runTraceFilterFields = map[string]struct{}{
	"event_name":      {},
	"entity_id":       {},
	"delivery_status": {},
	"subscriber_id":   {},
	"subscriber_type": {},
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
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
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
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return store.OperatorRuntimeIncidentListOptions{}, err
	}
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
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return store.RunHeaderListOptions{}, err
	}
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

func bundleCatalogListOptionsFromParams(params map[string]any) (store.BundleCatalogListOptions, error) {
	out := store.BundleCatalogListOptions{}
	var err error
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return store.BundleCatalogListOptions{}, err
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 500); err != nil {
		return store.BundleCatalogListOptions{}, err
	}
	return out, nil
}

func requiredBundleHashParam(params map[string]any, name string) (string, error) {
	value, err := requiredStringParam(params, name)
	if err != nil {
		return "", err
	}
	if !bundleHashPattern.MatchString(value) {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "must be bundle-v1:sha256:<64 lowercase hex>"})
	}
	return value, nil
}

func optionalBundleHashParam(params map[string]any, name string) (string, error) {
	value, _, err := optionalStringParam(params, name)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", nil
	}
	if !bundleHashPattern.MatchString(value) {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "must be bundle-v1:sha256:<64 lowercase hex>"})
	}
	return value, nil
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
