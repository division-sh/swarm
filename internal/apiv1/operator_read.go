package apiv1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type Pinger interface {
	Ping(context.Context) error
}

type RunReadStore = operatorread.RunReader
type ObservabilityReadStore = operatorread.ObservabilityReader
type EntityReadStore = operatorread.EntityReader

type AgentIdentityResolver interface {
	ResolveOperatorAgentIdentity(context.Context, string, string) (agentidentity.Identity, error)
}

type AgentReadStore = operatorread.AgentReader
type ConversationReadStore = operatorread.ConversationReader
type AgentDeliveryLifecycleReadStore = operatorread.AgentDeliveryLifecycleReader
type AgentUsageReadStore = operatorread.AgentUsageReader

type BundleCatalogReadStore interface {
	ListBundleCatalog(context.Context, bundlecatalog.ListOptions) (bundlecatalog.ListResult, error)
	LoadBundleCatalog(context.Context, string) (bundlecatalog.Detail, error)
	ListBundleCatalogAgents(context.Context, string, bundlecatalog.AgentListOptions) (bundlecatalog.AgentsResult, error)
}

type healthPingResult struct {
	OK bool   `json:"ok"`
	TS string `json:"ts"`
}

type healthCheckResult struct {
	Alive            bool                            `json:"alive"`
	Ready            bool                            `json:"ready"`
	DBOK             bool                            `json:"db_ok"`
	RuntimeOK        bool                            `json:"runtime_ok"`
	ExecutionPosture executionposture.Posture        `json:"execution_posture"`
	Bundle           runtimecontracts.BundleIdentity `json:"bundle"`
}

type RuntimeIdentityResult struct {
	RuntimeInstanceID   string   `json:"runtime_instance_id"`
	StartedAt           string   `json:"started_at"`
	APIVersion          string   `json:"api_version"`
	SupportedTransports []string `json:"supported_transports"`
}

type runGetResult struct {
	Run operatorread.RunHeader `json:"run"`
}

type runListResult struct {
	Runs       []operatorread.RunHeader `json:"runs"`
	NextCursor string                   `json:"next_cursor,omitempty"`
}

type runTraceListResult struct {
	Trace      []operatorread.RunDebugTraceRow `json:"trace"`
	NextCursor string                          `json:"next_cursor,omitempty"`
}

type runDiagnosis struct {
	Run              operatorread.RunHeader                 `json:"run"`
	OperationalState string                                 `json:"operational_state"`
	BlockingLayer    string                                 `json:"blocking_layer"`
	BlockingReason   string                                 `json:"blocking_reason"`
	Heuristics       []string                               `json:"heuristics"`
	FailedDeliveries []operatorread.RunDebugFailureDelivery `json:"failed_deliveries"`
	TestQuiescence   operatorread.RunTestQuiescence         `json:"test_quiescence"`
}

var runListStatuses = map[string]struct{}{
	"running":   {},
	"paused":    {},
	"completed": {},
	"failed":    {},
	"cancelled": {},
	"forked":    {},
}

func OperatorHealthHandlers(opts HealthHandlerOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	ready := opts.Ready
	if ready == nil {
		ready = func() bool { return false }
	}
	return map[string]MethodHandler{
		"health.ping": func(context.Context, Request) (any, error) {
			return healthPingResult{OK: true, TS: now().UTC().Format(time.RFC3339Nano)}, nil
		},
		"health.check": func(ctx context.Context, _ Request) (any, error) {
			return operatorHealthSnapshot(ctx, ready, opts.Database, opts.Bundle, opts.ExecutionPosture), nil
		},
	}
}

func OperatorRuntimeIdentityHandlers(opts RuntimeIdentityHandlerOptions) map[string]MethodHandler {
	return map[string]MethodHandler{
		"runtime.identity": func(context.Context, Request) (any, error) {
			identity := opts.Identity
			if strings.TrimSpace(identity.RuntimeInstanceID) == "" {
				return nil, fmt.Errorf("runtime identity is not configured")
			}
			if identity.SupportedTransports == nil {
				identity.SupportedTransports = []string{}
			}
			return identity, nil
		},
	}
}

func OperatorRunReadHandlers(opts RunReadHandlerOptions) map[string]MethodHandler {
	return map[string]MethodHandler{
		"run.get": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			header, err := runs.LoadRunHeader(ctx, runID)
			if errors.Is(err, operatorread.ErrRunNotFound) {
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
			if errors.Is(err, operatorread.ErrInvalidRunListCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid run list cursor"})
			}
			if err != nil {
				return nil, err
			}
			if headers == nil {
				headers = []operatorread.RunHeader{}
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
			if errors.Is(err, operatorread.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			report, err := runs.LoadRunDebugReport(ctx, runID, operatorread.RunDebugQueryOptions{})
			if errors.Is(err, operatorread.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			status := operatorread.ProjectRunOperationalStatus(report)
			failedDeliveries := report.FailedDeliveries
			if failedDeliveries == nil {
				failedDeliveries = []operatorread.RunDebugFailureDelivery{}
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
}

func normalizeRunTestQuiescence(value operatorread.RunTestQuiescence) operatorread.RunTestQuiescence {
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

func requireAgentReadStore(reads AgentReadStore) (AgentReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("agent read store is required")
	}
	return reads, nil
}

func requireConversationReadStore(reads ConversationReadStore) (ConversationReadStore, error) {
	if reads == nil {
		return nil, fmt.Errorf("conversation read store is required")
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

func OperatorBundleCatalogHandlers(opts BundleCatalogHandlerOptions) map[string]MethodHandler {
	if opts.Catalog == nil {
		return nil
	}
	return map[string]MethodHandler{
		"bundle.list": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.Catalog)
			if err != nil {
				return nil, err
			}
			listOpts, err := bundleCatalogListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListBundleCatalog(ctx, listOpts)
			if errors.Is(err, bundlecatalog.ErrInvalidCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid bundle catalog cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"bundle.get": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.Catalog)
			if err != nil {
				return nil, err
			}
			bundleHash, err := requiredBundleHashParam(req.Params, "bundle_hash")
			if err != nil {
				return nil, err
			}
			result, err := reads.LoadBundleCatalog(ctx, bundleHash)
			if errors.Is(err, bundlecatalog.ErrNotFound) {
				return nil, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		"bundle.agents": func(ctx context.Context, req Request) (any, error) {
			reads, err := requireBundleCatalogReadStore(opts.Catalog)
			if err != nil {
				return nil, err
			}
			bundleHash, err := requiredBundleHashParam(req.Params, "bundle_hash")
			if err != nil {
				return nil, err
			}
			listOpts, err := bundleCatalogAgentListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := reads.ListBundleCatalogAgents(ctx, bundleHash, listOpts)
			if errors.Is(err, bundlecatalog.ErrNotFound) {
				return nil, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
			}
			if errors.Is(err, bundlecatalog.ErrInvalidCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid bundle agents cursor"})
			}
			var tooLarge *bundlecatalog.AgentDefinitionTooLargeError
			if errors.As(err, &tooLarge) {
				return nil, NewApplicationError(BundleAgentDefinitionTooLargeCode, false, map[string]any{
					"bundle_hash":         tooLarge.BundleHash,
					"agent_name_owner":    tooLarge.AgentNameOwner,
					"agent_id":            tooLarge.AgentID,
					"encoded_row_bytes":   tooLarge.EncodedRowBytes,
					"result_byte_ceiling": tooLarge.ResultByteCeiling,
				})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func OperatorAgentConversationHandlers(opts AgentConversationHandlerOptions) map[string]MethodHandler {
	handlers := map[string]MethodHandler{}
	usageHandler := func(ctx context.Context, req Request) (any, error) {
		reads, err := requireAgentUsageReadStore(opts.Usage)
		if err != nil {
			return nil, err
		}
		agentID, err := requiredStringParam(req.Params, "agent_id")
		if err != nil {
			return nil, err
		}
		identity, err := resolveOperatorAgentIdentityParam(ctx, reads, req.Params, agentID)
		if err != nil {
			return nil, err
		}
		usageOpts, err := operatorAgentUsageOptionsFromParams(req.Params)
		if err != nil {
			return nil, err
		}
		result, err := reads.LoadOperatorAgentUsage(ctx, identity, usageOpts)
		if errors.Is(err, operatorread.ErrAgentNotFound) {
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
		reads, err := requireAgentDeliveryLifecycleReadStore(opts.DeliveryLifecycle)
		if err != nil {
			return nil, err
		}
		agentID, err := requiredStringParam(req.Params, "agent_id")
		if err != nil {
			return nil, err
		}
		identity, err := resolveOperatorAgentIdentityParam(ctx, reads, req.Params, agentID)
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
		limit, err := boundedIntegerParam(req.Params, "limit", 1, operatorread.MaxAgentDeliveryLifecycleLimit)
		if err != nil {
			return nil, err
		}
		cursor, _, err := optionalStringParam(req.Params, "cursor")
		if err != nil {
			return nil, err
		}
		result, err := reads.LoadOperatorAgentDeliveryLifecycle(ctx, identity, operatorread.OperatorAgentDeliveryLifecycleOptions{
			RunID:    runID,
			Statuses: statuses,
			Limit:    limit,
			Cursor:   cursor,
		})
		if errors.Is(err, operatorread.ErrAgentNotFound) {
			return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
		}
		if errors.Is(err, operatorread.ErrInvalidAgentDeliveryLifecycleCursor) {
			return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid agent.delivery_lifecycle cursor"})
		}
		if errors.Is(err, operatorread.ErrInvalidAgentDeliveryLifecycleStatus) {
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
	if opts.DeliveryLifecycle != nil {
		handlers["agent.delivery_lifecycle"] = lifecycleHandler
	}
	if opts.Agents != nil || opts.Conversations != nil {
		for name, handler := range map[string]MethodHandler{
			"agent.list": func(ctx context.Context, req Request) (any, error) {
				reads, err := requireAgentReadStore(opts.Agents)
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
				reads, err := requireAgentReadStore(opts.Agents)
				if err != nil {
					return nil, err
				}
				agentID, err := requiredStringParam(req.Params, "agent_id")
				if err != nil {
					return nil, err
				}
				identity, err := resolveOperatorAgentIdentityParam(ctx, reads, req.Params, agentID)
				if err != nil {
					return nil, err
				}
				result, err := reads.LoadOperatorAgent(ctx, identity)
				if errors.Is(err, operatorread.ErrAgentNotFound) {
					return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
				}
				if err != nil {
					return nil, err
				}
				return result, nil
			},
			"agent.diagnose": func(ctx context.Context, req Request) (any, error) {
				reads, err := requireAgentReadStore(opts.Agents)
				if err != nil {
					return nil, err
				}
				agentID, err := requiredStringParam(req.Params, "agent_id")
				if err != nil {
					return nil, err
				}
				identity, err := resolveOperatorAgentIdentityParam(ctx, reads, req.Params, agentID)
				if err != nil {
					return nil, err
				}
				queueLimit, err := boundedIntegerParam(req.Params, "queue_limit", 1, operatorread.MaxAgentDiagnosisQueueLimit)
				if err != nil {
					return nil, err
				}
				queueCursor, _, err := optionalStringParam(req.Params, "queue_cursor")
				if err != nil {
					return nil, err
				}
				result, err := reads.LoadOperatorAgentDiagnosis(ctx, identity, operatorread.OperatorAgentDiagnosisOptions{
					QueueLimit:  queueLimit,
					QueueCursor: queueCursor,
				})
				if errors.Is(err, operatorread.ErrAgentNotFound) {
					return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
				}
				if errors.Is(err, operatorread.ErrInvalidPendingAgentDeliveryCursor) {
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
				reads, err := requireAgentReadStore(opts.Agents)
				if err != nil {
					return nil, err
				}
				agentID, err := requiredStringParam(req.Params, "agent_id")
				if err != nil {
					return nil, err
				}
				identity, err := resolveOperatorAgentIdentityParam(ctx, reads, req.Params, agentID)
				if err != nil {
					return nil, err
				}
				failureLimit, err := boundedIntegerParam(req.Params, "failure_limit", 1, operatorread.MaxAgentDeliveryDiagnosticsLimit)
				if err != nil {
					return nil, err
				}
				deadLetterLimit, err := boundedIntegerParam(req.Params, "dead_letter_limit", 1, operatorread.MaxAgentDeliveryDiagnosticsLimit)
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
				result, err := reads.LoadOperatorAgentDeliveryDiagnostics(ctx, identity, operatorread.OperatorAgentDeliveryDiagnosticsOptions{
					FailureLimit:     failureLimit,
					FailureCursor:    failureCursor,
					DeadLetterLimit:  deadLetterLimit,
					DeadLetterCursor: deadLetterCursor,
				})
				if errors.Is(err, operatorread.ErrAgentNotFound) {
					return nil, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
				}
				var cursorErr operatorread.AgentDeliveryDiagnosticsCursorError
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
				reads, err := requireConversationReadStore(opts.Conversations)
				if err != nil {
					return nil, err
				}
				listOpts, err := operatorConversationListOptionsFromParams(req.Params)
				if err != nil {
					return nil, err
				}
				if listOpts.AgentID != "" {
					agents, err := requireAgentReadStore(opts.Agents)
					if err != nil {
						return nil, err
					}
					identity, err := resolveOperatorAgentIdentityParam(ctx, agents, req.Params, listOpts.AgentID)
					if err != nil {
						return nil, err
					}
					listOpts.AgentID = identity.AgentID()
					listOpts.FlowInstance = identity.FlowInstance()
				}
				result, err := reads.ListOperatorConversations(ctx, listOpts)
				if errors.Is(err, operatorread.ErrInvalidConversationCursor) {
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
			"conversation.list_turns": func(ctx context.Context, req Request) (any, error) {
				reads, err := requireConversationReadStore(opts.Conversations)
				if err != nil {
					return nil, err
				}
				sessionID, err := requiredStringParam(req.Params, "session_id")
				if err != nil {
					return nil, err
				}
				limit, err := boundedIntegerParam(req.Params, "limit", 1, 500)
				if err != nil {
					return nil, err
				}
				cursor, _, err := optionalStringParam(req.Params, "cursor")
				if err != nil {
					return nil, err
				}
				result, err := reads.ListOperatorConversationTurns(ctx, operatorread.OperatorConversationTurnListOptions{SessionID: sessionID, Limit: limit, Cursor: cursor})
				if errors.Is(err, operatorread.ErrSessionNotFound) {
					return nil, NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": sessionID})
				}
				if errors.Is(err, operatorread.ErrInvalidConversationCursor) {
					return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid conversation turn cursor"})
				}
				if paramErr := entityReadParamError(err); paramErr != nil {
					return nil, NewInvalidParamsError(map[string]any{"field": paramErr.Field, "reason": paramErr.Reason})
				}
				if err != nil {
					return nil, err
				}
				return result, nil
			},
			"conversation.get_turn": func(ctx context.Context, req Request) (any, error) {
				reads, err := requireConversationReadStore(opts.Conversations)
				if err != nil {
					return nil, err
				}
				sessionID, err := requiredStringParam(req.Params, "session_id")
				if err != nil {
					return nil, err
				}
				turnID, err := requiredStringParam(req.Params, "turn_id")
				if err != nil {
					return nil, err
				}
				result, err := reads.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
				if errors.Is(err, operatorread.ErrSessionNotFound) {
					return nil, NewApplicationError(SessionNotFoundCode, false, map[string]any{"session_id": sessionID})
				}
				if errors.Is(err, operatorread.ErrTurnNotFound) {
					return nil, NewApplicationError(TurnNotFoundCode, false, map[string]any{"session_id": sessionID, "turn_id": turnID})
				}
				if err != nil {
					return nil, err
				}
				return result, nil
			},
		} {
			if strings.HasPrefix(name, "agent.") && opts.Agents == nil {
				continue
			}
			if strings.HasPrefix(name, "conversation.") && opts.Conversations == nil {
				continue
			}
			handlers[name] = handler
		}
	}
	if opts.Usage != nil {
		handlers["agent.usage"] = usageHandler
	}
	if len(handlers) == 0 {
		return nil
	}
	return handlers
}

func resolveOperatorAgentIdentityParam(ctx context.Context, resolver AgentIdentityResolver, params map[string]any, agentID string) (agentidentity.Identity, error) {
	flowInstance, _, err := optionalStringParam(params, "flow_instance")
	if err != nil {
		return agentidentity.Identity{}, err
	}
	identity, err := resolver.ResolveOperatorAgentIdentity(ctx, agentID, flowInstance)
	if errors.Is(err, operatorread.ErrAgentNotFound) {
		return agentidentity.Identity{}, NewApplicationError(AgentNotFoundCode, false, map[string]any{
			"agent_id": agentID, "flow_instance": flowInstance,
		})
	}
	if operatorread.IsAgentTargetAmbiguous(err) {
		return agentidentity.Identity{}, NewInvalidParamsError(map[string]any{
			"field":  "flow_instance",
			"reason": err.Error(),
		})
	}
	return identity, err
}

func validateAgentDiagnosisResult(item operatorread.OperatorAgentDiagnosis) error {
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
		if strings.TrimSpace(detail.DeliveryID) == "" {
			return fmt.Errorf("agent.diagnose owner returned malformed result: queue.pending_deliveries[%d].delivery_id is required", i)
		}
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

func validateAgentDeliveryDiagnosticsResult(item operatorread.OperatorAgentDeliveryDiagnostics) error {
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

func validateAgentDeliveryLifecycleListResult(item operatorread.OperatorAgentDeliveryLifecycleList) error {
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

func validateAgentDeliveryLifecycleRowResult(item operatorread.OperatorAgentDeliveryLifecycleRow, index int) error {
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

func validateAgentUsageResult(item operatorread.OperatorAgentUsage) error {
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
		if row.ExecutionMode != "live" && row.ExecutionMode != "mock" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.execution_mode=%q is invalid", prefix, row.ExecutionMode)
		}
		if strings.TrimSpace(row.CostDisplay) == "" {
			return fmt.Errorf("agent.usage owner returned malformed result: %s.cost_display is required", prefix)
		}
		switch row.UsageAccounting {
		case operatorread.AgentUsageAccountingExact, operatorread.AgentUsageAccountingEstimated:
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

func validateAgentUsageTotals(path string, totals operatorread.OperatorAgentUsageTotals) error {
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

func validateAgentDeliveryFailureResult(item operatorread.OperatorAgentDeliveryFailure, index int) error {
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

func validateAgentDeadLetterDeliveryResult(item operatorread.OperatorAgentDeadLetterDelivery, index int) error {
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
		if err := runtimefailures.ValidateEnvelope(record.Failure); err != nil {
			return fmt.Errorf("%s.failure is invalid: %w", recordPrefix, err)
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

func validateAgentDiagnosisActiveResult(item *operatorread.OperatorAgentDiagnosisActive) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: active.turn_id is required")
	}
	return nil
}

func validateAgentDiagnosisRuntimeStateResult(item *operatorread.OperatorAgentDiagnosisRuntimeState) error {
	if item == nil {
		return nil
	}
	if item.Watchdog == nil {
		return fmt.Errorf("agent.diagnose owner returned malformed result: runtime_state.watchdog is required")
	}
	watchdog := operatorread.ConversationRuntimeWatchdogDescriptor{
		State:         strings.TrimSpace(item.Watchdog.State),
		BlockingLayer: strings.TrimSpace(item.Watchdog.BlockingLayer),
		Action:        strings.TrimSpace(item.Watchdog.Action),
		Outcome:       strings.TrimSpace(item.Watchdog.Outcome),
		LastOutputAt:  strings.TrimSpace(item.Watchdog.LastOutputAt),
		RecordedAt:    strings.TrimSpace(item.Watchdog.RecordedAt),
	}
	if err := operatorread.ValidateConversationRuntimeWatchdogDescriptor(watchdog); err != nil {
		return fmt.Errorf("agent.diagnose owner returned malformed result: runtime_state.watchdog is invalid: %w", err)
	}
	return nil
}

func validateAgentDiagnosisLastToolOutcomeResult(item *operatorread.OperatorAgentLastToolOutcome) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.turn_id is required")
	}
	if strings.TrimSpace(item.ToolName) == "" {
		return fmt.Errorf("agent.diagnose owner returned malformed result: last_tool_outcome.tool_name is required")
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

func OperatorEntityHandlers(opts EntityHandlerOptions) map[string]MethodHandler {
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
			if errors.Is(err, operatorread.ErrInvalidEntityCursor) {
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
			if errors.Is(err, operatorread.ErrEntityNotFound) {
				return nil, NewApplicationError(EntityNotFoundCode, false, map[string]any{"entity_id": entityID})
			}
			if errors.Is(err, operatorread.ErrAmbiguousEntityRunID) {
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

func OperatorObservabilityHandlers(opts ObservabilityHandlerOptions) map[string]MethodHandler {
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
			rows, nextCursor, err := reads.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{
				Limit:              limit,
				Cursor:             cursor,
				Since:              since,
				Until:              until,
				Filter:             filter,
				ExcludeRuntimeLogs: !includeInternal,
			})
			if errors.Is(err, operatorread.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid run trace cursor"})
			}
			if err != nil {
				return nil, err
			}
			if rows == nil {
				rows = []operatorread.RunDebugTraceRow{}
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
			if errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
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
			if errors.Is(err, operatorread.ErrEventNotFound) {
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
			if errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
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
			if errors.Is(err, operatorread.ErrInvalidObservabilityCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid runtime incident cursor"})
			}
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}

func operatorEntityListOptionsFromParams(params map[string]any) (operatorread.OperatorEntityListOptions, error) {
	out := operatorread.OperatorEntityListOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return operatorread.OperatorEntityListOptions{}, err
	}
	if out.Flow, _, err = optionalStringParam(params, "flow"); err != nil {
		return operatorread.OperatorEntityListOptions{}, err
	}
	if out.Type, _, err = optionalStringParam(params, "type"); err != nil {
		return operatorread.OperatorEntityListOptions{}, err
	}
	if out.CurrentState, _, err = optionalStringParam(params, "current_state"); err != nil {
		return operatorread.OperatorEntityListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return operatorread.OperatorEntityListOptions{}, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return operatorread.OperatorEntityListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	return out, nil
}

func operatorEntityAggregateOptionsFromParams(params map[string]any) (operatorread.OperatorEntityAggregateOptions, error) {
	out := operatorread.OperatorEntityAggregateOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return operatorread.OperatorEntityAggregateOptions{}, err
	}
	if out.GroupBy, _, err = optionalStringParam(params, "group_by"); err != nil {
		return operatorread.OperatorEntityAggregateOptions{}, err
	}
	if out.Type, _, err = optionalStringParam(params, "type"); err != nil {
		return operatorread.OperatorEntityAggregateOptions{}, err
	}
	return out, nil
}

func operatorAgentListOptionsFromParams(params map[string]any) (operatorread.OperatorAgentListOptions, error) {
	out := operatorread.OperatorAgentListOptions{}
	var err error
	if out.Flow, _, err = optionalStringParam(params, "flow"); err != nil {
		return operatorread.OperatorAgentListOptions{}, err
	}
	if out.Role, _, err = optionalStringParam(params, "role"); err != nil {
		return operatorread.OperatorAgentListOptions{}, err
	}
	return out, nil
}

func operatorAgentUsageOptionsFromParams(params map[string]any) (operatorread.OperatorAgentUsageOptions, error) {
	out := operatorread.OperatorAgentUsageOptions{}
	var err error
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return operatorread.OperatorAgentUsageOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return operatorread.OperatorAgentUsageOptions{}, err
	}
	if out.Since != nil && out.Until != nil && !out.Since.Before(*out.Until) {
		return operatorread.OperatorAgentUsageOptions{}, NewInvalidParamsError(map[string]any{"field": "until", "reason": "must be after since"})
	}
	return out, nil
}

func operatorConversationListOptionsFromParams(params map[string]any) (operatorread.OperatorConversationListOptions, error) {
	out := operatorread.OperatorConversationListOptions{}
	var err error
	if out.AgentID, _, err = optionalStringParam(params, "agent_id"); err != nil {
		return operatorread.OperatorConversationListOptions{}, err
	}
	if out.FlowInstance, _, err = optionalStringParam(params, "flow_instance"); err != nil {
		return operatorread.OperatorConversationListOptions{}, err
	}
	if out.FlowInstance != "" && out.AgentID == "" {
		return operatorread.OperatorConversationListOptions{}, NewInvalidParamsError(map[string]any{"field": "flow_instance", "reason": "requires agent_id"})
	}
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return operatorread.OperatorConversationListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return operatorread.OperatorConversationListOptions{}, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return operatorread.OperatorConversationListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	return out, nil
}

func entityReadParamError(err error) *operatorread.EntityReadParamError {
	if err == nil {
		return nil
	}
	var paramErr *operatorread.EntityReadParamError
	if errors.As(err, &paramErr) {
		return paramErr
	}
	return nil
}

func operatorEventListOptionsFromParams(params map[string]any) (operatorread.OperatorEventListOptions, error) {
	out := operatorread.OperatorEventListOptions{}
	filter, err := eventListFilterParam(params)
	if err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	if err := requireEventListRunScope(filter); err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	out.Filter = filter
	limit, err := boundedIntegerParam(params, "limit", 1, 1000)
	if err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	out.Limit = limit
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	out.Cursor = cursor
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return operatorread.OperatorEventListOptions{}, err
	}
	return out, nil
}

func eventListFilterParam(params map[string]any) (operatorread.OperatorEventListFilter, error) {
	raw, ok := params["filter"]
	if !ok || isEmptyParam(raw) {
		return operatorread.OperatorEventListFilter{}, nil
	}
	filter, ok := raw.(map[string]any)
	if !ok {
		return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter", "reason": "must be an object"})
	}
	for name := range filter {
		if _, ok := eventListFilterFields[name]; !ok {
			return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "unknown parameter"})
		}
	}
	out := operatorread.OperatorEventListFilter{}
	var err error
	if out.RunID, _, err = optionalStringParam(filter, "run_id"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.RunID != "" && !opaqueIDPattern.MatchString(out.RunID) {
		return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.run_id", "reason": "must match OpaqueId pattern"})
	}
	if out.EntityID, _, err = optionalStringParam(filter, "entity_id"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.EntityID != "" && !opaqueIDPattern.MatchString(out.EntityID) {
		return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.entity_id", "reason": "must match OpaqueId pattern"})
	}
	if out.EventName, _, err = optionalStringParam(filter, "event_name"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.DeliveryStatus, _, err = optionalStringParam(filter, "delivery_status"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.DeliveryStatus != "" {
		if _, ok := eventListDeliveryStatuses[out.DeliveryStatus]; !ok {
			return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.delivery_status", "reason": "must be a valid DeliveryStatus"})
		}
	}
	if out.SubscriberID, _, err = optionalStringParam(filter, "subscriber_id"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.SubscriberType, _, err = optionalStringParam(filter, "subscriber_type"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if out.SubscriberType != "" {
		if _, ok := eventListSubscriberTypes[out.SubscriberType]; !ok {
			return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.subscriber_type", "reason": "must be a valid SubscriberType"})
		}
	}
	if out.ReasonCode, _, err = optionalStringParam(filter, "reason_code"); err != nil {
		return operatorread.OperatorEventListFilter{}, err
	}
	if rawBool, ok := filter["has_dead_letter"]; ok && !isEmptyParam(rawBool) {
		value, ok := rawBool.(bool)
		if !ok {
			return operatorread.OperatorEventListFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.has_dead_letter", "reason": "must be a boolean"})
		}
		out.HasDeadLetter = &value
	}
	return out, nil
}

func requireEventListRunScope(filter operatorread.OperatorEventListFilter) error {
	if strings.TrimSpace(filter.RunID) == "" {
		return NewApplicationError(EventObservationRunScopeRequiredCode, false, map[string]any{
			"field":  "filter.run_id",
			"reason": "required run scope is missing",
		})
	}
	return nil
}

func runTraceFilterParam(params map[string]any) (operatorread.RunDebugTraceFilter, error) {
	raw, ok := params["filter"]
	if !ok || isEmptyParam(raw) {
		return operatorread.RunDebugTraceFilter{}, nil
	}
	filter, ok := raw.(map[string]any)
	if !ok {
		return operatorread.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter", "reason": "must be an object"})
	}
	for name := range filter {
		if _, ok := runTraceFilterFields[name]; !ok {
			return operatorread.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter." + name, "reason": "unknown parameter"})
		}
	}
	out := operatorread.RunDebugTraceFilter{}
	var err error
	if out.EventNames, err = requiredRunTraceStringListFilter(filter, "event_name"); err != nil {
		return operatorread.RunDebugTraceFilter{}, err
	}
	if out.EntityIDs, err = requiredRunTraceStringListFilter(filter, "entity_id"); err != nil {
		return operatorread.RunDebugTraceFilter{}, err
	}
	for _, entityID := range out.EntityIDs {
		if !opaqueIDPattern.MatchString(entityID) {
			return operatorread.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.entity_id", "reason": "must contain only OpaqueId values"})
		}
	}
	if out.DeliveryStatuses, err = requiredRunTraceStringListFilter(filter, "delivery_status"); err != nil {
		return operatorread.RunDebugTraceFilter{}, err
	}
	for _, status := range out.DeliveryStatuses {
		if _, ok := eventListDeliveryStatuses[status]; !ok {
			return operatorread.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.delivery_status", "reason": "must contain only valid DeliveryStatus values"})
		}
	}
	if out.SubscriberIDs, err = requiredRunTraceStringListFilter(filter, "subscriber_id"); err != nil {
		return operatorread.RunDebugTraceFilter{}, err
	}
	if out.SubscriberTypes, err = requiredRunTraceStringListFilter(filter, "subscriber_type"); err != nil {
		return operatorread.RunDebugTraceFilter{}, err
	}
	for _, subscriberType := range out.SubscriberTypes {
		if _, ok := eventListSubscriberTypes[subscriberType]; !ok {
			return operatorread.RunDebugTraceFilter{}, NewInvalidParamsError(map[string]any{"field": "filter.subscriber_type", "reason": "must contain only valid SubscriberType values"})
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

func operatorRuntimeLogListOptionsFromParams(params map[string]any) (operatorread.OperatorRuntimeLogListOptions, error) {
	out := operatorread.OperatorRuntimeLogListOptions{}
	var err error
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.EntityID, _, err = optionalStringParam(params, "entity_id"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.SessionID, _, err = optionalStringParam(params, "session_id"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Component, _, err = optionalStringParam(params, "component"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Level, _, err = optionalStringParam(params, "level"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.ErrorCode, _, err = optionalStringParam(params, "error_code"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Source, _, err = optionalStringParam(params, "source"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Order, _, err = optionalStringParam(params, "order"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	if out.Since != nil && out.Until != nil && out.Since.After(*out.Until) {
		return operatorread.OperatorRuntimeLogListOptions{}, NewInvalidParamsError(map[string]any{"field": "until", "reason": "must be at or after since"})
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 1000); err != nil {
		return operatorread.OperatorRuntimeLogListOptions{}, err
	}
	return out, nil
}

func operatorRuntimeIncidentListOptionsFromParams(params map[string]any) (operatorread.OperatorRuntimeIncidentListOptions, error) {
	out := operatorread.OperatorRuntimeIncidentListOptions{}
	var err error
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Component, _, err = optionalStringParam(params, "component"); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Level, _, err = optionalStringParam(params, "level"); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	if rawBool, ok := params["mcp_only"]; ok && !isEmptyParam(rawBool) {
		value, ok := rawBool.(bool)
		if !ok {
			return operatorread.OperatorRuntimeIncidentListOptions{}, NewInvalidParamsError(map[string]any{"field": "mcp_only", "reason": "must be a boolean"})
		}
		out.MCPOnly = value
	}
	if out.SinceHours, err = boundedIntegerParam(params, "since_hours", 1, 720); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 500); err != nil {
		return operatorread.OperatorRuntimeIncidentListOptions{}, err
	}
	return out, nil
}

func runHeaderListOptionsFromParams(params map[string]any) (operatorread.RunHeaderListOptions, error) {
	out := operatorread.RunHeaderListOptions{}
	status, _, err := optionalStringParam(params, "status")
	if err != nil {
		return operatorread.RunHeaderListOptions{}, err
	}
	status = strings.ToLower(status)
	if status != "" {
		if _, ok := runListStatuses[status]; !ok {
			return operatorread.RunHeaderListOptions{}, NewInvalidParamsError(map[string]any{"field": "status", "reason": "must be a valid RunStatus"})
		}
		out.Status = status
	}
	cursor, _, err := optionalStringParam(params, "cursor")
	if err != nil {
		return operatorread.RunHeaderListOptions{}, err
	}
	out.Cursor = cursor
	if out.BundleHash, err = optionalBundleHashParam(params, "bundle_hash"); err != nil {
		return operatorread.RunHeaderListOptions{}, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return operatorread.RunHeaderListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return operatorread.RunHeaderListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return operatorread.RunHeaderListOptions{}, err
	}
	return out, nil
}

func bundleCatalogListOptionsFromParams(params map[string]any) (bundlecatalog.ListOptions, error) {
	out := bundlecatalog.ListOptions{}
	var err error
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return bundlecatalog.ListOptions{}, err
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, 500); err != nil {
		return bundlecatalog.ListOptions{}, err
	}
	return out, nil
}

func bundleCatalogAgentListOptionsFromParams(params map[string]any) (bundlecatalog.AgentListOptions, error) {
	out := bundlecatalog.AgentListOptions{}
	var err error
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return bundlecatalog.AgentListOptions{}, err
	}
	if out.Limit, err = boundedIntegerParam(params, "limit", 1, bundlecatalog.MaxAgentListLimit); err != nil {
		return bundlecatalog.AgentListOptions{}, err
	}
	return out, nil
}

func requiredBundleHashParam(params map[string]any, name string) (string, error) {
	value, err := requiredStringParam(params, name)
	if err != nil {
		return "", err
	}
	if err := runtimecontracts.ValidateBundleHash(value); err != nil {
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
	if err := runtimecontracts.ValidateBundleHash(value); err != nil {
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
