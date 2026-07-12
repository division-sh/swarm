package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type ClaudeCLIRuntime struct {
	cfg                 *config.Config
	sessions            sessions.Registry
	turns               TurnPersistence
	conversations       ConversationPersistence
	budget              BudgetGuard
	lockOwner           string
	workspaces          workspace.Resolver
	monitor             MonitorSink
	events              EventPublisher
	mcpTurns            MCPTurnContextStore
	toolGateway         toolgateway.Binding
	providerCredentials ProviderCredentialResolver
	execWorkspaceFn     func(ctx context.Context, target *workspace.Target, stdin string, args ...string) ([]byte, []byte, int, error)
	providerAdmission   *ProviderAdmissionRegistry
}

var ErrClaudeWorkspaceRequired = errors.New("claude workspace target required")
var ErrClaudeWorkspaceCLIUnavailable = errors.New("claude workspace cli unavailable")

type promptTransportFallback struct {
	Attempted bool
	Used      bool
}

type ClaudeCLIRuntimeOptions struct {
	MonitorSink         MonitorSink
	MCPTurnContextStore MCPTurnContextStore
	ToolGateway         toolgateway.Binding
	ProviderCredentials ProviderCredentialResolver
}

func NewClaudeCLIRuntime(
	cfg *config.Config,
	sessions sessions.Registry,
	lockOwner string,
	turns TurnPersistence,
	budget BudgetGuard,
	workspaces workspace.Resolver,
	conversations ConversationPersistence,
	publisher EventPublisher,
) *ClaudeCLIRuntime {
	return NewClaudeCLIRuntimeWithOptions(cfg, sessions, lockOwner, turns, budget, workspaces, conversations, publisher, ClaudeCLIRuntimeOptions{})
}

func NewClaudeCLIRuntimeWithOptions(
	cfg *config.Config,
	sessions sessions.Registry,
	lockOwner string,
	turns TurnPersistence,
	budget BudgetGuard,
	workspaces workspace.Resolver,
	conversations ConversationPersistence,
	publisher EventPublisher,
	opts ClaudeCLIRuntimeOptions,
) *ClaudeCLIRuntime {
	monitor := opts.MonitorSink
	if monitor == nil {
		monitor = NewFileMonitorSink(DefaultMonitorDir())
	}
	providerCredentials := opts.ProviderCredentials
	if providerCredentials.EnvLookup == nil {
		providerCredentials = NewProviderCredentialResolver(providerCredentials.Store)
	}
	return &ClaudeCLIRuntime{
		cfg:                 cfg,
		sessions:            sessions,
		turns:               turns,
		conversations:       conversations,
		budget:              budget,
		lockOwner:           lockOwner,
		workspaces:          workspaces,
		monitor:             monitor,
		events:              publisher,
		mcpTurns:            opts.MCPTurnContextStore,
		toolGateway:         opts.ToolGateway,
		providerCredentials: providerCredentials,
		providerAdmission:   NewProviderAdmissionRegistry(cfg),
	}
}

func (r *ClaudeCLIRuntime) ProviderContract() ProviderContract {
	return ClaudeCLIProviderContract()
}

func ClaudeCLIProviderContract() ProviderContract {
	return ProviderContract{
		RuntimeMode: "cli_test",
		Provider:    "claude",
		Transport:   ProviderTransportCLI,
		ToolSchema: ProviderToolSchemaContract{
			ValidatesInputSchemas: true,
			TranslatesTools:       true,
			ReturnsToolResults:    true,
		},
		SessionLifecycle: ProviderSessionLifecycleContract{
			StartsSessions:            true,
			ContinuesSessions:         true,
			SupportsConversationModes: true,
			ProviderSessionIDStrategy: "provider_adopted",
			RotatesSessions:           true,
			PreservesRetryLineage:     true,
		},
		Response: ProviderResponseContract{
			NormalizesMessages:     true,
			NormalizesToolCalls:    true,
			PreservesRawResponse:   true,
			NormalizesVisibleTools: true,
			StreamingParser:        "claude_cli_stream_json",
		},
		NativeTools: ProviderNativeToolContract{
			Capabilities: NativeToolCapabilities{
				Bash:      true,
				WebSearch: true,
				FileIO:    true,
			},
			StrictProviderNativeSupport: true,
			StartupVisibleSurfaceProbe:  true,
		},
		Persistence: ProviderPersistenceContract{
			PersistsTurns:                 true,
			PersistsConversationSnapshots: true,
			PersistsTaskModeAudit:         true,
		},
		Budget: ProviderBudgetContract{
			UsageAccounting: BudgetUsageEstimated,
		},
	}
}

func (r *ClaudeCLIRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	mode, err := sessions.ParseConversationRuntimeMode(s.ConversationMode)
	if err != nil {
		return err
	}
	if !shouldPersistConversationMode(mode) {
		return nil
	}
	return r.conversations.UpsertConversation(ctx, ConversationRecord{
		SessionID:    s.ID,
		AgentID:      s.AgentID,
		SessionScope: strings.TrimSpace(s.SessionScope),
		ScopeKey:     strings.TrimSpace(s.ScopeKey),
		Watchdog:     s.Watchdog,
		RunID:        strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)),
		Mode:         mode.String(),
		Messages:     s.Messages,
		Summary:      BuildSessionSummary(s),
		TurnCount:    s.TurnCount,
		Status:       "active",
	})
}

func (r *ClaudeCLIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	scope := sessions.ScopeFromContext(ctx)
	resolved, err := resolvedSessionScope(ctx, sessions.NormalizeConversationRuntimeMode(scope.ConversationMode), sessions.NormalizeSessionScope(scope.SessionScope), scope.ScopeKey)
	if err != nil {
		return nil, err
	}

	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, agentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, err
		}
		if err := r.sessions.Release(ctx, lease); err != nil {
			return nil, err
		}
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)

	s := &Session{
		ID: ensurePlatformSessionID(func() string {
			if lease != nil {
				return lease.SessionID
			}
			return ""
		}()),
		ProviderSessionID: func() string {
			if lease != nil {
				return lease.ProviderSessionID
			}
			return ""
		}(),
		AgentID:          agentID,
		RuntimeMode:      resolved.RuntimeMode.String(),
		ConversationMode: resolved.RuntimeMode.String(),
		SessionScope:     resolved.Scope.String(),
		ScopeKey:         resolved.ScopeKey,
		RetryReason: func() string {
			if lease != nil {
				return lease.RetryReason
			}
			return ""
		}(),
		RetriesFromSessionID: func() string {
			if lease != nil {
				return lease.RetriesFromSessionID
			}
			return ""
		}(),
		SystemPrompt: augmentCLISystemPrompt(systemPrompt, actor, tools),
		Tools:        tools,
		Messages:     nil,
	}
	if r.conversations != nil && !resolved.Stateless {
		if rec, ok, err := r.conversations.LoadActiveConversation(ctx, agentID, resolved.RuntimeMode.String(), resolved.Scope.String(), resolved.ScopeKey); err == nil && ok {
			s.Messages = rec.Messages
			s.TurnCount = rec.TurnCount
			s.RetryReason = strings.TrimSpace(rec.RetryReason)
			s.RetriesFromSessionID = strings.TrimSpace(rec.RetriesFromSessionID)
			s.Watchdog = rec.Watchdog
		}
	}
	publishAgentStarted(ctx, r.events, s, events.EventType("platform.agent_started"))
	return s, nil
}

func (r *ClaudeCLIRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()
	scopeKey := budgetExecutionScopeKey(actor)
	disallowedBuiltinTools := claudeDisallowedBuiltinToolsArgForActor(actor, s.Tools)
	allowedToolsArg := claudeAllowedToolsArgForActor(actor, s.Tools)

	// Spec v2.0 budget cap enforcement: at 100% (budget.emergency) we hard-stop
	// LLM execution for the affected scope(s). Treated as transient so deliveries
	// can be retried after budget resumes.
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEntityEmergency(entityID) {
			return nil, budgetEmergencyFailure(entityID)
		}
	}

	resolved, err := resolvedSessionScope(ctx, sessions.NormalizeConversationRuntimeMode(coalesce(s.ConversationMode, s.RuntimeMode)), sessions.NormalizeSessionScope(coalesce(s.SessionScope, "")), s.ScopeKey)
	if err != nil {
		return nil, err
	}
	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, sessionAcquireFailure(err, s.AgentID)
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, resolved.RuntimeMode, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the CLI session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
				"runtime_mode": resolved.RuntimeMode.String(),
				"scope_key":    resolved.ScopeKey,
			}, heartbeatErr)
		})
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdoptedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode.String(), s.ID, lease.SessionID, resolved.ScopeKey)
			s.ID = lease.SessionID
		}
		if sid := strings.TrimSpace(lease.ProviderSessionID); sid != "" {
			s.ProviderSessionID = sid
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, s, "error", "Marking the reused agent delivery in progress failed", map[string]any{
		"runtime_mode": resolved.RuntimeMode.String(),
		"scope_key":    resolved.ScopeKey,
	}, entityID); err != nil {
		return nil, fmt.Errorf("mark inbound delivery active for reused cli session: %w", err)
	}
	target, err := r.resolveWorkspace(ctx)
	if err != nil {
		return nil, err
	}

	prompt := message.Content
	if strings.TrimSpace(prompt) == "" {
		err := runtimefailures.New(runtimefailures.ClassSchemaInvalid, "empty_agent_prompt", "llm-runtime", "claude_cli_turn", nil)
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode.String(),
			SessionID:      s.ID,
			RequestPayload: jsonBytes(map[string]any{"message": message}),
			ParseOK:        false,
			Latency:        0,
			Failure:        agentTurnFailure(err, "claude_cli_turn"),
		}, nil))
		return nil, err
	}

	confirmedHead := strings.TrimSpace(s.ProviderSessionID)
	if s.TurnCount > 0 && confirmedHead == "" {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "claude_provider_head_missing", "claude-cli-adapter", "continue_session", map[string]any{"session_id": s.ID})
	}
	transportFallback := promptTransportFallback{}
	mcpConfig, _, mcpEnabled, err := r.buildMCPConfigArg(ctx, s)
	if err != nil {
		return nil, err
	}
	requestFingerprintInput := jsonBytes(map[string]any{
		"allowed_tools":           allowedToolsArg,
		"confirmed_provider_head": confirmedHead,
		"conversation_mode":       resolved.RuntimeMode.String(),
		"disallowed_tools":        disallowedBuiltinTools,
		"mcp_enabled":             mcpEnabled,
		"output_format":           configuredCLIOutputFormat(r.cfg),
		"permission_mode_args":    permissionModeArgs(),
		"prompt":                  prompt,
		"scope_key":               resolved.ScopeKey,
		"session_scope":           resolved.Scope.String(),
		"system_prompt":           strings.TrimSpace(s.SystemPrompt),
		"tools":                   s.Tools,
		"turn_count":              s.TurnCount,
	})
	attempt, err := runtimeeffects.Begin(ctx, "claude_cli", requestFingerprintInput, nil)
	if err != nil {
		return nil, err
	}
	childSessionID := strings.TrimSpace(attempt.Attempt().AttemptID)
	_, differentOwner := runtimeeffects.DifferentOwnerFromContext(ctx)
	if childSessionID == "" && differentOwner {
		childSessionID = uuid.NewString()
	}
	if childSessionID == "" {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "claude_attempt_identity_missing", "claude-cli-adapter", "prepare_turn", nil)
	}

	var args []string
	if s.TurnCount == 0 {
		args = []string{
			"-p",
			"--session-id", childSessionID,
			"--output-format", configuredCLIOutputFormat(r.cfg),
		}
		args = appendClaudePrintModeArgs(args, r.cfg)
		args = append(args, permissionModeArgs()...)
		if sys := strings.TrimSpace(s.SystemPrompt); sys != "" {
			args = append(args, "--system-prompt", sys)
		}
		if strings.TrimSpace(disallowedBuiltinTools) != "" {
			args = append(args, "--disallowedTools", disallowedBuiltinTools)
		}
		if strings.TrimSpace(allowedToolsArg) != "" {
			args = append(args, "--allowedTools", allowedToolsArg)
		}
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
	} else {
		args = []string{
			"-p",
			"--resume", confirmedHead,
			"--fork-session",
			"--session-id", childSessionID,
			"--output-format", configuredCLIOutputFormat(r.cfg),
		}
		args = appendClaudePrintModeArgs(args, r.cfg)
		args = append(args, permissionModeArgs()...)
		if strings.TrimSpace(disallowedBuiltinTools) != "" {
			args = append(args, "--disallowedTools", disallowedBuiltinTools)
		}
		if strings.TrimSpace(allowedToolsArg) != "" {
			args = append(args, "--allowedTools", allowedToolsArg)
		}
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
	}

	start := time.Now()
	longRunningAfter, noOutputAfter := conversationWatchdogThresholds(r.effectiveCLITimeout(ctx))
	monitorMeta := MonitorTurnMeta{
		AgentID:                  s.AgentID,
		Runtime:                  "cli_test",
		SessionID:                s.ID,
		ScopeKey:                 s.ScopeKey,
		SessionScope:             s.SessionScope,
		ConversationMode:         resolved.RuntimeMode.String(),
		InputRole:                message.Role,
		InputText:                prompt,
		WatchdogLongRunningAfter: longRunningAfter,
		WatchdogNoOutputAfter:    noOutputAfter,
		TargetName: func() string {
			if target == nil {
				return ""
			}
			return target.Container
		}(),
	}
	resp, fallback, err := r.runWithPreparedPrompt(ctx, args, target, prompt, monitorMeta, attempt)
	transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
	transportFallback.Used = transportFallback.Used || fallback.Used
	latency := time.Since(start)
	if err != nil {
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:     s.AgentID,
			RuntimeMode: resolved.RuntimeMode.String(),
			SessionID:   s.ID,
			RequestPayload: jsonBytes(map[string]any{
				"args":                          args,
				"message":                       message,
				"provider_session_id":           strings.TrimSpace(s.ProviderSessionID),
				"prompt_arg_fallback_attempted": transportFallback.Attempted,
				"prompt_arg_fallback_used":      transportFallback.Used,
			}),
			ParseOK: false,
			Latency: latency,
			Failure: agentTurnFailure(err, "claude_cli_turn"),
		}, nil))
		projectionErr := requireCurrentProviderProjection(ctx, s.AgentID)
		if projectionErr == nil {
			s.ParseFailures++
		}
		if projectionErr == nil && !resolved.Stateless {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		if projectionErr != nil {
			return nil, errors.Join(err, projectionErr)
		}
		return nil, err
	}
	if err := validateCLIResponseToolCallsForTurn(actor, s.Tools, resp); err != nil {
		err = settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "validate_tool_calls", map[string]any{"provider_session_id": strings.TrimSpace(resp.SessionID)})
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:     s.AgentID,
			RuntimeMode: resolved.RuntimeMode.String(),
			SessionID:   s.ID,
			RequestPayload: jsonBytes(map[string]any{
				"args":                          args,
				"message":                       message,
				"provider_session_id":           strings.TrimSpace(s.ProviderSessionID),
				"prompt_arg_fallback_attempted": transportFallback.Attempted,
				"prompt_arg_fallback_used":      transportFallback.Used,
			}),
			ResponseRaw: resp.Raw,
			ParseOK:     true,
			Latency:     latency,
			Failure:     agentTurnFailure(err, "claude_cli_tool_validation"),
		}, resp))
		if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
			return nil, errors.Join(err, projectionErr)
		}
		return nil, err
	}
	if returnedSessionID := strings.TrimSpace(resp.SessionID); returnedSessionID != childSessionID {
		err := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "claude_provider_child_identity_mismatch", "claude-cli-adapter", "validate_response", map[string]any{"expected_provider_session_id": childSessionID, "returned_provider_session_id": returnedSessionID})
		return nil, settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "validate_response", map[string]any{"expected_provider_session_id": childSessionID, "returned_provider_session_id": returnedSessionID})
	}

	r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:     s.AgentID,
		RuntimeMode: resolved.RuntimeMode.String(),
		SessionID:   s.ID,
		RequestPayload: jsonBytes(map[string]any{
			"args":                          args,
			"message":                       message,
			"provider_session_id":           strings.TrimSpace(s.ProviderSessionID),
			"prompt_arg_fallback_attempted": transportFallback.Attempted,
			"prompt_arg_fallback_used":      transportFallback.Used,
		}),
		ResponseRaw: resp.Raw,
		ParseOK:     true,
		Latency:     latency,
	}, resp))

	// Spend is immutable evidence of the launched provider attempt and remains
	// attributable even when this generation was superseded before projection.
	if r.budget != nil {
		usage := estimateCLIUsageTokens(message, resp, actor)
		profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
		meta := usageMetadataForContext(ctx, profile, usage.Model)
		meta["session_id"] = s.ID
		if err := r.budget.RecordEntityLLMUsage(ctx, entityID, s.AgentID, profile.ID, usage, false, meta); err != nil {
			logPublisherRuntime(ctx, r.events, "warn", "record_cli_llm_usage_failed", "Recording CLI LLM usage failed", s.AgentID, s.ID, entityID, nil, err)
		}
	}

	if err := requireCurrentProviderProjection(ctx, s.AgentID); err != nil {
		return nil, settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "project_provider_turn", map[string]any{"provider_session_id": childSessionID})
	}
	settlementEvidence := map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(resp.Raw), "provider_session_id": childSessionID}
	if resolved.Stateless {
		if err := attempt.Succeed(ctx, settlementEvidence); err != nil {
			return nil, settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "settle_provider_turn", settlementEvidence)
		}
	} else if differentOwner {
		if err := adoptRegistrySessionID(ctx, r.sessions, s.AgentID, resolved.RuntimeMode, resolved.Scope, lease.LockOwner, childSessionID, resolved.ScopeKey); err != nil {
			return nil, err
		}
		if err := attempt.Succeed(ctx, settlementEvidence); err != nil {
			return nil, err
		}
	} else {
		if lease == nil {
			err := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "claude_session_lease_missing", "claude-cli-adapter", "settle_provider_head", nil)
			return nil, settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "settle_provider_head", settlementEvidence)
		}
		if err := attempt.SucceedAndPromoteProviderHead(ctx, runtimeeffects.ProviderHeadSettlement{
			Settlement: runtimeeffects.Settlement{Evidence: settlementEvidence},
			AgentID:    s.AgentID, RuntimeMode: resolved.RuntimeMode.String(), SessionID: s.ID,
			ScopeKey: resolved.ScopeKey, LockOwner: lease.LockOwner,
			ExpectedProviderHead: confirmedHead, NewProviderHead: childSessionID,
		}); err != nil {
			return nil, settleClaudeAttemptFailure(ctx, attempt, runtimeeffects.StateOutcomeUncertain, err, "settle_provider_head", settlementEvidence)
		}
	}
	s.Messages = append(s.Messages, message, resp.Message)
	s.ProviderSessionID = childSessionID
	LogSessionAdoptedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode.String(), confirmedHead, childSessionID, resolved.ScopeKey)
	s.TurnCount++
	s.ParseFailures = 0

	if !resolved.Stateless {
		if err := r.sessions.IncrementTurn(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, s.ID, resolved.ScopeKey); err != nil {
			return nil, err
		}
	}

	r.persistConversation(ctx, s)

	if !resolved.Stateless {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}
	return resp, nil
}

func (r *ClaudeCLIRuntime) admitProviderDispatch(ctx context.Context) (func(), error) {
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	resolvedModel, err := resolveProviderAdmissionModel(ctx, r.cfg, r.providerAdmission, profile)
	if err != nil {
		return noopProviderAdmissionRelease, err
	}
	release, err := admitProviderRequest(ctx, r.providerAdmission, profile, resolvedModel)
	if err != nil {
		return noopProviderAdmissionRelease, err
	}
	return release, nil
}

func validateCLIResponseToolCallsForTurn(actor runtimeactors.AgentConfig, tools []ToolDefinition, resp *Response) error {
	if resp == nil || len(resp.ToolCalls) == 0 {
		return nil
	}
	for _, call := range resp.ToolCalls {
		if cliToolCallAllowedForTurn(actor, tools, resp, call.Name) {
			continue
		}
		return fmt.Errorf("tool %q was not provider-visible or locally allowed on this turn", strings.TrimSpace(call.Name))
	}
	return nil
}
