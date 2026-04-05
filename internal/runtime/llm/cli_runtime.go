package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

type ClaudeCLIRuntime struct {
	cfg           *config.Config
	sessions      sessions.Registry
	turns         TurnPersistence
	conversations ConversationPersistence
	budget        BudgetGuard
	lockOwner     string
	workspaces    workspace.Resolver
	monitor       MonitorSink
	events        EventPublisher
	mcpTurns      MCPTurnContextStore
}

var ErrClaudeAuthRequired = errors.New("claude auth required")
var ErrClaudeWorkspaceRequired = errors.New("claude workspace target required")

type promptTransportFallback struct {
	Attempted bool
	Used      bool
}

type ClaudeCLIRuntimeOptions struct {
	MonitorSink         MonitorSink
	MCPTurnContextStore MCPTurnContextStore
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
	return &ClaudeCLIRuntime{
		cfg:           cfg,
		sessions:      sessions,
		turns:         turns,
		conversations: conversations,
		budget:        budget,
		lockOwner:     lockOwner,
		workspaces:    workspaces,
		monitor:       monitor,
		events:        publisher,
		mcpTurns:      opts.MCPTurnContextStore,
	}
}

func (r *ClaudeCLIRuntime) NativeToolCapabilities() NativeToolCapabilities {
	return NativeToolCapabilities{
		Bash:      true,
		WebSearch: true,
		FileIO:    true,
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
		RunID:        strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)),
		Mode:         mode,
		Messages:     s.Messages,
		Summary:      BuildSessionSummary(s),
		TurnCount:    s.TurnCount,
		Status:       "active",
	})
}

func (r *ClaudeCLIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	scope := sessions.ScopeFromContext(ctx)
	resolved, err := resolvedSessionScope(ctx, scope.ConversationMode, scope.SessionScope, scope.ScopeKey)
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
		RuntimeMode:      resolved.RuntimeMode,
		ConversationMode: resolved.RuntimeMode,
		SessionScope:     resolved.Scope,
		ScopeKey:         resolved.ScopeKey,
		SystemPrompt:     augmentCLISystemPrompt(systemPrompt, tools),
		Tools:            tools,
		Messages:         nil,
	}
	if r.conversations != nil && !resolved.Stateless {
		if rec, ok, err := r.conversations.LoadActiveConversation(ctx, agentID, resolved.RuntimeMode, resolved.Scope, resolved.ScopeKey); err == nil && ok {
			s.Messages = rec.Messages
			s.TurnCount = rec.TurnCount
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
	disallowedBuiltinTools := claudeDisallowedBuiltinToolsArgForActor(actor)
	allowedToolsArg := claudeAllowedToolsArgForActor(actor, s.Tools)

	// Spec v2.0 budget cap enforcement: at 100% (budget.emergency) we hard-stop
	// LLM execution for the affected scope(s). Treated as transient so deliveries
	// can be retried after budget resumes.
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEntityEmergency(entityID) {
			return nil, fmt.Errorf("budget emergency: refusing llm execution (entity=%s)", entityID)
		}
	}

	resolved, err := resolvedSessionScope(ctx, coalesce(s.ConversationMode, s.RuntimeMode), coalesce(s.SessionScope, ""), s.ScopeKey)
	if err != nil {
		return nil, err
	}
	var lease *sessions.Lease
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, resolved.RuntimeMode, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the CLI session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
				"runtime_mode": resolved.RuntimeMode,
				"scope_key":    resolved.ScopeKey,
			}, heartbeatErr)
		})
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdoptedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode, s.ID, lease.SessionID, resolved.ScopeKey)
			s.ID = lease.SessionID
		}
		if sid := strings.TrimSpace(lease.ProviderSessionID); sid != "" {
			s.ProviderSessionID = sid
		}
	}
	target, err := r.resolveWorkspace(ctx)
	if err != nil {
		return nil, err
	}

	prompt := message.Content
	if strings.TrimSpace(prompt) == "" {
		err := errors.New("empty prompt input for claude cli")
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode,
			SessionID:      s.ID,
			RequestPayload: jsonBytes(map[string]any{"message": message}),
			ParseOK:        false,
			Latency:        0,
			Error:          err.Error(),
		}, nil))
		return nil, err
	}

	var args []string
	transportFallback := promptTransportFallback{}
	mcpConfig, _, mcpEnabled, err := r.buildMCPConfigArg(ctx, s)
	if err != nil {
		return nil, err
	}
	if s.TurnCount == 0 {
		args = []string{
			"-p",
			"--session-id", sessionToken(s),
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
			"-r", sessionToken(s),
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
	monitorMeta := MonitorTurnMeta{
		AgentID:   s.AgentID,
		Runtime:   "cli_test",
		SessionID: sessionToken(s),
		ScopeKey:  s.ScopeKey,
		InputRole: message.Role,
		InputText: prompt,
		TargetName: func() string {
			if target == nil {
				return ""
			}
			return target.Container
		}(),
	}
	resp, fallback, err := r.runWithPromptTransportFallback(ctx, args, target, prompt, monitorMeta)
	transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
	transportFallback.Used = transportFallback.Used || fallback.Used
	if err != nil && s.TurnCount == 0 && isUnsupportedCLIFlagError(err) {
		args = []string{
			"-p",
			"--session-id", sessionToken(s),
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
		resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, buildInitialPrompt(s, prompt), monitorMeta)
		transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
		transportFallback.Used = transportFallback.Used || fallback.Used
	}
	if err != nil && shouldRotateSessionOnCLIError(err) {
		rotateReason := rotateSessionRetryReason(err)
		oldSessionID := s.ID
		oldTurnCount := s.TurnCount
		oldParseFailures := s.ParseFailures
		checkpoint := BuildRotationCheckpoint(rotateReason, s)
		if !resolved.Stateless {
			rotated, rotateErr := r.sessions.Rotate(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, r.lockOwner, checkpoint, resolved.ScopeKey)
			if rotateErr == nil && rotated != nil {
				s.ID = rotated.SessionID
				s.ProviderSessionID = rotated.ProviderSessionID
				s.TurnCount = 0
				if len(s.Messages) > 0 {
					s.Messages = []Message{{Role: "system", Content: "Session rotated due to CLI runtime recovery."}}
				}
				LogSessionRotatedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode, oldSessionID, rotated.SessionID, resolved.ScopeKey, rotateReason, oldTurnCount, oldParseFailures)
				args = []string{
					"-p",
					"--session-id", sessionToken(s),
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
				monitorMeta.SessionID = sessionToken(s)
				resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, message.Content, monitorMeta)
				transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
				transportFallback.Used = transportFallback.Used || fallback.Used
				if err != nil && isUnsupportedCLIFlagError(err) {
					args = []string{
						"-p",
						"--session-id", sessionToken(s),
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
					resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, buildInitialPrompt(s, message.Content), monitorMeta)
					transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
					transportFallback.Used = transportFallback.Used || fallback.Used
				}
			}
		}
	}
	latency := time.Since(start)
	if err != nil {
		s.ParseFailures++
		r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
			AgentID:     s.AgentID,
			RuntimeMode: resolved.RuntimeMode,
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
			Error:   err.Error(),
		}, nil))
		if !resolved.Stateless {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		return nil, err
	}

	s.Messages = append(s.Messages, message, resp.Message)
	if sid := strings.TrimSpace(resp.SessionID); sid != "" && sid != s.ProviderSessionID {
		oldSessionID := strings.TrimSpace(s.ProviderSessionID)
		if !resolved.Stateless {
			if err := adoptRegistrySessionID(ctx, r.sessions, s.AgentID, resolved.RuntimeMode, resolved.Scope, lease.LockOwner, sid, resolved.ScopeKey); err != nil {
				logPublisherRuntime(ctx, r.events, "warn", "adopt_cli_provider_session_failed", "Adopting the CLI provider session failed", s.AgentID, s.ID, entityID, map[string]any{
					"old_provider_session_id": oldSessionID,
					"new_provider_session_id": sid,
				}, err)
			} else {
				s.ProviderSessionID = sid
				LogSessionAdoptedForRun(ctx, r.events, s.AgentID, resolved.RuntimeMode, oldSessionID, sid, resolved.ScopeKey)
			}
		} else {
			s.ProviderSessionID = sid
		}
	}
	s.TurnCount++
	s.ParseFailures = 0

	if !resolved.Stateless {
		if err := r.sessions.IncrementTurn(ctx, s.AgentID, resolved.RuntimeMode, resolved.Scope, s.ID, resolved.ScopeKey); err != nil {
			return nil, err
		}
	}

	r.persistTurn(ctx, enrichTurnRecord(ctx, s, AgentTurnRecord{
		AgentID:     s.AgentID,
		RuntimeMode: resolved.RuntimeMode,
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
	r.persistConversation(ctx, s)

	// Spend ledger: CLI runtime does not expose exact usage; estimate from payload sizes.
	if r.budget != nil {
		usage := estimateCLIUsageTokens(message, resp, actor)
		if err := r.budget.RecordEntityLLMUsage(ctx, entityID, s.AgentID, "cli_test", usage, false, map[string]any{
			"session_id": s.ID,
		}); err != nil {
			logPublisherRuntime(ctx, r.events, "warn", "record_cli_llm_usage_failed", "Recording CLI LLM usage failed", s.AgentID, s.ID, entityID, nil, err)
		}
	}

	if !resolved.Stateless {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}
	return resp, nil
}
