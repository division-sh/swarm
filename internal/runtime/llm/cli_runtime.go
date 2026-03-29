package llm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
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
}

var ErrClaudeAuthRequired = errors.New("claude auth required")

type promptTransportFallback struct {
	Attempted bool
	Used      bool
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
	return &ClaudeCLIRuntime{
		cfg:           cfg,
		sessions:      sessions,
		turns:         turns,
		conversations: conversations,
		budget:        budget,
		lockOwner:     lockOwner,
		workspaces:    workspaces,
		monitor:       NewFileMonitorSink(DefaultMonitorDir()),
		events:        publisher,
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
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = sessions.RuntimeModeSession
	}
	if !shouldPersistConversationMode(mode) {
		return nil
	}
	return r.conversations.UpsertConversation(ctx, ConversationRecord{
		SessionID: s.ID,
		AgentID:   s.AgentID,
		ScopeKey:  strings.TrimSpace(s.ScopeKey),
		Mode:      mode,
		Messages:  s.Messages,
		Summary:   BuildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	})
}

func (r *ClaudeCLIRuntime) SetWorkspaceResolver(resolver workspace.Resolver) {
	r.workspaces = resolver
}

func (r *ClaudeCLIRuntime) SetMonitorSink(sink MonitorSink) {
	r.monitor = sink
}

func (r *ClaudeCLIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	scope := sessions.ScopeFromContext(ctx)
	mode := strings.TrimSpace(scope.ConversationMode)
	if mode == "" {
		mode = sessions.RuntimeModeSession
	}
	resolved := resolvedSessionScope(mode, scope.ScopeKey)

	var lease *sessions.Lease
	if !resolved.Stateless {
		var err error
		lease, err = r.sessions.Acquire(ctx, agentID, resolved.RuntimeMode, r.lockOwner, resolved.ScopeKey)
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
		ConversationMode: mode,
		ScopeKey:         resolved.ScopeKey,
		SystemPrompt:     systemPrompt,
		Tools:            tools,
		Messages:         nil,
	}
	if r.conversations != nil && !resolved.Stateless {
		if rec, ok, err := r.conversations.LoadActiveConversation(ctx, agentID, mode, resolved.ScopeKey); err == nil && ok {
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

	resolved := resolvedSessionScope(s.ConversationMode, s.ScopeKey)
	var lease *sessions.Lease
	var err error
	if !resolved.Stateless {
		lease, err = r.sessions.Acquire(ctx, s.AgentID, resolved.RuntimeMode, r.lockOwner, resolved.ScopeKey)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeat(ctx, r.sessions, lease, resolved.RuntimeMode)
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdopted(s.AgentID, resolved.RuntimeMode, s.ID, lease.SessionID, resolved.ScopeKey)
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
		r.persistTurn(ctx, AgentTurnRecord{
			AgentID:        s.AgentID,
			RuntimeMode:    resolved.RuntimeMode,
			SessionID:      s.ID,
			RequestPayload: jsonBytes(map[string]any{"message": message}),
			ParseOK:        false,
			Latency:        0,
			Error:          err.Error(),
		})
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
			rotated, rotateErr := r.sessions.Rotate(ctx, s.AgentID, resolved.RuntimeMode, r.lockOwner, checkpoint, resolved.ScopeKey)
			if rotateErr == nil && rotated != nil {
				s.ID = rotated.SessionID
				s.ProviderSessionID = rotated.ProviderSessionID
				s.TurnCount = 0
				if len(s.Messages) > 0 {
					s.Messages = []Message{{Role: "system", Content: "Session rotated due to CLI runtime recovery."}}
				}
				LogSessionRotated(
					s.AgentID,
					resolved.RuntimeMode,
					oldSessionID,
					rotated.SessionID,
					resolved.ScopeKey,
					rotateReason,
					oldTurnCount,
					oldParseFailures,
				)
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
		r.persistTurn(ctx, AgentTurnRecord{
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
		})
		if !resolved.Stateless {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		return nil, err
	}

	s.Messages = append(s.Messages, message, resp.Message)
	if sid := strings.TrimSpace(resp.SessionID); sid != "" && sid != s.ProviderSessionID {
		oldSessionID := strings.TrimSpace(s.ProviderSessionID)
		if !resolved.Stateless {
			if err := adoptRegistrySessionID(ctx, r.sessions, s.AgentID, resolved.RuntimeMode, lease.LockOwner, sid, resolved.ScopeKey); err != nil {
				log.Printf("failed to adopt claude session id: agent=%s old=%s new=%s err=%v", s.AgentID, oldSessionID, sid, err)
			} else {
				s.ProviderSessionID = sid
				LogSessionAdopted(s.AgentID, resolved.RuntimeMode, oldSessionID, sid, resolved.ScopeKey)
			}
		} else {
			s.ProviderSessionID = sid
		}
	}
	s.TurnCount++
	s.ParseFailures = 0

	if !resolved.Stateless {
		if err := r.sessions.IncrementTurn(ctx, s.AgentID, resolved.RuntimeMode, s.ID, resolved.ScopeKey); err != nil {
			return nil, err
		}
	}

	r.persistTurn(ctx, AgentTurnRecord{
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
	})
	r.persistConversation(ctx, s)

	// Spend ledger: CLI runtime does not expose exact usage; estimate from payload sizes.
	if r.budget != nil {
		usage := estimateCLIUsageTokens(message, resp, actor)
		if err := r.budget.RecordEntityLLMUsage(ctx, entityID, s.AgentID, "cli_test", usage, false, map[string]any{
			"session_id": s.ID,
		}); err != nil {
			log.Printf("failed to record cli llm usage: agent=%s session=%s err=%v", s.AgentID, s.ID, err)
		}
	}

	if !resolved.Stateless {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, resolved.RuntimeMode, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns); rotateErr == nil && rotated != nil {
			lease = rotated
		}
	}
	return resp, nil
}
