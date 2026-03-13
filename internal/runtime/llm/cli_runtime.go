package llm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/config"
	runtimeactor "empireai/internal/runtime/actorctx"
	"empireai/internal/runtime/sessions"
	workspace "empireai/internal/runtime/workspace"
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
	}
}

func (r *ClaudeCLIRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	return r.conversations.UpsertConversation(ctx, ConversationRecord{
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
		mode = "session"
	}
	scopeKey := strings.TrimSpace(scope.ScopeKey)
	lease, err := r.sessions.Acquire(ctx, agentID, "cli_test", r.lockOwner, scopeKey)
	if err != nil {
		return nil, err
	}
	if err := r.sessions.Release(ctx, lease); err != nil {
		return nil, err
	}

	s := &Session{
		ID:               lease.SessionID,
		AgentID:          agentID,
		RuntimeMode:      "cli_test",
		ConversationMode: mode,
		ScopeKey:         scopeKey,
		SystemPrompt:     systemPrompt,
		Tools:            tools,
		Messages:         nil,
	}
	if r.conversations != nil {
		if rec, ok, err := r.conversations.LoadActiveConversation(ctx, agentID, mode, scopeKey); err == nil && ok {
			s.Messages = rec.Messages
			s.TurnCount = rec.TurnCount
		}
	}
	return s, nil
}

func (r *ClaudeCLIRuntime) ContinueSession(ctx context.Context, s *Session, message Message) (*Response, error) {
	if s == nil {
		return nil, errors.New("nil session")
	}
	actor, _ := runtimeactor.ActorFromContext(ctx)
	entityID := actor.EffectiveEntityID()
	scopeKey := budgetExecutionScopeKey(actor)

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

	lease, err := r.sessions.Acquire(ctx, s.AgentID, "cli_test", r.lockOwner, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.sessions.Release(ctx, lease) }()
	stopLeaseHeartbeat := sessions.StartLeaseHeartbeat(ctx, r.sessions, lease, "cli_test")
	defer stopLeaseHeartbeat()

	if lease.SessionID != s.ID {
		LogSessionAdopted(s.AgentID, "cli_test", s.ID, lease.SessionID, strings.TrimSpace(s.ScopeKey))
		s.ID = lease.SessionID
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
			RuntimeMode:    "cli_test",
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
			"--session-id", s.ID,
			"--output-format", configuredCLIOutputFormat(r.cfg),
		}
		if shouldIncludePartialMessages(r.cfg) {
			args = append(args, "--include-partial-messages")
		}
		args = append(args, permissionModeArgs()...)
		if sys := strings.TrimSpace(s.SystemPrompt); sys != "" {
			args = append(args, "--system-prompt", sys)
		}
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		} else if tools := claudeToolsArg(s.Tools); tools != "" {
			args = append(args, "--tools", tools)
		}
	} else {
		args = []string{
			"-p",
			"-r", s.ID,
			"--output-format", configuredCLIOutputFormat(r.cfg),
		}
		if shouldIncludePartialMessages(r.cfg) {
			args = append(args, "--include-partial-messages")
		}
		args = append(args, permissionModeArgs()...)
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
	}

	start := time.Now()
	monitorMeta := MonitorTurnMeta{
		AgentID:   s.AgentID,
		Runtime:   "cli_test",
		SessionID: s.ID,
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
			"--session-id", s.ID,
			"--output-format", configuredCLIOutputFormat(r.cfg),
		}
		if shouldIncludePartialMessages(r.cfg) {
			args = append(args, "--include-partial-messages")
		}
		args = append(args, permissionModeArgs()...)
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
		rotated, rotateErr := r.sessions.Rotate(ctx, s.AgentID, "cli_test", r.lockOwner, checkpoint, strings.TrimSpace(s.ScopeKey))
		if rotateErr == nil && rotated != nil {
			s.ID = rotated.SessionID
			s.TurnCount = 0
			if len(s.Messages) > 0 {
				s.Messages = []Message{{Role: "system", Content: "Session rotated due to CLI runtime recovery."}}
			}
			LogSessionRotated(
				s.AgentID,
				"cli_test",
				oldSessionID,
				rotated.SessionID,
				strings.TrimSpace(s.ScopeKey),
				rotateReason,
				oldTurnCount,
				oldParseFailures,
			)
			args = []string{
				"-p",
				"--session-id", s.ID,
				"--output-format", configuredCLIOutputFormat(r.cfg),
			}
			if shouldIncludePartialMessages(r.cfg) {
				args = append(args, "--include-partial-messages")
			}
			args = append(args, permissionModeArgs()...)
			if sys := strings.TrimSpace(s.SystemPrompt); sys != "" {
				args = append(args, "--system-prompt", sys)
			}
			if mcpEnabled {
				args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
			} else if tools := claudeToolsArg(s.Tools); tools != "" {
				args = append(args, "--tools", tools)
			}
			monitorMeta.SessionID = s.ID
			resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, message.Content, monitorMeta)
			transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
			transportFallback.Used = transportFallback.Used || fallback.Used
			if err != nil && isUnsupportedCLIFlagError(err) {
				args = []string{
					"-p",
					"--session-id", s.ID,
					"--output-format", configuredCLIOutputFormat(r.cfg),
				}
				if shouldIncludePartialMessages(r.cfg) {
					args = append(args, "--include-partial-messages")
				}
				args = append(args, permissionModeArgs()...)
				if mcpEnabled {
					args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
				}
				resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, buildInitialPrompt(s, message.Content), monitorMeta)
				transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
				transportFallback.Used = transportFallback.Used || fallback.Used
			}
		}
	}
	latency := time.Since(start)
	if err != nil {
		s.ParseFailures++
		r.persistTurn(ctx, AgentTurnRecord{
			AgentID:     s.AgentID,
			RuntimeMode: "cli_test",
			SessionID:   s.ID,
			RequestPayload: jsonBytes(map[string]any{
				"args":                          args,
				"message":                       message,
				"prompt_arg_fallback_attempted": transportFallback.Attempted,
				"prompt_arg_fallback_used":      transportFallback.Used,
			}),
			ParseOK: false,
			Latency: latency,
			Error:   err.Error(),
		})
		if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, "cli_test", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures); rotateErr == nil && rotated != nil {
			lease = rotated
		}
		return nil, err
	}

	s.Messages = append(s.Messages, message, resp.Message)
	if sid := strings.TrimSpace(resp.SessionID); sid != "" && sid != s.ID {
		oldSessionID := s.ID
		if err := adoptRegistrySessionID(ctx, r.sessions, s.AgentID, "cli_test", lease.LockOwner, sid, strings.TrimSpace(s.ScopeKey)); err != nil {
			log.Printf("failed to adopt claude session id: agent=%s old=%s new=%s err=%v", s.AgentID, s.ID, sid, err)
		} else {
			s.ID = sid
			lease.SessionID = sid
			LogSessionAdopted(s.AgentID, "cli_test", oldSessionID, sid, strings.TrimSpace(s.ScopeKey))
		}
	}
	s.TurnCount++
	s.ParseFailures = 0

	if err := r.sessions.IncrementTurn(ctx, s.AgentID, "cli_test", s.ID, strings.TrimSpace(s.ScopeKey)); err != nil {
		return nil, err
	}

	r.persistTurn(ctx, AgentTurnRecord{
		AgentID:     s.AgentID,
		RuntimeMode: "cli_test",
		SessionID:   s.ID,
		RequestPayload: jsonBytes(map[string]any{
			"args":                          args,
			"message":                       message,
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

	if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, "cli_test", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns); rotateErr == nil && rotated != nil {
		lease = rotated
	}
	return resp, nil
}
