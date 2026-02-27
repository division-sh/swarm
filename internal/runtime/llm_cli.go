package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

type ClaudeCLIRuntime struct {
	cfg           *config.Config
	sessions      SessionRegistry
	turns         TurnPersistence
	conversations ConversationPersistence
	budget        *BudgetTracker
	lockOwner     string
	workspaces    WorkspaceResolver
}

var ErrClaudeAuthRequired = errors.New("claude auth required")

type promptTransportFallback struct {
	Attempted bool
	Used      bool
}

func NewClaudeCLIRuntime(
	cfg *config.Config,
	sessions SessionRegistry,
	lockOwner string,
	turns TurnPersistence,
	budget *BudgetTracker,
	workspaces WorkspaceResolver,
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
		Summary:   buildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	})
}

func (r *ClaudeCLIRuntime) SetWorkspaceResolver(resolver WorkspaceResolver) {
	r.workspaces = resolver
}

func (r *ClaudeCLIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	scope := sessionScopeFromContext(ctx)
	mode := strings.TrimSpace(scope.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	scopeKey := strings.TrimSpace(scope.ScopeKey)
	lease, err := r.sessions.Acquire(agentID, "cli_test", r.lockOwner, scopeKey)
	if err != nil {
		return nil, err
	}
	if err := r.sessions.Release(lease); err != nil {
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
		if rec, ok, err := r.conversations.LoadActiveConversation(context.Background(), agentID, mode, scopeKey); err == nil && ok {
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
	actor, _ := ActorFromContext(ctx)
	verticalID := strings.TrimSpace(actor.VerticalID)
	scopeKey := budgetExecutionScopeKey(actor)

	// Spec v2.0 budget cap enforcement: at 100% (budget.emergency) we hard-stop
	// LLM execution for the affected scope(s). Treated as transient so deliveries
	// can be retried after budget resumes.
	if r.budget != nil {
		unlockScope := r.budget.LockExecutionScope(scopeKey)
		defer unlockScope()
		if r.budget.IsEmergency(verticalID) {
			return nil, fmt.Errorf("budget emergency: refusing llm execution (vertical=%s)", verticalID)
		}
	}

	lease, err := r.sessions.Acquire(s.AgentID, "cli_test", r.lockOwner, strings.TrimSpace(s.ScopeKey))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.sessions.Release(lease) }()
	stopLeaseHeartbeat := startLeaseHeartbeat(ctx, r.sessions, lease, "cli_test")
	defer stopLeaseHeartbeat()

	if lease.SessionID != s.ID {
		logSessionAdopted(s.AgentID, "cli_test", s.ID, lease.SessionID, strings.TrimSpace(s.ScopeKey))
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
			"--output-format", "json",
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
			"--output-format", "json",
		}
		args = append(args, permissionModeArgs()...)
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
	}

	start := time.Now()
	resp, fallback, err := r.runWithPromptTransportFallback(ctx, args, target, prompt)
	transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
	transportFallback.Used = transportFallback.Used || fallback.Used
	if err != nil && s.TurnCount == 0 && isUnsupportedCLIFlagError(err) {
		args = []string{
			"-p",
			"--session-id", s.ID,
			"--output-format", "json",
		}
		args = append(args, permissionModeArgs()...)
		if mcpEnabled {
			args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
		}
		resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, buildInitialPrompt(s, prompt))
		transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
		transportFallback.Used = transportFallback.Used || fallback.Used
	}
	if err != nil && shouldRotateSessionOnCLIError(err) {
		rotateReason := rotateSessionRetryReason(err)
		oldSessionID := s.ID
		oldTurnCount := s.TurnCount
		oldParseFailures := s.ParseFailures
		checkpoint := buildRotationCheckpoint(rotateReason, s)
		rotated, rotateErr := r.sessions.Rotate(s.AgentID, "cli_test", r.lockOwner, checkpoint, strings.TrimSpace(s.ScopeKey))
		if rotateErr == nil && rotated != nil {
			s.ID = rotated.SessionID
			s.TurnCount = 0
			if len(s.Messages) > 0 {
				s.Messages = []Message{{Role: "system", Content: "Session rotated due to CLI runtime recovery."}}
			}
			logSessionRotated(
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
				"--output-format", "json",
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
			resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, message.Content)
			transportFallback.Attempted = transportFallback.Attempted || fallback.Attempted
			transportFallback.Used = transportFallback.Used || fallback.Used
			if err != nil && isUnsupportedCLIFlagError(err) {
				args = []string{
					"-p",
					"--session-id", s.ID,
					"--output-format", "json",
				}
				args = append(args, permissionModeArgs()...)
				if mcpEnabled {
					args = append(args, "--mcp-config", mcpConfig, "--strict-mcp-config")
				}
				resp, fallback, err = r.runWithPromptTransportFallback(ctx, args, target, buildInitialPrompt(s, message.Content))
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
		if rotated, rotateErr := maybeRotateAfterParseFailures(s, "cli_test", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures); rotateErr == nil && rotated != nil {
			lease = rotated
		}
		return nil, err
	}

	s.Messages = append(s.Messages, message, resp.Message)
	if sid := strings.TrimSpace(resp.SessionID); sid != "" && sid != s.ID {
		oldSessionID := s.ID
		if err := adoptRegistrySessionID(r.sessions, s.AgentID, "cli_test", lease.LockOwner, sid, strings.TrimSpace(s.ScopeKey)); err != nil {
			log.Printf("failed to adopt claude session id: agent=%s old=%s new=%s err=%v", s.AgentID, s.ID, sid, err)
		} else {
			s.ID = sid
			lease.SessionID = sid
			logSessionAdopted(s.AgentID, "cli_test", oldSessionID, sid, strings.TrimSpace(s.ScopeKey))
		}
	}
	s.TurnCount++
	s.ParseFailures = 0

	if err := r.sessions.IncrementTurn(s.AgentID, "cli_test", s.ID, strings.TrimSpace(s.ScopeKey)); err != nil {
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
		if err := r.budget.RecordLLMUsage(ctx, verticalID, s.AgentID, "cli_test", usage, false, map[string]any{
			"session_id": s.ID,
		}); err != nil {
			log.Printf("failed to record cli llm usage: agent=%s session=%s err=%v", s.AgentID, s.ID, err)
		}
	}

	if rotated, rotateErr := maybeRotateAfterTurn(s, "cli_test", r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns); rotateErr == nil && rotated != nil {
		lease = rotated
	}
	return resp, nil
}

func isSessionInUseError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "session id") && strings.Contains(msg, "already in use")
}

func isSessionNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no conversation found with session id") {
		return true
	}
	if strings.Contains(msg, "conversation not found") && strings.Contains(msg, "session") {
		return true
	}
	if strings.Contains(msg, "session") && strings.Contains(msg, "not found") {
		return true
	}
	return false
}

func shouldRotateSessionOnCLIError(err error) bool {
	return isSessionInUseError(err) || isSessionNotFoundError(err)
}

func rotateSessionRetryReason(err error) string {
	switch {
	case isSessionInUseError(err):
		return "session in use"
	case isSessionNotFoundError(err):
		return "session not found"
	default:
		return "runtime recovery"
	}
}

func isUnsupportedCLIFlagError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !(strings.Contains(msg, "unknown option") || strings.Contains(msg, "unknown flag") || strings.Contains(msg, "unrecognized option")) {
		return false
	}
	return strings.Contains(msg, "--system-prompt") || strings.Contains(msg, "--tools")
}

func isPromptArgRequiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "input must be provided either through stdin or as a prompt argument when using --print")
}

func (r *ClaudeCLIRuntime) runWithPromptArg(ctx context.Context, args []string, target *WorkspaceTarget, prompt string) (*Response, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, errors.New("prompt argument fallback requires non-empty prompt")
	}
	runArgs := append(append([]string{}, args...), "--", prompt)
	return r.runWithInput(ctx, runArgs, target, "")
}

func (r *ClaudeCLIRuntime) buildMCPConfigArg(ctx context.Context, s *Session) (configJSON string, contextToken string, enabled bool, err error) {
	if !shouldUseMCPBridge() || s == nil || len(s.Tools) == 0 {
		return "", "", false, nil
	}
	actor, _ := ActorFromContext(ctx)
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = strings.TrimSpace(s.AgentID)
	}
	if strings.TrimSpace(actor.Mode) == "" {
		actor.Mode = "operating"
	}
	if strings.TrimSpace(actor.Role) == "" {
		actor.Role = actor.ID
	}
	if strings.TrimSpace(actor.ID) == "" {
		return "", "", false, nil
	}

	gatewayURL := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_URL"))
	if gatewayURL == "" {
		gatewayURL = "http://orchestrator:8090"
	}
	serverURL := normalizeMCPServerURL(gatewayURL)
	if serverURL == "" {
		return "", "", false, nil
	}
	allowedTools := toolNamesCSV(s.Tools)
	headers := map[string]string{
		"X-Empire-Agent-Id":      strings.TrimSpace(actor.ID),
		"X-Empire-Agent-Role":    strings.TrimSpace(actor.Role),
		"X-Empire-Agent-Mode":    strings.TrimSpace(actor.Mode),
		"X-Empire-Vertical-Id":   strings.TrimSpace(actor.VerticalID),
		"X-Empire-Allowed-Tools": allowedTools,
	}
	if token := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN")); token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	contextToken = registerMCPTurnContextWithTTL(ctx, r.mcpContextTokenTTL(ctx))
	traceID := strings.TrimSpace(contextToken)
	if contextToken != "" {
		headers["X-Empire-Context-Token"] = contextToken
	}
	if traceID != "" {
		headers["X-Empire-Trace-Id"] = traceID
	}
	serverURL = withMCPContextQuery(serverURL, actor, contextToken, allowedTools, traceID)
	cfg := map[string]any{
		"mcpServers": map[string]any{
			"empire-runtime": map[string]any{
				"type":    "http",
				"url":     serverURL,
				"headers": headers,
			},
		},
	}
	raw, marshalErr := json.Marshal(cfg)
	if marshalErr != nil {
		if contextToken != "" {
			unregisterMCPTurnContext(contextToken)
			contextToken = ""
		}
		return "", "", false, marshalErr
	}
	return string(raw), contextToken, true, nil
}

func (r *ClaudeCLIRuntime) mcpContextTokenTTL(ctx context.Context) time.Duration {
	timeout := r.effectiveCLITimeout(ctx)
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	ttl := timeout * 3
	const (
		minTTL = 45 * time.Minute
		maxTTL = 6 * time.Hour
	)
	if ttl < minTTL {
		ttl = minTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}
	return ttl
}

func shouldUseMCPBridge() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_CLAUDE_USE_MCP")))
	return v == "1" || v == "true" || v == "yes"
}

func normalizeMCPServerURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return ""
	}
	path := strings.TrimSpace(u.Path)
	switch path {
	case "", "/":
		u.Path = "/mcp"
	case "/mcp":
	default:
		// Respect explicit path when operator already targets a specific endpoint.
	}
	return strings.TrimSpace(u.String())
}

func withMCPContextQuery(rawURL string, actor models.AgentConfig, contextToken, allowedTools, traceID string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	if v := strings.TrimSpace(contextToken); v != "" {
		q.Set("empire_ctx_token", v)
	}
	if v := strings.TrimSpace(actor.ID); v != "" {
		q.Set("empire_agent_id", v)
	}
	if v := strings.TrimSpace(actor.Role); v != "" {
		q.Set("empire_agent_role", v)
	}
	if v := strings.TrimSpace(actor.Mode); v != "" {
		q.Set("empire_agent_mode", v)
	}
	if v := strings.TrimSpace(actor.VerticalID); v != "" {
		q.Set("empire_vertical_id", v)
	}
	if v := strings.TrimSpace(allowedTools); v != "" {
		q.Set("empire_allowed_tools", v)
	}
	if v := strings.TrimSpace(traceID); v != "" {
		q.Set("empire_trace_id", v)
	}
	u.RawQuery = q.Encode()
	return strings.TrimSpace(u.String())
}

func (r *ClaudeCLIRuntime) runWithPromptTransportFallback(ctx context.Context, args []string, target *WorkspaceTarget, prompt string) (*Response, promptTransportFallback, error) {
	resp, err := r.runWithInput(ctx, args, target, prompt)
	if err == nil || !isPromptArgRequiredError(err) {
		return resp, promptTransportFallback{}, err
	}
	used := promptTransportFallback{Attempted: true}
	resp, err = r.runWithPromptArg(ctx, args, target, prompt)
	if err == nil {
		used.Used = true
		log.Printf("claude cli transport fallback: switched to prompt argument mode")
	}
	return resp, used, err
}

func (r *ClaudeCLIRuntime) run(ctx context.Context, args []string, target *WorkspaceTarget) (*Response, error) {
	return r.runWithInput(ctx, args, target, "")
}

func (r *ClaudeCLIRuntime) runWithInput(ctx context.Context, args []string, target *WorkspaceTarget, input string) (*Response, error) {
	timeout := r.effectiveCLITimeout(ctx)
	if target != nil && target.Enabled() {
		if strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")) == "" {
			return nil, fmt.Errorf("%w: CLAUDE_CODE_OAUTH_TOKEN is missing", ErrClaudeAuthRequired)
		}
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := r.buildCommand(runCtx, args, target)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if strings.TrimSpace(input) != "" {
		cmd.Stdin = strings.NewReader(input)
	}

	if err := cmd.Run(); err != nil {
		stderrText := strings.TrimSpace(stderr.String())
		stdoutText := strings.TrimSpace(stdout.String())
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			errOut := summarizeCLIErrorOutput(stderrText)
			if errOut == "" {
				errOut = summarizeCLIErrorOutput(stdoutText)
			}
			if errOut == "" {
				return nil, fmt.Errorf("claude cli timeout after %s", timeout)
			}
			return nil, fmt.Errorf("claude cli timeout after %s: %s", timeout, errOut)
		}
		if isClaudeAuthOutput(stderrText) || isClaudeAuthOutput(stdoutText) {
			msg := summarizeCLIErrorOutput(stderrText)
			if msg == "" {
				msg = summarizeCLIErrorOutput(stdoutText)
			}
			if msg == "" {
				msg = "not logged in"
			}
			return nil, fmt.Errorf("%w: %s", ErrClaudeAuthRequired, msg)
		}
		errOut := summarizeCLIErrorOutput(stderrText)
		if errOut == "" {
			errOut = summarizeCLIErrorOutput(stdoutText)
		}
		if errOut == "" {
			return nil, fmt.Errorf("claude cli run failed: %w", err)
		}
		return nil, fmt.Errorf("claude cli run failed: %w, stderr=%s", err, errOut)
	}

	raw := bytes.TrimSpace(stdout.Bytes())
	resp := parseCLIResponse(raw)
	resp.Raw = raw
	return resp, nil
}

func (r *ClaudeCLIRuntime) effectiveCLITimeout(ctx context.Context) time.Duration {
	timeout := r.cfg.LLM.ClaudeCLI.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	// Factory agents routinely run deeper research loops; apply a floor to
	// prevent repeated hard-kill timeouts at the process layer.
	if actor, ok := ActorFromContext(ctx); ok {
		if strings.EqualFold(strings.TrimSpace(actor.Mode), "factory") && timeout < 300*time.Second {
			timeout = 300 * time.Second
		}
	}
	// Explicit env override wins (operator emergency control).
	if raw := strings.TrimSpace(os.Getenv("EMPIREAI_CLAUDE_TIMEOUT_SECONDS")); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			timeout = time.Duration(seconds) * time.Second
		}
	}
	return timeout
}

func isClaudeAuthOutput(raw string) bool {
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return false
	}
	return strings.Contains(msg, "not logged in") ||
		strings.Contains(msg, "please run /login") ||
		strings.Contains(msg, "authentication required") ||
		strings.Contains(msg, "oauth token")
}

func summarizeCLIErrorOutput(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	resp := parseCLIResponse([]byte(raw))
	msg := strings.TrimSpace(resp.Message.Content)
	if msg == "" {
		msg = raw
	}
	msg = strings.Join(strings.Fields(msg), " ")
	const maxLen = 240
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "..."
	}
	return msg
}

func (r *ClaudeCLIRuntime) buildCommand(ctx context.Context, args []string, target *WorkspaceTarget) *exec.Cmd {
	if target != nil && target.Enabled() {
		dockerBin := strings.TrimSpace(os.Getenv("EMPIREAI_DOCKER_BIN"))
		if dockerBin == "" {
			dockerBin = "docker"
		}
		dockerArgs := []string{"exec", "-i"}
		gatewayURL := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_URL"))
		if gatewayURL == "" {
			gatewayURL = "http://orchestrator:8090"
		}
		if gatewayURL != "" {
			dockerArgs = append(dockerArgs, "-e", "EMPIREAI_TOOL_GATEWAY_URL="+gatewayURL)
		}
		if gatewayToken := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN")); gatewayToken != "" {
			dockerArgs = append(dockerArgs, "-e", "EMPIREAI_TOOL_GATEWAY_TOKEN="+gatewayToken)
		}
		if oauthToken := strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")); oauthToken != "" {
			dockerArgs = append(dockerArgs, "-e", "CLAUDE_CODE_OAUTH_TOKEN="+oauthToken)
		}
		if strings.TrimSpace(target.Workdir) != "" {
			dockerArgs = append(dockerArgs, "-w", target.Workdir)
		}
		dockerArgs = append(dockerArgs, target.Container, r.cfg.LLM.ClaudeCLI.Command)
		dockerArgs = append(dockerArgs, args...)
		return exec.CommandContext(ctx, dockerBin, dockerArgs...)
	}
	return exec.CommandContext(ctx, r.cfg.LLM.ClaudeCLI.Command, args...)
}

func (r *ClaudeCLIRuntime) resolveWorkspace(ctx context.Context) (*WorkspaceTarget, error) {
	if r.workspaces == nil {
		return nil, nil
	}
	actor, ok := ActorFromContext(ctx)
	if !ok {
		return nil, nil
	}
	return r.workspaces.ResolveWorkspace(ctx, actor)
}

func (r *ClaudeCLIRuntime) persistTurn(ctx context.Context, turn AgentTurnRecord) {
	if r.turns == nil {
		return
	}
	if err := r.turns.AppendAgentTurn(ctx, turn); err != nil {
		log.Printf("failed to persist cli agent turn: agent=%s session=%s err=%v", turn.AgentID, turn.SessionID, err)
	}
}

func (r *ClaudeCLIRuntime) persistConversation(ctx context.Context, s *Session) {
	if r.conversations == nil || s == nil {
		return
	}
	mode := strings.TrimSpace(s.ConversationMode)
	if mode == "" {
		mode = "session"
	}
	if err := r.conversations.UpsertConversation(ctx, ConversationRecord{
		AgentID:   s.AgentID,
		ScopeKey:  strings.TrimSpace(s.ScopeKey),
		Mode:      mode,
		Messages:  s.Messages,
		Summary:   buildSessionSummary(s),
		TurnCount: s.TurnCount,
		Status:    "active",
	}); err != nil {
		log.Printf("failed to persist cli conversation: agent=%s err=%v", s.AgentID, err)
	}
}

func parseCLIResponse(raw []byte) *Response {
	resp := &Response{
		Message: Message{Role: "assistant"},
	}
	if len(raw) == 0 {
		return resp
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		if sid := strings.TrimSpace(asString(obj["session_id"])); sid != "" {
			resp.SessionID = sid
		}
		if resp.SessionID == "" {
			if sid := strings.TrimSpace(asString(obj["sessionId"])); sid != "" {
				resp.SessionID = sid
			}
		}
		texts := make([]string, 0, 4)
		if v, ok := obj["result"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["content"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["message"].(string); ok {
			texts = append(texts, v)
		}
		if v, ok := obj["output"].(string); ok {
			texts = append(texts, v)
		}
		if content, ok := obj["content"].([]any); ok {
			for _, item := range content {
				m, _ := item.(map[string]any)
				typ := strings.TrimSpace(strings.ToLower(asString(m["type"])))
				switch typ {
				case "text":
					text := strings.TrimSpace(asString(m["text"]))
					if text != "" {
						texts = append(texts, text)
					}
				case "tool_use":
					name := strings.TrimSpace(asString(m["name"]))
					if name == "" {
						continue
					}
					args := m["input"]
					if args == nil {
						args = m["arguments"]
					}
					resp.ToolCalls = append(resp.ToolCalls, ToolCall{
						Name:      name,
						Arguments: args,
					})
				}
			}
		}
		if calls, ok := obj["tool_calls"].([]any); ok {
			for _, c := range calls {
				m, _ := c.(map[string]any)
				name := strings.TrimSpace(asString(m["name"]))
				if name == "" {
					continue
				}
				args := m["arguments"]
				if args == nil {
					args = m["input"]
				}
				resp.ToolCalls = append(resp.ToolCalls, ToolCall{
					Name:      name,
					Arguments: args,
				})
			}
		}
		if len(texts) > 0 {
			resp.Message.Content = strings.TrimSpace(strings.Join(texts, "\n"))
			return resp
		}
		if len(resp.ToolCalls) > 0 {
			return resp
		}
	}

	resp.Message.Content = strings.TrimSpace(string(raw))
	return resp
}

func dedupeToolCalls(calls []ToolCall) []ToolCall {
	if len(calls) <= 1 {
		return calls
	}
	type key struct {
		name string
		args string
	}
	seen := map[key]struct{}{}
	out := make([]ToolCall, 0, len(calls))
	for _, c := range calls {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		argsRaw, _ := json.Marshal(c.Arguments)
		k := key{name: name, args: string(argsRaw)}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, c)
	}
	return out
}

type sessionIDAdopter interface {
	AdoptSessionID(agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error
}

func adoptRegistrySessionID(reg SessionRegistry, agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
	if reg == nil {
		return nil
	}
	adopter, ok := reg.(sessionIDAdopter)
	if !ok {
		return nil
	}
	return adopter.AdoptSessionID(agentID, runtimeMode, lockOwner, newSessionID, scopeKey)
}

func claudeToolsArg(tools []ToolDefinition) string {
	if len(tools) == 0 {
		return ""
	}
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	b, err := json.Marshal(names)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func estimateCLIUsageTokens(in Message, out *Response, actor models.AgentConfig) UsageTokens {
	// This is intentionally crude. Claude Code does not currently expose usage
	// metadata in a stable non-interactive way, so we approximate from payload sizes
	// and apply a role-based floor to avoid undercounting long-session context.
	inText := strings.TrimSpace(in.Content)
	outRaw := []byte{}
	if out != nil && len(out.Raw) > 0 {
		outRaw = out.Raw
	}

	inTokens := estimateTokensFromBytes([]byte(inText))
	outTokens := estimateTokensFromBytes(outRaw)

	minIn := 800
	role := strings.ToLower(strings.TrimSpace(actor.Role))
	id := strings.ToLower(strings.TrimSpace(actor.ID))
	switch {
	case strings.Contains(role, "ceo") || strings.Contains(id, "ceo"):
		minIn = 2000
	case strings.Contains(role, "cto") || strings.Contains(id, "cto"):
		minIn = 1200
	case actor.Mode == "factory":
		minIn = 1200
	}
	if inTokens < minIn {
		inTokens = minIn
	}
	if outTokens < 200 {
		outTokens = 200
	}

	// BudgetTracker only needs model string for tier detection. For CLI mode we use
	// the configured model tier (e.g. "haiku" or "sonnet") from actor.Type.
	model := strings.TrimSpace(actor.Type)

	return UsageTokens{
		InputTokens:  inTokens,
		OutputTokens: outTokens,
		Model:        model,
	}
}

func estimateTokensFromBytes(b []byte) int {
	// Rough: ~4 bytes per token for English/ASCII-heavy text.
	// Clamp to zero for empty payloads.
	if len(b) == 0 {
		return 0
	}
	return (len(b) + 3) / 4
}

func toolNamesCSV(tools []ToolDefinition) string {
	if len(tools) == 0 {
		return ""
	}
	names := make([]string, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return strings.Join(names, ",")
}

func buildInitialPrompt(s *Session, firstMessage string) string {
	var b strings.Builder
	if strings.TrimSpace(s.SystemPrompt) != "" {
		b.WriteString("System: ")
		b.WriteString(s.SystemPrompt)
		b.WriteString("\n\n")
	}
	if len(s.Tools) > 0 {
		b.WriteString("Tools:\n")
		for _, t := range s.Tools {
			b.WriteString("- ")
			b.WriteString(t.Name)
			if t.Description != "" {
				b.WriteString(": ")
				b.WriteString(t.Description)
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	b.WriteString(firstMessage)
	return b.String()
}

func jsonBytes(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func permissionModeArgs() []string {
	args := make([]string, 0, 3)
	if mode := strings.TrimSpace(os.Getenv("EMPIREAI_CLAUDE_PERMISSION_MODE")); mode != "" {
		args = append(args, "--permission-mode", mode)
	}
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_CLAUDE_BYPASS_PERMISSIONS")))
	if v == "1" || v == "true" || v == "yes" {
		args = append(args, "--dangerously-skip-permissions")
	}
	return args
}
