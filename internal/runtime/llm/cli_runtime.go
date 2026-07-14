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
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type ClaudeCLIRuntime struct {
	cfg                  *config.Config
	sessions             sessions.Registry
	conversations        ConversationPersistence
	lockOwner            string
	workspaces           workspace.Resolver
	monitor              MonitorSink
	events               EventPublisher
	mcpTurns             MCPTurnContextStore
	toolGateway          toolgateway.Binding
	providerCredentials  ProviderCredentialResolver
	execWorkspaceFn      func(ctx context.Context, target *workspace.Target, stdin string, args ...string) ([]byte, []byte, int, error)
	providerAdmission    *ProviderAdmissionRegistry
	completionController *runtimeeffects.Controller
}

var ErrClaudeWorkspaceRequired = errors.New("claude workspace target required")
var ErrClaudeWorkspaceCLIUnavailable = errors.New("claude workspace cli unavailable")

type promptTransportFallback struct {
	Attempted bool
	Used      bool
}

type ClaudeCLIRuntimeOptions struct {
	MonitorSink          MonitorSink
	MCPTurnContextStore  MCPTurnContextStore
	ToolGateway          toolgateway.Binding
	ProviderCredentials  ProviderCredentialResolver
	CompletionController *runtimeeffects.Controller
}

func NewClaudeCLIRuntime(
	cfg *config.Config,
	sessions sessions.Registry,
	lockOwner string,
	workspaces workspace.Resolver,
	conversations ConversationPersistence,
	publisher EventPublisher,
) *ClaudeCLIRuntime {
	return NewClaudeCLIRuntimeWithOptions(cfg, sessions, lockOwner, workspaces, conversations, publisher, ClaudeCLIRuntimeOptions{})
}

func NewClaudeCLIRuntimeWithOptions(
	cfg *config.Config,
	sessions sessions.Registry,
	lockOwner string,
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
		cfg:                  cfg,
		sessions:             sessions,
		conversations:        conversations,
		lockOwner:            lockOwner,
		workspaces:           workspaces,
		monitor:              monitor,
		events:               publisher,
		mcpTurns:             opts.MCPTurnContextStore,
		toolGateway:          opts.ToolGateway,
		providerCredentials:  providerCredentials,
		providerAdmission:    NewProviderAdmissionRegistry(cfg),
		completionController: opts.CompletionController,
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
			SupportsMemoryPlans:       true,
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
			PersistsStatelessAudit:        true,
		},
		Budget: ProviderBudgetContract{
			UsageAccounting: BudgetUsageExact,
		},
	}
}

func (r *ClaudeCLIRuntime) PersistConversationSnapshot(ctx context.Context, s *Session) error {
	if r.conversations == nil || s == nil {
		return nil
	}
	record, persist, err := memoryConversationRecord(s)
	if err != nil || !persist {
		return err
	}
	return r.conversations.UpsertConversation(ctx, record)
}

func (r *ClaudeCLIRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []ToolDefinition) (*Session, error) {
	lease, hydrated, resolved, err := startMemory(ctx, r.sessions, r.conversations, agentID, r.lockOwner)
	if err != nil {
		return nil, err
	}
	if resolved.Enabled() {
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
		AgentID:        agentID,
		Memory:         resolved.Plan,
		MemoryIdentity: resolved.Identity,
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
		Messages:     append([]Message(nil), hydrated.Messages...),
		TurnCount:    hydrated.TurnCount,
		Watchdog:     hydrated.Watchdog,
	}
	if resolved.Enabled() {
		s.RetryReason = strings.TrimSpace(hydrated.RetryReason)
		s.RetriesFromSessionID = strings.TrimSpace(hydrated.RetriesFromSessionID)
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
	disallowedBuiltinTools := claudeDisallowedBuiltinToolsArgForActor(actor, s.Tools)
	allowedToolsArg := claudeAllowedToolsArgForActor(actor, s.Tools)

	lease, resolved, err := acquireContinuedMemory(ctx, r.sessions, s, r.lockOwner)
	if err != nil {
		return nil, sessionAcquireFailure(err, s.AgentID)
	}
	if resolved.Enabled() {
		defer func() { _ = r.sessions.Release(ctx, lease) }()
		stopLeaseHeartbeat := sessions.StartLeaseHeartbeatWithErrorHandler(ctx, r.sessions, lease, func(heartbeatErr error) {
			logPublisherRuntime(ctx, r.events, "warn", "session_lease_heartbeat_failed", "Refreshing the CLI session lease heartbeat failed", s.AgentID, s.ID, entityID, map[string]any{
				"run_id": resolved.Identity.RunID, "flow_instance": resolved.Identity.FlowInstance,
			}, heartbeatErr)
		})
		defer stopLeaseHeartbeat()

		if lease.SessionID != s.ID {
			LogSessionAdoptedForRun(ctx, r.events, resolved.Identity, s.ID, lease.SessionID)
			s.ID = lease.SessionID
		}
		if sid := strings.TrimSpace(lease.ProviderSessionID); sid != "" {
			s.ProviderSessionID = sid
		}
	}
	if err := requireInboundDeliveryActiveForSession(ctx, r.events, s, "error", "Marking the reused agent delivery in progress failed", map[string]any{
		"memory_enabled": resolved.Enabled(), "run_id": resolved.Identity.RunID, "flow_instance": resolved.Identity.FlowInstance,
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
		"memory_enabled":          resolved.Enabled(),
		"disallowed_tools":        disallowedBuiltinTools,
		"mcp_enabled":             mcpEnabled,
		"output_format":           configuredCLIOutputFormat(r.cfg),
		"permission_mode_args":    permissionModeArgs(),
		"prompt":                  prompt,
		"run_id":                  resolved.Identity.RunID,
		"flow_instance":           resolved.Identity.FlowInstance,
		"system_prompt":           strings.TrimSpace(s.SystemPrompt),
		"tools":                   s.Tools,
		"turn_count":              s.TurnCount,
	})
	ctx, completionTargetID, err := prepareCompletionContext(ctx, r.completionController, r.cfg, s, entityID)
	if err != nil {
		return nil, err
	}
	attempt, err := runtimeeffects.BeginCompletion(ctx, claudeCLICompletionAdapter, requestFingerprintInput, nil)
	if err != nil {
		return nil, err
	}
	dispatch := &completionDispatch{handle: attempt}
	childSessionID := strings.TrimSpace(attempt.Attempt().AttemptID)
	if childSessionID == "" {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "claude_attempt_identity_missing", "claude-cli-adapter", "prepare_turn", nil)
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	completionModel := strings.TrimSpace(actor.ResolvedModel)
	if completionModel == "" {
		completionModel, _ = llmselection.ResolveModelName(profile, llmselection.ModelResolution{Model: actor.Model})
	}
	completionModel = coalesce(completionModel, "unknown")

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
		Memory:                   resolved.Plan,
		MemoryIdentity:           resolved.Identity,
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
	requestPayload := jsonBytes(map[string]any{
		"args":                          args,
		"message":                       message,
		"provider_session_id":           strings.TrimSpace(s.ProviderSessionID),
		"prompt_arg_fallback_attempted": transportFallback.Attempted,
		"prompt_arg_fallback_used":      transportFallback.Used,
	})
	if err != nil {
		turn := enrichTurnRecord(ctx, s, completionTurnBase(ctx, s, requestPayload, nil, false, latency, agentTurnFailure(err, "claude_cli_turn")), nil)
		state := claudeCompletionFailureState(err)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, nil, profile, unavailableCompletionUsage(completionModel), state, turn.Failure, map[string]any{"stage": "provider_call"}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		projectionErr := requireCurrentProviderProjection(ctx, s.AgentID)
		if projectionErr == nil {
			s.ParseFailures++
		}
		if projectionErr == nil && resolved.Enabled() {
			if rotated, rotateErr := MaybeRotateAfterParseFailures(ctx, s, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateOnParseFailures, r.events); rotateErr == nil && rotated != nil {
				lease = rotated
			}
		}
		if projectionErr != nil {
			return nil, errors.Join(err, projectionErr)
		}
		return nil, err
	}
	usage, usageErr := claudeCompletionUsageFromRaw(resp.Raw, completionModel)
	if usageErr != nil {
		usage = unavailableCompletionUsage(completionModel)
	}
	if err := validateCLIResponseToolCallsForTurn(actor, s.Tools, resp); err != nil {
		turn := enrichTurnRecord(ctx, s, completionTurnBase(ctx, s, requestPayload, resp.Raw, true, latency, agentTurnFailure(err, "claude_cli_tool_validation")), resp)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, resp, profile, usage, runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "validate_tool_calls", "provider_session_id": strings.TrimSpace(resp.SessionID)}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		if projectionErr := requireCurrentProviderProjection(ctx, s.AgentID); projectionErr != nil {
			return nil, errors.Join(err, projectionErr)
		}
		return nil, err
	}
	if returnedSessionID := strings.TrimSpace(resp.SessionID); returnedSessionID != childSessionID {
		err := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "claude_provider_child_identity_mismatch", "claude-cli-adapter", "validate_response", map[string]any{"expected_provider_session_id": childSessionID, "returned_provider_session_id": returnedSessionID})
		turn := enrichTurnRecord(ctx, s, completionTurnBase(ctx, s, requestPayload, resp.Raw, false, latency, agentTurnFailure(err, "claude_cli_identity_validation")), resp)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, resp, profile, usage, runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "validate_response", "expected_provider_session_id": childSessionID, "returned_provider_session_id": returnedSessionID}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		return nil, err
	}

	if err := requireCurrentProviderProjection(ctx, s.AgentID); err != nil {
		turn := enrichTurnRecord(ctx, s, completionTurnBase(ctx, s, requestPayload, resp.Raw, true, latency, agentTurnFailure(err, "claude_cli_projection")), resp)
		if settleErr := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, resp, profile, usage, runtimeeffects.StateOutcomeUncertain, turn.Failure, map[string]any{"stage": "project_provider_turn", "provider_session_id": childSessionID}); settleErr != nil {
			return nil, errors.Join(err, settleErr)
		}
		return nil, err
	}
	settlementEvidence := map[string]any{"response_fingerprint": runtimeeffects.Fingerprint(resp.Raw), "provider_session_id": childSessionID}
	if usageErr != nil {
		settlementEvidence["usage_exactness"] = string(runtimeeffects.CompletionUsageUnavailable)
	}
	turn := enrichTurnRecord(ctx, s, completionTurnBase(ctx, s, requestPayload, resp.Raw, true, latency, nil), resp)
	if !resolved.Enabled() {
		if err := settleCompletionTurn(ctx, dispatch, completionTargetID, turn, resp, profile, usage, runtimeeffects.StateSettled, nil, settlementEvidence); err != nil {
			return nil, err
		}
	} else {
		if lease == nil {
			return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "claude_session_lease_missing", "claude-cli-adapter", "settle_provider_head", nil)
		}
		if err := settleCompletionTurnWithProviderHead(ctx, dispatch, completionTargetID, turn, resp, profile, usage, runtimeeffects.StateSettled, nil, settlementEvidence, &runtimeeffects.CompletionProviderHead{
			Identity: resolved.Identity, SessionID: s.ID, LockOwner: lease.LockOwner,
			ExpectedProviderHead: confirmedHead, NewProviderHead: childSessionID,
		}); err != nil {
			return nil, err
		}
	}
	s.Messages = append(s.Messages, message, resp.Message)
	s.ProviderSessionID = childSessionID
	if resolved.Enabled() {
		LogSessionAdoptedForRun(ctx, r.events, resolved.Identity, confirmedHead, childSessionID)
	}
	s.TurnCount++
	s.ParseFailures = 0

	if resolved.Enabled() {
		if err := r.sessions.IncrementTurn(ctx, resolved.Identity, s.ID); err != nil {
			return nil, err
		}
	}

	r.persistConversation(ctx, s)

	if resolved.Enabled() {
		if rotated, rotateErr := MaybeRotateAfterTurn(ctx, s, r.sessions, r.lockOwner, r.cfg.LLM.Session.RotateAfterTurns, r.events); rotateErr == nil && rotated != nil {
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
