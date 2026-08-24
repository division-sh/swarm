package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

type completionDispatch struct {
	handle        *runtimeeffects.Handle
	state         runtimeeffects.State
	evidence      map[string]any
	providerModel llmselection.ResolvedModel
	invocation    completionProviderInvocation
	request       []byte
	projection    *completionProjection
	continuation  json.RawMessage
}

type completionProviderInvocation uint8

const (
	completionProviderInvocationUnclassified completionProviderInvocation = iota
	completionProviderInvocationNotStarted
	completionProviderInvocationStarted
)

func newCompletionDispatch(handle *runtimeeffects.Handle, state runtimeeffects.State) *completionDispatch {
	return &completionDispatch{
		handle:     handle,
		state:      state,
		invocation: completionProviderInvocationNotStarted,
	}
}

func (d *completionDispatch) markProviderInvocationStarted() {
	if d != nil {
		d.invocation = completionProviderInvocationStarted
	}
}

const completionContinuationVersion = "provider-response-continuation.v1"

type completionProjection struct {
	SessionID         string               `json:"session_id"`
	ExpectedTurnCount int                  `json:"expected_turn_count"`
	TurnCount         int                  `json:"turn_count"`
	Messages          []Message            `json:"messages"`
	Identity          agentmemory.Identity `json:"identity"`
	Memory            agentmemory.Plan     `json:"memory"`
	FrameID           string               `json:"frame_id,omitempty"`
}

type completionContinuationEnvelope struct {
	Version    string                         `json:"version"`
	Adapter    string                         `json:"adapter"`
	Response   Response                       `json:"response"`
	Usage      runtimeeffects.CompletionUsage `json:"usage"`
	Projection completionProjection           `json:"projection"`
}

func bindCompletionProjection(dispatch *completionDispatch, session *Session, input Message, managed *managedProviderCall) error {
	if dispatch == nil || dispatch.handle == nil || session == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_projection_input_missing", "llm-completion-authority", "bind_projection", nil)
	}
	frameID := ""
	if managed != nil {
		frameID = strings.TrimSpace(managed.frame.FrameID)
	}
	dispatch.projection = &completionProjection{
		SessionID:         strings.TrimSpace(session.ID),
		ExpectedTurnCount: session.TurnCount,
		TurnCount:         session.TurnCount + 1,
		Messages:          append(append([]Message(nil), session.Messages...), input),
		Identity:          session.MemoryIdentity.Normalize(),
		Memory:            session.Memory,
		FrameID:           frameID,
	}
	return nil
}

func completionContinuationForHandle(handle *runtimeeffects.Handle, adapter string) (*completionContinuationEnvelope, error) {
	if handle == nil {
		return nil, nil
	}
	snapshot, ok := handle.CompletionContinuation()
	if !ok {
		return nil, nil
	}
	return completionContinuationForPayload(snapshot.Payload, adapter)
}

func completionContinuationForPayload(payload json.RawMessage, adapter string) (*completionContinuationEnvelope, error) {
	var continuation completionContinuationEnvelope
	if err := json.Unmarshal(payload, &continuation); err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_continuation_decode_failed", "llm-completion-authority", "recover_completion", nil, err)
	}
	if err := validateCompletionContinuation(continuation, adapter); err != nil {
		return nil, err
	}
	continuation.Response.CapabilitySurface = nil
	return &continuation, nil
}

func validateCompletionContinuation(continuation completionContinuationEnvelope, adapter string) error {
	projection := continuation.Projection
	if continuation.Version != completionContinuationVersion || continuation.Adapter != strings.TrimSpace(adapter) ||
		strings.TrimSpace(continuation.Response.Message.Role) == "" || len(continuation.Response.Raw) == 0 ||
		strings.TrimSpace(projection.SessionID) == "" || projection.ExpectedTurnCount < 0 ||
		projection.TurnCount != projection.ExpectedTurnCount+1 || len(projection.Messages) < 2 {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_continuation_payload_invalid", "llm-completion-authority", "recover_completion", map[string]any{"adapter": strings.TrimSpace(adapter), "version": continuation.Version})
	}
	if !reflect.DeepEqual(projection.Messages[len(projection.Messages)-1], continuation.Response.Message) {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_continuation_projection_mismatch", "llm-completion-authority", "recover_completion", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if err := continuation.Usage.Validate(); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_continuation_usage_invalid", "llm-completion-authority", "recover_completion", nil, err)
	}
	if continuation.Response.ToolOutputAuthority == nil || continuation.Response.ToolOutputAuthority.Validate() != nil {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_continuation_tool_output_authority_invalid", "llm-completion-authority", "recover_completion", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if _, err := projection.Memory.Normalize(); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_continuation_memory_invalid", "llm-completion-authority", "recover_completion", nil, err)
	}
	return nil
}

func ValidateCompletionToolContinuation(payload json.RawMessage, adapter string, successor agentframe.ToolContinuation) error {
	continuation, err := completionContinuationForPayload(payload, adapter)
	if err != nil {
		return err
	}
	if continuation == nil || successor.Validate() != nil || strings.TrimSpace(continuation.Projection.FrameID) == "" ||
		successor.ParentFrameID() != strings.TrimSpace(continuation.Projection.FrameID) {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_successor_frame_mismatch", "llm-completion-authority", "consume_completion", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	return nil
}

func recoverCompletionContinuation(ctx context.Context, controller *runtimeeffects.Controller, session *Session, adapter string) (*Response, bool, error) {
	if controller == nil || session == nil {
		return nil, false, nil
	}
	handle, found, err := controller.RecoverCompletionContinuation(ctx, session.ID, session.Memory)
	if err != nil || !found {
		return nil, found, err
	}
	continuation, err := completionContinuationForHandle(handle, adapter)
	if err != nil {
		return nil, true, err
	}
	projection := continuation.Projection
	if projection.Identity.Normalize() != session.MemoryIdentity.Normalize() || projection.Memory != session.Memory ||
		(projection.Memory.Enabled && projection.SessionID != strings.TrimSpace(session.ID)) {
		return nil, true, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_session_stale", "llm-completion-authority", "recover_completion", map[string]any{"session_id": strings.TrimSpace(session.ID)})
	}
	if !projection.Memory.Enabled {
		projection.SessionID = strings.TrimSpace(session.ID)
	}
	messages, err := json.Marshal(projection.Messages)
	if err != nil {
		return nil, true, fmt.Errorf("marshal completion continuation projection: %w", err)
	}
	snapshot, _ := handle.CompletionContinuation()
	if err := handle.ProjectCompletionConversation(ctx, runtimeeffects.CompletionConversationProjection{
		Payload: snapshot.Payload, SessionID: projection.SessionID, Identity: projection.Identity,
		Memory: projection.Memory, ExpectedTurnCount: projection.ExpectedTurnCount,
		TurnCount: projection.TurnCount, Messages: messages,
	}); err != nil {
		return nil, true, err
	}
	session.Messages = append([]Message(nil), projection.Messages...)
	session.TurnCount = projection.TurnCount
	session.ParseFailures = 0
	if providerSessionID := strings.TrimSpace(continuation.Response.SessionID); providerSessionID != "" {
		session.ProviderSessionID = providerSessionID
	}
	response := continuation.Response
	response.CapabilitySurface = &snapshot.Surface
	response.completionHandle = handle
	response.completionFrameID = projection.FrameID
	response.completionConsumed = snapshot.Phase == runtimeeffects.CompletionProjectionResponseConsumed
	if successor, ok := snapshot.ToolContinuation(); ok {
		response.completionSuccessor = &successor
	}
	return &response, true, nil
}

func projectCompletionContinuation(ctx context.Context, dispatch *completionDispatch, session *Session, response *Response) (bool, error) {
	if dispatch == nil || dispatch.handle == nil || session == nil || response == nil {
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_projection_input_missing", "llm-completion-authority", "project_completion", nil)
	}
	attempt := dispatch.handle.Attempt()
	if attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent || attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return false, nil
	}
	continuation, err := completionContinuationForPayload(dispatch.continuation, attempt.Adapter)
	if err != nil {
		return true, err
	}
	if continuation == nil {
		return true, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_continuation_missing", "llm-completion-authority", "project_completion", nil)
	}
	projection := continuation.Projection
	messages, err := json.Marshal(projection.Messages)
	if err != nil {
		return true, fmt.Errorf("marshal completion conversation projection: %w", err)
	}
	if err := dispatch.handle.ProjectCompletionConversation(ctx, runtimeeffects.CompletionConversationProjection{
		Payload: dispatch.continuation, SessionID: projection.SessionID, Identity: projection.Identity,
		Memory: projection.Memory, ExpectedTurnCount: projection.ExpectedTurnCount,
		TurnCount: projection.TurnCount, Messages: messages,
	}); err != nil {
		return true, err
	}
	session.Messages = append([]Message(nil), projection.Messages...)
	session.TurnCount = projection.TurnCount
	session.ParseFailures = 0
	response.completionHandle = dispatch.handle
	response.completionFrameID = projection.FrameID
	return true, nil
}

func consumeCompletionContinuation(ctx context.Context, response *Response, successor *agentframe.ToolContinuation) error {
	if response == nil || response.completionHandle == nil {
		return nil
	}
	return response.completionHandle.ConsumeCompletionResponse(ctx, successor)
}

const (
	completionAttemptHeartbeatInterval = 30 * time.Second
	completionAttemptHeartbeatLease    = 2 * time.Minute
)

type completionAttemptHeartbeat struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	done         chan struct{}
	handle       *runtimeeffects.Handle
	lease        time.Duration
	claimHandoff *runtimedelivery.ClaimRenewalHandoff
	renewMu      sync.Mutex
	mu           sync.Mutex
	err          error
	stopped      bool
}

type completionAttemptHeartbeatContextKey struct{}

func startCompletionAttemptHeartbeat(ctx context.Context, handle *runtimeeffects.Handle) (context.Context, *completionAttemptHeartbeat, error) {
	return startCompletionAttemptHeartbeatWithTiming(ctx, handle, completionAttemptHeartbeatInterval, completionAttemptHeartbeatLease)
}

func startCompletionAttemptHeartbeatWithTiming(ctx context.Context, handle *runtimeeffects.Handle, interval, lease time.Duration) (context.Context, *completionAttemptHeartbeat, error) {
	if handle == nil {
		return ctx, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	if interval <= 0 || lease <= 0 {
		return ctx, nil, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_heartbeat_timing_invalid", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	var workLease *worklifetime.Lease
	var err error
	normalProviderAttempt := handle.Attempt().Kind == runtimeeffects.KindProviderTurn && handle.Attempt().Authority.Kind == runtimeeffects.AuthorityNormalAgent
	owner, occurrenceOwned := worklifetime.OccurrenceFromContext(ctx)
	if normalProviderAttempt {
		process, processOwned := worklifetime.ProcessFromContext(ctx)
		if !processOwned {
			return ctx, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_process_work_owner_missing", "llm-completion-authority", "heartbeat_attempt", nil)
		}
		workLease, err = process.Begin(context.WithoutCancel(ctx))
	} else if occurrenceOwned {
		workLease, err = owner.Begin(ctx)
	} else if process, processOwned := worklifetime.ProcessFromContext(ctx); processOwned {
		workLease, err = process.Begin(ctx)
	} else {
		return ctx, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_heartbeat_work_owner_missing", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	if err != nil {
		return ctx, nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_heartbeat_admission_failed", "llm-completion-authority", "heartbeat_attempt", nil, err)
	}
	heartbeatParent := workLease.Context()
	if occurrenceOwned && !normalProviderAttempt {
		heartbeatParent = worklifetime.WithOccurrence(heartbeatParent, owner)
	}
	var claimHandoff *runtimedelivery.ClaimRenewalHandoff
	if normalProviderAttempt {
		if claimHeartbeat, ok := runtimedelivery.ClaimHeartbeatFromContext(ctx); ok {
			claimHandoff, err = claimHeartbeat.BeginRenewalHandoff()
			if err != nil {
				_ = workLease.Done()
				return ctx, nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_origin_renewal_handoff_failed", "llm-completion-authority", "heartbeat_attempt", map[string]any{"attempt_id": handle.Attempt().AttemptID}, err)
			}
		}
	}
	if err := handle.Heartbeat(heartbeatParent, lease); err != nil {
		claimHandoff.Finish()
		_ = workLease.Done()
		return ctx, nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_failed", "llm-completion-authority", "heartbeat_attempt", map[string]any{"stage": "prelaunch"}, err)
	}
	heartbeatCtx, cancel := context.WithCancelCause(heartbeatParent)
	heartbeat := &completionAttemptHeartbeat{
		ctx: heartbeatCtx, cancel: cancel, done: make(chan struct{}), handle: handle, lease: lease,
		claimHandoff: claimHandoff,
	}
	go func() {
		defer close(heartbeat.done)
		defer func() { _ = workLease.Done() }()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := heartbeat.renew(); err != nil {
					if heartbeatCtx.Err() != nil {
						return
					}
					heartbeat.mu.Lock()
					heartbeat.err = err
					heartbeat.mu.Unlock()
					cancel(err)
					return
				}
			}
		}
	}()
	return context.WithValue(heartbeatCtx, completionAttemptHeartbeatContextKey{}, heartbeat), heartbeat, nil
}

func requireCompletionAttemptHeartbeat(ctx context.Context) error {
	if ctx == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_missing", "llm-completion-authority", "launch_attempt", nil)
	}
	heartbeat, ok := ctx.Value(completionAttemptHeartbeatContextKey{}).(*completionAttemptHeartbeat)
	if !ok || heartbeat == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_attempt_heartbeat_missing", "llm-completion-authority", "launch_attempt", nil)
	}
	return nil
}

func (h *completionAttemptHeartbeat) Stop() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	doStop := !h.stopped
	if doStop {
		h.stopped = true
	}
	h.mu.Unlock()
	if doStop {
		renewErr := h.renew()
		h.mu.Lock()
		h.err = errors.Join(h.err, renewErr)
		h.mu.Unlock()
		h.cancel(nil)
	}
	<-h.done
	h.claimHandoff.Finish()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *completionAttemptHeartbeat) renew() error {
	if h == nil || h.handle == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	h.renewMu.Lock()
	defer h.renewMu.Unlock()
	return h.handle.Heartbeat(h.ctx, h.lease)
}

func finishCompletionAttemptHeartbeat(heartbeat *completionAttemptHeartbeat, prior error) error {
	if heartbeat == nil {
		return prior
	}
	heartbeatErr := heartbeat.Stop()
	if heartbeatErr == nil {
		return prior
	}
	return errors.Join(prior, completionAttemptHeartbeatLoss(heartbeatErr))
}

func finishCompletionDispatchHeartbeat(dispatch *completionDispatch, heartbeat *completionAttemptHeartbeat, prior error) error {
	if heartbeat == nil {
		return prior
	}
	heartbeatErr := heartbeat.Stop()
	if heartbeatErr == nil {
		return prior
	}
	if dispatch != nil && dispatch.invocation == completionProviderInvocationStarted {
		dispatch.state = runtimeeffects.StateOutcomeUncertain
	}
	return errors.Join(prior, completionAttemptHeartbeatLoss(heartbeatErr))
}

func completionAttemptHeartbeatLoss(err error) error {
	return runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "completion_attempt_heartbeat_lost", "llm-completion-authority", "heartbeat_attempt", map[string]any{"stage": "provider_execution"}, err)
}

func prepareCompletionContext(ctx context.Context, controller *runtimeeffects.Controller, cfg *config.Config, session *Session, entityID string) (context.Context, string, error) {
	if controller == nil || !controller.Enabled() {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_controller_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	if session == nil {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_session_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	ctx = runtimeeffects.WithLogicalOperationIdentitySegment(ctx, fmt.Sprintf("completion:%d", session.TurnCount+1))
	ctx = runtimeeffects.WithController(ctx, controller)
	authority, ok := runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	var target runtimeeffects.UsageTarget
	if authority.Kind == runtimeeffects.AuthorityConversationForkChat {
		ordinal := 1
		if session != nil {
			ordinal = session.TurnCount + 1
		}
		target = runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetConversationForkCompletion, ID: authority.ForkChat.ForkTurnID, Ordinal: ordinal}
	} else {
		actor, _ := runtimeactors.ActorFromContext(ctx)
		effectiveEntityID := strings.TrimSpace(actor.EffectiveEntityID())
		effectiveFlowInstance := session.MemoryIdentity.FlowInstance()
		if effectiveEntityID == "" {
			if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
				effectiveEntityID = strings.TrimSpace(inbound.EntityID())
			}
		}
		if effectiveEntityID == "" {
			effectiveEntityID = strings.TrimSpace(entityID)
		}
		turnID := uuid.NewString()
		if capabilityAuthority, ok := providerTurnAuthorityFromContext(ctx); ok {
			turnID = capabilityAuthority.ID
		}
		target = runtimeeffects.UsageTarget{
			Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID,
			RunID: strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx)), AgentID: strings.TrimSpace(session.AgentID),
			AgentIdentity: session.MemoryIdentity.Agent, SessionID: strings.TrimSpace(session.ID), Memory: session.Memory,
			FlowInstance: effectiveFlowInstance, EntityID: effectiveEntityID,
		}
		if session.Memory.Enabled {
			target.RunID = session.MemoryIdentity.RunID
			target.FlowInstance = session.MemoryIdentity.FlowInstance()
		}
		entityID = effectiveEntityID
	}
	if surface, ok := managedcapabilities.FromContext(ctx); ok {
		if !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(target, surface) {
			return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_capability_turn_identity_mismatch", "llm-completion-authority", "prepare_completion", map[string]any{"surface_id": surface.ID, "surface_authority_id": surface.Authority.ID, "target_id": target.ID})
		}
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	if mode, found := runtimeeffects.ExecutionModeFromContext(ctx); !found || mode != runtimeeffects.ExecutionModeMock {
		ctx = runtimeeffects.WithBudgetAdmissionScopes(ctx, completionBudgetScopes(cfg, entityID))
	}
	authority, ok = runtimeeffects.CompletionAuthorityFromContext(ctx)
	if !ok || !authority.Target.Valid() {
		return ctx, "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_usage_target_missing", "llm-completion-authority", "prepare_completion", nil)
	}
	return ctx, target.ID, nil
}

func completionBudgetScopes(cfg *config.Config, entityID string) []runtimeeffects.BudgetAdmissionScope {
	if cfg == nil {
		return nil
	}
	budget := cfg.Budget()
	scopes := make([]runtimeeffects.BudgetAdmissionScope, 0, 2)
	if budget.SystemMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "system", CapUSD: float64(budget.SystemMonthlyCap)})
	}
	if entityID = strings.TrimSpace(entityID); entityID != "" && budget.PerEntityMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "entity", Key: entityID, CapUSD: float64(budget.PerEntityMonthlyCap)})
	} else if entityID == "" && budget.GlobalMonthlyCap > 0 {
		scopes = append(scopes, runtimeeffects.BudgetAdmissionScope{Kind: "global", CapUSD: float64(budget.GlobalMonthlyCap)})
	}
	return scopes
}

func settleCompletionTurn(ctx context.Context, dispatch *completionDispatch, targetID string, turn AgentTurnRecord, response *Response, profile llmselection.Profile, usage runtimeeffects.CompletionUsage, state runtimeeffects.State, failure *runtimefailures.Envelope, evidence map[string]any) (runtimeeffects.CompletionSettlementResult, error) {
	return settleCompletionTurnWithProviderHead(ctx, dispatch, targetID, turn, response, profile, usage, state, failure, evidence, nil)
}

func settleCompletionTurnWithProviderHead(ctx context.Context, dispatch *completionDispatch, targetID string, turn AgentTurnRecord, response *Response, profile llmselection.Profile, usage runtimeeffects.CompletionUsage, state runtimeeffects.State, failure *runtimefailures.Envelope, evidence map[string]any, providerHead *runtimeeffects.CompletionProviderHead) (runtimeeffects.CompletionSettlementResult, error) {
	if dispatch == nil || dispatch.handle == nil {
		return runtimeeffects.CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "settle_completion", nil)
	}
	if dispatch.providerModel.ModelAlias == "" || dispatch.providerModel.ConcreteModel == "" ||
		dispatch.providerModel.Backend != profile.ID || dispatch.providerModel.Provider != profile.Provider ||
		dispatch.providerModel.Transport != profile.Transport || dispatch.providerModel.RuntimeMode != profile.RuntimeMode {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("completion dispatch provider selection is incomplete or does not match profile %q", profile.ID)
	}
	settledAt := time.Now().UTC().Truncate(time.Microsecond)
	// The dispatch state can narrow a provider-call failure to a proven
	// prelaunch failure. A successful transport does not make later response
	// conversion, usage validation, or target persistence successful.
	if dispatch.state != "" && dispatch.state != runtimeeffects.StateSettled {
		state = dispatch.state
	}
	if len(dispatch.evidence) > 0 {
		if evidence == nil {
			evidence = map[string]any{}
		}
		for key, value := range dispatch.evidence {
			evidence[key] = value
		}
	}
	switch dispatch.invocation {
	case completionProviderInvocationNotStarted:
		if state == runtimeeffects.StateSettled || response != nil || providerHead != nil {
			return runtimeeffects.CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_provider_invocation_missing", "llm-completion-authority", "settle_completion", nil)
		}
		if failure == nil {
			envelope := runtimefailures.FromError(fmt.Errorf("completion failed before provider invocation without failure detail"), "llm-completion-authority", "settle_completion")
			failure = &envelope.Failure
		}
		return runtimeeffects.CompletionSettlementResult{}, dispatch.handle.Settle(ctx, runtimeeffects.StateTerminalFailure, failure, evidence)
	case completionProviderInvocationStarted:
		// Provider-visible outcomes may materialize the immutable turn below.
	default:
		return runtimeeffects.CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_provider_invocation_unclassified", "llm-completion-authority", "settle_completion", nil)
	}
	if dispatch.providerModel.ModelAlias == "" || dispatch.providerModel.ConcreteModel == "" ||
		dispatch.providerModel.Backend != profile.ID || dispatch.providerModel.Provider != profile.Provider ||
		dispatch.providerModel.Transport != profile.Transport || dispatch.providerModel.RuntimeMode != profile.RuntimeMode {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("completion dispatch provider selection is incomplete or does not match profile %q", profile.ID)
	}
	turn = enrichTurnRecord(ctx, nil, turn, response)
	turn = CanonicalizeTurnForPersistence(turn)
	if usage.ResolvedModel == "" {
		usage.ResolvedModel = "unknown"
	}
	if state != runtimeeffects.StateSettled && failure == nil {
		envelope := runtimefailures.FromError(fmt.Errorf("completion failed without provider failure detail"), "llm-completion-authority", "settle_completion")
		failure = &envelope.Failure
	}
	if state == runtimeeffects.StateSettled && response != nil {
		if evidence == nil {
			evidence = map[string]any{}
		}
		authority := ToolOutputAuthority{ProviderOperationID: dispatch.handle.Attempt().OperationID, SettledAt: settledAt}
		if err := authority.Validate(); err != nil {
			return runtimeeffects.CompletionSettlementResult{}, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_tool_output_authority_invalid", "llm-completion-authority", "settle_completion", nil, err)
		}
		response.ToolOutputAuthority = &authority
		attempt := dispatch.handle.Attempt()
		if attempt.Authority.Kind == runtimeeffects.AuthorityNormalAgent && attempt.Origin.Kind == runtimeeffects.CompletionOriginDelivery {
			if dispatch.projection == nil || len(bytes.TrimSpace(dispatch.request)) == 0 {
				return runtimeeffects.CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_projection_input_missing", "llm-completion-authority", "settle_completion", map[string]any{"attempt_id": attempt.AttemptID})
			}
			projection := *dispatch.projection
			projection.Messages = append(append([]Message(nil), projection.Messages...), response.Message)
			continuationResponse := *response
			continuationResponse.CapabilitySurface = nil
			continuationResponse.completionHandle = nil
			continuationResponse.completionFrameID = ""
			raw, err := json.Marshal(completionContinuationEnvelope{
				Version: completionContinuationVersion, Adapter: attempt.Adapter,
				Response: continuationResponse, Usage: usage, Projection: projection,
			})
			if err != nil {
				return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("marshal completion continuation evidence: %w", err)
			}
			if _, err := completionContinuationForPayload(raw, attempt.Adapter); err != nil {
				return runtimeeffects.CompletionSettlementResult{}, err
			}
			if err := runtimeeffects.AttachCompletionContinuationEvidence(evidence, dispatch.request, raw); err != nil {
				return runtimeeffects.CompletionSettlementResult{}, err
			}
			dispatch.continuation = raw
		}
	}
	turn.Failure = failure
	settlement := runtimeeffects.CompletionSettlement{
		Settlement:   runtimeeffects.Settlement{State: state, Failure: failure, Evidence: evidence},
		Usage:        usage,
		Spend:        completionSpendForContext(ctx, profile, turn, usage, dispatch.providerModel),
		ProviderHead: providerHead,
		Now:          settledAt,
	}
	if authority := dispatch.handle.Attempt().Authority; authority.Target.Kind == runtimeeffects.UsageTargetAgentTurn {
		settlement.AgentTurn = completionAgentTurn(targetID, turn)
	}
	result, err := dispatch.handle.SettleCompletion(ctx, settlement)
	if result.Committed && err == nil && result.Disposition == runtimeeffects.CompletionSettlementCurrent && state == runtimeeffects.StateSettled && response != nil &&
		dispatch.handle.Attempt().Authority.Kind == runtimeeffects.AuthorityNormalAgent &&
		dispatch.handle.Attempt().Origin.Kind == runtimeeffects.CompletionOriginDelivery {
		snapshot, ok := dispatch.handle.CompletionContinuation()
		if !ok {
			return result, errors.Join(err, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_commit_result_missing", "llm-completion-authority", "settle_completion", map[string]any{"attempt_id": dispatch.handle.Attempt().AttemptID}))
		}
		dispatch.continuation = snapshot.Payload
	}
	return result, err
}

func completionAgentTurn(targetID string, turn AgentTurnRecord) *runtimeeffects.CompletionAgentTurn {
	latency := int(turn.Latency / time.Millisecond)
	if latency < 0 {
		latency = 0
	}
	capabilitySurfaceID := ""
	capabilitySurface := json.RawMessage(nil)
	if turn.CapabilitySurface != nil {
		capabilitySurfaceID = turn.CapabilitySurface.ID
		capabilitySurface = completionMarshal(turn.CapabilitySurface, `{}`)
	}
	return &runtimeeffects.CompletionAgentTurn{
		TurnID:              targetID,
		RunID:               strings.TrimSpace(turn.RunID),
		AgentID:             strings.TrimSpace(turn.AgentID),
		Identity:            turn.Identity.Normalize(),
		SessionID:           strings.TrimSpace(turn.SessionID),
		Memory:              turn.Memory,
		FlowInstance:        strings.TrimSpace(turn.FlowInstance),
		EntityID:            strings.TrimSpace(turn.EntityID),
		TriggerEventID:      strings.TrimSpace(turn.TriggerEventID),
		TriggerEventType:    strings.TrimSpace(turn.TriggerEventType),
		TaskID:              strings.TrimSpace(turn.TaskID),
		CapabilitySurfaceID: capabilitySurfaceID,
		CapabilitySurface:   capabilitySurface,
		ToolCalls:           completionMarshal(turn.ToolCalls, `[]`),
		EmittedEvents:       completionMarshal(turn.EmittedEvents, `[]`),
		RequestPayload:      completionRaw(turn.RequestPayload),
		ResponsePayload:     completionRaw(turn.ResponseRaw),
		TurnBlocks:          completionMarshal(turn.TurnBlocks, `[]`),
		ParseOK:             turn.ParseOK,
		LatencyMS:           latency,
		RetryCount:          turn.RetryCount,
		Failure:             turn.Failure,
	}
}

func completionSpendForContext(ctx context.Context, profile llmselection.Profile, turn AgentTurnRecord, usage runtimeeffects.CompletionUsage, providerModel llmselection.ResolvedModel) runtimeeffects.CompletionSpend {
	meta := usageMetadataForProvider(profile, providerModel)
	actor, _ := runtimeactors.ActorFromContext(ctx)
	flowInstance := strings.TrimSpace(turn.FlowInstance)
	if !turn.Identity.Agent.IsZero() {
		flowInstance = turn.Identity.Agent.FlowInstance()
	}
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(actor.CanonicalFlowPath())
	}
	if flowInstance == "" && turn.Identity.Agent.IsZero() {
		flowInstance = "global"
	}
	input, output := int64(0), int64(0)
	if usage.InputTokens != nil {
		input = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		output = *usage.OutputTokens
	}
	cost := estimateCompletionCostUSD(usage.ResolvedModel, input, output)
	if usage.ProviderReportedCostUSD != nil {
		cost = *usage.ProviderReportedCostUSD
	}
	return runtimeeffects.CompletionSpend{
		EntityID:       strings.TrimSpace(turn.EntityID),
		FlowInstance:   flowInstance,
		AgentID:        strings.TrimSpace(turn.AgentID),
		AgentIdentity:  turn.Identity.Agent,
		Model:          usage.ResolvedModel,
		ModelAlias:     mapString(meta, "model_alias"),
		BackendProfile: coalesce(mapString(meta, "backend_profile"), profile.ID),
		Provider:       coalesce(mapString(meta, "provider"), profile.Provider),
		Transport:      coalesce(mapString(meta, "transport"), profile.Transport),
		ResolvedModel:  coalesce(mapString(meta, "resolved_model"), usage.ResolvedModel),
		CostUSD:        cost,
		InvocationType: profile.ID,
	}
}

func completionUsage(input, output int, model string, exactness runtimeeffects.CompletionUsageExactness) runtimeeffects.CompletionUsage {
	in, out := int64(input), int64(output)
	return runtimeeffects.CompletionUsage{ResolvedModel: strings.TrimSpace(model), Exactness: exactness, InputTokens: &in, OutputTokens: &out}
}

func unavailableCompletionUsage(model string) runtimeeffects.CompletionUsage {
	return runtimeeffects.CompletionUsage{ResolvedModel: coalesce(strings.TrimSpace(model), "unknown"), Exactness: runtimeeffects.CompletionUsageUnavailable}
}

func claudeCompletionUsageFromRaw(raw []byte, fallbackModel string) (runtimeeffects.CompletionUsage, error) {
	type resultMessage struct {
		Type         string   `json:"type"`
		Model        string   `json:"model"`
		TotalCostUSD *float64 `json:"total_cost_usd"`
		Usage        struct {
			InputTokens              *int64 `json:"input_tokens"`
			OutputTokens             *int64 `json:"output_tokens"`
			CacheReadInputTokens     *int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens *int64 `json:"cache_creation_input_tokens"`
			CacheCreation            struct {
				Ephemeral5mInputTokens *int64 `json:"ephemeral_5m_input_tokens"`
				Ephemeral1hInputTokens *int64 `json:"ephemeral_1h_input_tokens"`
			} `json:"cache_creation"`
		} `json:"usage"`
	}
	decode := func(line []byte) (resultMessage, bool) {
		var result resultMessage
		if json.Unmarshal(bytes.TrimSpace(line), &result) != nil || strings.TrimSpace(strings.ToLower(result.Type)) != "result" {
			return resultMessage{}, false
		}
		return result, true
	}
	var terminal resultMessage
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		if result, ok := decode(line); ok {
			terminal, found = result, true
		}
	}
	if !found {
		if result, ok := decode(raw); ok {
			terminal, found = result, true
		}
	}
	if !found || terminal.Usage.InputTokens == nil || terminal.Usage.OutputTokens == nil {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage missing exact usage")
	}
	model := strings.TrimSpace(coalesce(terminal.Model, fallbackModel))
	if model == "" {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage completion model is unavailable")
	}
	usage := runtimeeffects.CompletionUsage{
		ResolvedModel:              model,
		Exactness:                  runtimeeffects.CompletionUsageExact,
		InputTokens:                terminal.Usage.InputTokens,
		OutputTokens:               terminal.Usage.OutputTokens,
		CacheReadInputTokens:       terminal.Usage.CacheReadInputTokens,
		CacheCreationInputTokens:   terminal.Usage.CacheCreationInputTokens,
		CacheCreation5mInputTokens: terminal.Usage.CacheCreation.Ephemeral5mInputTokens,
		CacheCreation1hInputTokens: terminal.Usage.CacheCreation.Ephemeral1hInputTokens,
		ProviderReportedCostUSD:    terminal.TotalCostUSD,
	}
	if err := usage.Validate(); err != nil {
		return runtimeeffects.CompletionUsage{}, fmt.Errorf("claude ResultMessage usage is invalid: %w", err)
	}
	return usage, nil
}

func completionFailure(err error, component, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	envelope := runtimefailures.FromError(err, component, operation)
	return &envelope.Failure
}

func completionRaw(raw []byte) json.RawMessage {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func completionMarshal(value any, fallback string) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil || !json.Valid(raw) {
		return json.RawMessage(fallback)
	}
	return raw
}

func mapString(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func estimateCompletionCostUSD(model string, input, output int64) float64 {
	inRate, outRate := 3.0, 15.0
	lower := strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(lower, "haiku") {
		inRate, outRate = 0.8, 4.0
	} else if strings.Contains(lower, "opus") {
		inRate, outRate = 15.0, 75.0
	}
	if input < 0 {
		input = 0
	}
	if output < 0 {
		output = 0
	}
	return float64(input)/1_000_000*inRate + float64(output)/1_000_000*outRate
}

func completionTurnBase(ctx context.Context, session *Session, request, response []byte, parseOK bool, latency time.Duration, failure *runtimefailures.Envelope) AgentTurnRecord {
	actor, _ := runtimeactors.ActorFromContext(ctx)
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	flowInstance := session.MemoryIdentity.FlowInstance()
	if session.Memory.Enabled {
		runID = session.MemoryIdentity.RunID
	}
	return AgentTurnRecord{
		AgentID:        session.AgentID,
		SessionID:      session.ID,
		Memory:         session.Memory,
		RunID:          runID,
		EntityID:       actor.EffectiveEntityID(),
		FlowInstance:   flowInstance,
		RequestPayload: request,
		ResponseRaw:    response,
		ParseOK:        parseOK,
		Latency:        latency,
		Failure:        failure,
	}
}
