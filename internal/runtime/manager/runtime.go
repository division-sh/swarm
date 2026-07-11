package manager

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type agentDirectiveRunTargetResolver interface {
	ResolveAgentDirectiveRunTarget(ctx context.Context, agentID, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error)
}

type bundleFingerprintContextOwner interface {
	WithBundleFingerprint(context.Context) context.Context
}

const DefaultShutdownGrace = 30 * time.Second

const (
	directiveExecutionLease           = 2 * time.Minute
	directiveExecutionHeartbeat       = 30 * time.Second
	directiveOperationTTL             = 24 * time.Hour
	directiveHeartbeatRenewalTimeout  = 5 * time.Second
	directiveHeartbeatShutdownTimeout = 5 * time.Second
)

type directiveHeartbeatConfig struct {
	interval        time.Duration
	renewalTimeout  time.Duration
	shutdownTimeout time.Duration
}

func defaultDirectiveHeartbeatConfig() directiveHeartbeatConfig {
	return directiveHeartbeatConfig{
		interval:        directiveExecutionHeartbeat,
		renewalTimeout:  directiveHeartbeatRenewalTimeout,
		shutdownTimeout: directiveHeartbeatShutdownTimeout,
	}
}

func (c directiveHeartbeatConfig) normalized() directiveHeartbeatConfig {
	defaults := defaultDirectiveHeartbeatConfig()
	if c.interval <= 0 {
		c.interval = defaults.interval
	}
	if c.renewalTimeout <= 0 {
		c.renewalTimeout = defaults.renewalTimeout
	}
	if c.shutdownTimeout <= 0 {
		c.shutdownTimeout = defaults.shutdownTimeout
	}
	return c
}

var errRuntimeShuttingDown = errors.New("runtime shutting down")

type ShutdownOptions struct {
	Grace time.Duration
}

func DefaultShutdownOptions() ShutdownOptions {
	return ShutdownOptions{Grace: DefaultShutdownGrace}
}

func ResolveShutdownGrace(grace time.Duration) (time.Duration, error) {
	switch {
	case grace < 0:
		return 0, fmt.Errorf("shutdown grace must be positive: %s", grace)
	case grace == 0:
		return DefaultShutdownGrace, nil
	default:
		return grace, nil
	}
}

func (am *AgentManager) RestartAgent(agentID string) error {
	_, err := am.Restart(context.Background(), runtimeagentcontrol.RestartRequest{AgentID: agentID})
	return legacyAgentControlError(err)
}

func (am *AgentManager) Restart(ctx context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	if am.shutdownAdmissionClosed() {
		return runtimeagentcontrol.RestartResult{}, agentControlNotRunning(req.AgentID, runtimeagentcontrol.StatusTerminated)
	}
	agentID := strings.TrimSpace(req.AgentID)
	if agentID == "" {
		return runtimeagentcontrol.RestartResult{}, errors.New("agent id is required")
	}
	am.mu.RLock()
	agent, ok := am.agents[agentID]
	am.mu.RUnlock()
	if !ok {
		return runtimeagentcontrol.RestartResult{}, agentControlNotFound(agentID)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	operationID := strings.TrimSpace(req.OperationID)
	if operationID == "" {
		operationID = uuid.NewString()
	}
	if err := am.startAgentLoopTransition(ctx, agent, agent.Subscriptions(), "restart", operationID, nil); err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	token, _ := am.lifecycle.token(agentID)
	return runtimeagentcontrol.RestartResult{AgentID: agentID, OperationID: operationID, Generation: token.Generation}, nil
}

func (am *AgentManager) Shutdown() error {
	return am.ShutdownWithOptions(DefaultShutdownOptions())
}

func (am *AgentManager) ShutdownWithOptions(opts ShutdownOptions) error {
	return am.shutdownWithOptions(opts, false)
}

func (am *AgentManager) shutdownWithOptions(opts ShutdownOptions, continueReset bool) error {
	grace, err := ResolveShutdownGrace(opts.Grace)
	if err != nil {
		return err
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), grace)
	defer cancelDrain()

	if am.lifecycle.phaseSnapshot() == runtimeLifecycleShuttingDown {
		return am.waitForRunShutdown(grace)
	}
	am.lifecycle.beginShutdownAdmission()

	var shutdownErr error
	if err := am.WaitForQuiescence(drainCtx); err != nil {
		shutdownErr = fmt.Errorf("agent manager shutdown drain timed out after %s", grace)
	}

	_, loopDone := am.lifecycle.cancelShutdownWork()
	for _, done := range loopDone {
		select {
		case <-done:
		case <-drainCtx.Done():
			if shutdownErr == nil {
				shutdownErr = fmt.Errorf("agent manager shutdown loop wait timed out after %s", grace)
			}
		}
	}

	if err := am.waitForRunShutdown(grace); err != nil {
		if shutdownErr == nil {
			shutdownErr = err
		}
	}
	if continueReset {
		am.lifecycle.beginReset()
	} else {
		am.lifecycle.finishShutdown()
	}
	return shutdownErr
}

func (am *AgentManager) Count() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.agents)
}

func (am *AgentManager) IsRunning() bool {
	_, _, running := am.lifecycle.runSnapshot()
	return running
}

func (am *AgentManager) isShuttingDown() bool {
	if am == nil {
		return false
	}
	phase := am.lifecycle.phaseSnapshot()
	return phase == runtimeLifecycleShuttingDown || phase == runtimeLifecycleResetting
}

func (am *AgentManager) shutdownAdmissionClosed() bool {
	if am == nil {
		return false
	}
	return am.shutdownAdmissionClosedLocked()
}

func (am *AgentManager) shutdownAdmissionClosedLocked() bool {
	if am == nil {
		return false
	}
	phase := am.lifecycle.phaseSnapshot()
	if phase == runtimeLifecycleShuttingDown || phase == runtimeLifecycleResetting {
		return true
	}
	if am.runtimeShutdownAdmissionClosed != nil {
		return am.runtimeShutdownAdmissionClosed()
	}
	return false
}

func (am *AgentManager) waitForRunShutdown(grace time.Duration) error {
	done := make(chan struct{})
	go func() {
		am.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(grace):
		return fmt.Errorf("agent manager shutdown timed out after %s", grace)
	}
}

func (am *AgentManager) GetAgentConfig(agentID string) (runtimeactors.AgentConfig, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	cfg, ok := am.agentCfg[agentID]
	return cfg, ok
}

func (am *AgentManager) ListAgentConfigs() []runtimeactors.AgentConfig {
	am.mu.RLock()
	defer am.mu.RUnlock()
	out := make([]runtimeactors.AgentConfig, 0, len(am.agentCfg))
	for _, cfg := range am.agentCfg {
		out = append(out, cfg)
	}
	return out
}

func (am *AgentManager) poisonKey(agentID, eventID string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventID)
}

func (am *AgentManager) incrementPoisonPanicCount(agentID, eventID string) int {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	am.poisonPanicCounts[key]++
	return am.poisonPanicCounts[key]
}

func (am *AgentManager) clearPoisonPanicCount(agentID, eventID string) {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	delete(am.poisonPanicCounts, key)
}

func (am *AgentManager) quarantinePoisonEvent(ctx context.Context, agentID string, evt events.Event, count int, panicFailure runtimefailures.Envelope) {
	affectedCount, shouldEmit := am.recordPoisonQuarantine(strings.TrimSpace(string(evt.Type())), evt.EntityID())
	if !shouldEmit {
		return
	}
	payload := map[string]any{
		"event_name":            strings.TrimSpace(string(evt.Type())),
		"reason_code":           "repeated_agent_panic",
		"panic_count":           count,
		"affected_entity_count": affectedCount,
		"last_failure":          panicFailure,
		"timestamp":             time.Now().UTC().Format(time.RFC3339Nano),
	}
	eventCtx := am.runtimePlatformControlEventContext(ctx)
	if err := am.bus.Publish(eventCtx, events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("platform.event_quarantined"), "runtime", "", mustJSON(payload), 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_event_quarantined_failed",
				EventID:   strings.TrimSpace(evt.ID()),
				EventType: strings.TrimSpace(string(evt.Type())),
				AgentID:   strings.TrimSpace(agentID),
				EntityID:  strings.TrimSpace(evt.EntityID()),
				Failure:   failureEnvelope(err, "agent-manager", "publish_event_quarantined"),
			})
		}
	}
}

func (am *AgentManager) recordPoisonQuarantine(eventName, entityID string) (int, bool) {
	eventName = strings.TrimSpace(eventName)
	entityID = strings.TrimSpace(entityID)
	if eventName == "" {
		return 0, false
	}
	if entityID == "" {
		entityID = "__unknown__"
	}
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	if am.poisonEventEntities[eventName] == nil {
		am.poisonEventEntities[eventName] = map[string]struct{}{}
	}
	am.poisonEventEntities[eventName][entityID] = struct{}{}
	affectedCount := len(am.poisonEventEntities[eventName])
	if affectedCount < poisonEventEntityThreshold {
		return affectedCount, false
	}
	if am.poisonEventEmitted[eventName] {
		return affectedCount, false
	}
	am.poisonEventEmitted[eventName] = true
	return affectedCount, true
}

func deterministicOutputEventID(inbound events.Event, agentID string, index int, out events.Event) string {
	return DeterministicOutputEventID(inbound, agentID, index, out)
}

func (am *AgentManager) defaultManagerAgentID(cfg runtimeactors.AgentConfig) string {
	if managerID := normalizedManagerFallback(cfg, cfg.ManagerFallback); managerID != "" {
		return managerID
	}
	if source := am.semanticSource; source != nil {
		if _, entry, ok := semanticview.ResolveAgentRegistryEntry(source, cfg); ok {
			if managerID := normalizedManagerFallback(cfg, strings.TrimSpace(entry.ManagerFallback)); managerID != "" {
				return managerID
			}
		}
	}
	return ""
}

func normalizedManagerFallback(cfg runtimeactors.AgentConfig, managerID string) string {
	managerID = strings.TrimSpace(managerID)
	if managerID == "" {
		return ""
	}
	return managerID
}

func flowPathFromAgentConfig(cfg runtimeactors.AgentConfig) string {
	return cfg.CanonicalFlowPath()
}

type eventExistenceReader interface {
	EventExists(ctx context.Context, eventID string) (bool, error)
}

func (am *AgentManager) shouldSkipAlreadyPublishedOutput(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || am.store == nil {
		return false
	}
	reader, ok := am.store.(eventExistenceReader)
	if !ok {
		return false
	}
	exists, err := reader.EventExists(ctx, eventID)
	if err != nil {
		return false
	}
	return exists
}

func (am *AgentManager) safeProcessEvent(ctx context.Context, agent Agent, evt events.Event) (err error, panicked bool, panicText string, stackTrace string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicText = fmt.Sprint(r)
			stackTrace = strings.TrimSpace(string(debug.Stack()))
		}
	}()
	err = am.processEvent(ctx, agent, evt)
	return
}

func (am *AgentManager) ChatWithAgent(ctx context.Context, agentID, directive string) (string, error) {
	result, err := am.SendDirective(ctx, runtimeagentcontrol.SendDirectiveRequest{
		AgentID:   agentID,
		Directive: directive,
	})
	if err != nil {
		return "", legacyAgentControlError(err)
	}
	return result.Response, nil
}

func (am *AgentManager) SendDirective(ctx context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if am.shutdownAdmissionClosed() {
		return runtimeagentcontrol.SendDirectiveResult{}, agentControlNotRunning(req.AgentID, runtimeagentcontrol.StatusTerminated)
	}
	agentID := strings.TrimSpace(req.AgentID)
	req.AgentID = agentID
	req.Directive = strings.TrimSpace(req.Directive)
	req.RunID = strings.TrimSpace(req.RunID)
	req.Source = strings.TrimSpace(req.Source)
	if req.Source == "" {
		req.Source = runtimeagentcontrol.DirectiveSourceBuilderRuntime
	}
	req.OperatorID = strings.TrimSpace(req.OperatorID)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	if req.ActorTokenID == "" {
		req.ActorTokenID = req.OperatorID
	}
	if req.ActorTokenID == "" {
		req.ActorTokenID = "internal:" + req.Source
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	if req.RequestHash == "" {
		var err error
		req.RequestHash, err = directiveRequestHash(req)
		if err != nil {
			return runtimeagentcontrol.SendDirectiveResult{}, err
		}
	}
	if agentID == "" {
		return runtimeagentcontrol.SendDirectiveResult{}, errors.New("agent id is required")
	}
	if req.Directive == "" {
		return runtimeagentcontrol.SendDirectiveResult{}, errors.New("directive is required")
	}
	operationStore, err := am.directiveOperationStore()
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	if req.IdempotencyKey != "" {
		existing, ok, err := operationStore.LoadDirectiveOperationByKey(ctx, runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
		if err != nil {
			return runtimeagentcontrol.SendDirectiveResult{}, err
		}
		if ok {
			now := time.Now().UTC()
			if (existing.State == runtimeagentcontrol.DirectiveOperationSucceeded || existing.State == runtimeagentcontrol.DirectiveOperationFailed) && !existing.ExpiresAt.IsZero() && !existing.ExpiresAt.After(now) {
				existing, ok, err = operationStore.ReconcileDirectiveOperation(ctx, existing.OperationID, now, directiveOperationTTL)
				if err != nil {
					return runtimeagentcontrol.SendDirectiveResult{}, err
				}
			}
			if ok {
				if existing.RequestHash != req.RequestHash {
					return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveIdempotencyConflictError{OriginalRequestHash: existing.RequestHash, ConflictingRequestHash: req.RequestHash, OperationID: existing.OperationID}
				}
				if existing.State == runtimeagentcontrol.DirectiveOperationExecuting && !existing.ExecutionLeaseExpiresAt.IsZero() && !existing.ExecutionLeaseExpiresAt.After(now) {
					existing, ok, err = operationStore.ReconcileDirectiveOperation(ctx, existing.OperationID, now, directiveOperationTTL)
					if err != nil {
						return runtimeagentcontrol.SendDirectiveResult{}, err
					}
					if !ok {
						return runtimeagentcontrol.SendDirectiveResult{}, errors.New("directive operation disappeared during reconciliation")
					}
				}
				return am.continueDirectiveOperation(ctx, operationStore, existing)
			}
		}
	}
	if _, err := am.directiveBoardAgent(agentID); err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	target, err := am.resolveAgentDirectiveRunTarget(ctx, agentID, req.RunID)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	now := time.Now().UTC()
	operationID := uuid.NewString()
	eventID := uuid.NewString()
	directiveEvent, err := runtimeagentcontrol.NewDirectiveEvent(req, target, operationID, eventID, now)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	reservationCtx := runtimecorrelation.WithRunID(am.runtimePlatformControlEventContext(ctx), target.RunID)
	if owner, ok := am.bus.(bundleFingerprintContextOwner); ok && owner != nil {
		reservationCtx = owner.WithBundleFingerprint(reservationCtx)
	}
	reservation, err := operationStore.ReserveDirectiveOperation(reservationCtx, runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID:      operationID,
			Method:           runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID:     req.ActorTokenID,
			IdempotencyKey:   req.IdempotencyKey,
			RequestHash:      req.RequestHash,
			AgentID:          agentID,
			Directive:        req.Directive,
			RequestedRunID:   req.RunID,
			ResolvedRunID:    target.RunID,
			RunIDResolution:  target.Mode,
			Source:           req.Source,
			OperatorID:       req.OperatorID,
			DirectiveEventID: eventID,
			State:            runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: directiveEvent,
		Now:   now,
	})
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	return am.continueDirectiveOperation(ctx, operationStore, reservation.Operation)
}

func (am *AgentManager) directiveOperationStore() (runtimeagentcontrol.DirectiveOperationStore, error) {
	if am == nil || am.bus == nil || am.bus.Store() == nil {
		return nil, errors.New("directive operation store is required")
	}
	store, ok := am.bus.Store().(runtimeagentcontrol.DirectiveOperationStore)
	if !ok || store == nil {
		return nil, errors.New("selected store does not support directive operations")
	}
	return store, nil
}

func (am *AgentManager) directiveBoardAgent(agentID string) (BoardInteractiveAgent, error) {
	am.mu.RLock()
	agent, ok := am.agents[strings.TrimSpace(agentID)]
	am.mu.RUnlock()
	if !ok {
		return nil, agentControlNotFound(agentID)
	}
	chatAgent, ok := agent.(BoardInteractiveAgent)
	if !ok {
		return nil, agentControlNotRunning(agentID, runtimeagentcontrol.StatusIdle)
	}
	return chatAgent, nil
}

func (am *AgentManager) continueDirectiveOperation(ctx context.Context, store runtimeagentcontrol.DirectiveOperationStore, op runtimeagentcontrol.DirectiveOperation) (runtimeagentcontrol.SendDirectiveResult, error) {
	op = op.Normalized()
	switch op.State {
	case runtimeagentcontrol.DirectiveOperationSucceeded:
		return directiveResultFromOperation(op)
	case runtimeagentcontrol.DirectiveOperationExecuted:
		finalized, err := store.FinalizeDirectiveSuccess(ctx, op.OperationID, time.Now().UTC(), directiveOperationTTL)
		if err != nil {
			return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{Err: runtimeagentcontrol.ErrDirectiveCompletionPending, Operation: op}
		}
		return directiveResultFromOperation(finalized)
	case runtimeagentcontrol.DirectiveOperationExecuting, runtimeagentcontrol.DirectiveOperationFailed, runtimeagentcontrol.DirectiveOperationIndeterminate:
		return runtimeagentcontrol.SendDirectiveResult{}, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	case runtimeagentcontrol.DirectiveOperationPrepared:
		return am.executePreparedDirectiveOperation(ctx, store, op)
	default:
		return runtimeagentcontrol.SendDirectiveResult{}, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
}

func (am *AgentManager) executePreparedDirectiveOperation(ctx context.Context, store runtimeagentcontrol.DirectiveOperationStore, op runtimeagentcontrol.DirectiveOperation) (runtimeagentcontrol.SendDirectiveResult, error) {
	chatAgent, err := am.directiveBoardAgent(op.AgentID)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	ownerID := uuid.NewString()
	admitted, err := store.AdmitDirectiveExecution(ctx, op.OperationID, ownerID, time.Now().UTC(), directiveExecutionLease)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	directiveEvent, err := directiveEventFromOperation(admitted)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	directiveCtx := runtimecorrelation.WithRunID(ctx, strings.TrimSpace(directiveEvent.RunID()))
	directiveCtx = runtimebus.WithInboundEvent(directiveCtx, directiveEvent)
	directiveCtx, err = am.lifecycle.effectContext(directiveCtx, admitted.AgentID)
	if err != nil {
		executionFailure := runtimeagentcontrol.DirectiveBoardStepFailure(err)
		failed, persistErr := store.FinalizeDirectiveFailure(ctx, admitted.OperationID, ownerID, executionFailure, time.Now().UTC(), directiveOperationTTL)
		if persistErr != nil {
			admitted.State = runtimeagentcontrol.DirectiveOperationIndeterminate
			failure := runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedFailure()
			admitted.Failure = &failure
			return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{Err: runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, Operation: admitted}
		}
		return runtimeagentcontrol.SendDirectiveResult{}, runtimeagentcontrol.ErrorForDirectiveOperation(failed)
	}
	heartbeatConfig := am.directiveHeartbeat.normalized()
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	go runDirectiveExecutionHeartbeat(heartbeatCtx, heartbeatDone, store, admitted.OperationID, ownerID, heartbeatConfig)
	response, executionErr := chatAgent.BoardStep(directiveCtx, runtimeagentcontrol.BoardDirective{
		Directive:       admitted.Directive,
		Event:           directiveEvent,
		RunIDResolution: admitted.RunIDResolution,
		OperatorID:      admitted.OperatorID,
		Source:          admitted.Source,
	})
	stopHeartbeat()
	heartbeatShutdown := time.NewTimer(heartbeatConfig.shutdownTimeout)
	select {
	case <-heartbeatDone:
		if !heartbeatShutdown.Stop() {
			select {
			case <-heartbeatShutdown.C:
			default:
			}
		}
	case <-heartbeatShutdown.C:
		admitted.State = runtimeagentcontrol.DirectiveOperationIndeterminate
		failure := runtimeagentcontrol.DirectiveHeartbeatShutdownUnconfirmedFailure()
		admitted.Failure = &failure
		return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{
			Err:       runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate,
			Operation: admitted,
		}
	}
	if executionErr != nil {
		executionFailure := runtimeagentcontrol.DirectiveBoardStepFailure(executionErr)
		failed, persistErr := store.FinalizeDirectiveFailure(ctx, admitted.OperationID, ownerID, executionFailure, time.Now().UTC(), directiveOperationTTL)
		if persistErr != nil {
			admitted.State = runtimeagentcontrol.DirectiveOperationIndeterminate
			failure := runtimeagentcontrol.DirectiveFailurePersistenceUnconfirmedFailure()
			admitted.Failure = &failure
			return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{Err: runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, Operation: admitted}
		}
		return runtimeagentcontrol.SendDirectiveResult{}, runtimeagentcontrol.ErrorForDirectiveOperation(failed)
	}
	result := runtimeagentcontrol.SendDirectiveResult{
		OK:                 true,
		AgentID:            admitted.AgentID,
		OperationID:        admitted.OperationID,
		Response:           response,
		RunID:              admitted.ResolvedRunID,
		RunIDResolution:    admitted.RunIDResolution,
		DirectiveEventID:   admitted.DirectiveEventID,
		DirectiveEventType: string(directiveEvent.Type()),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	executed, err := store.RecordDirectiveExecuted(ctx, admitted.OperationID, ownerID, encoded, time.Now().UTC())
	if err != nil {
		admitted.State = runtimeagentcontrol.DirectiveOperationIndeterminate
		failure := runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedFailure()
		admitted.Failure = &failure
		return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{Err: runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate, Operation: admitted}
	}
	finalized, err := store.FinalizeDirectiveSuccess(ctx, admitted.OperationID, time.Now().UTC(), directiveOperationTTL)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, &runtimeagentcontrol.DirectiveOperationError{Err: runtimeagentcontrol.ErrDirectiveCompletionPending, Operation: executed}
	}
	return directiveResultFromOperation(finalized)
}

func runDirectiveExecutionHeartbeat(ctx context.Context, done chan<- struct{}, store runtimeagentcontrol.DirectiveOperationStore, operationID, ownerID string, config directiveHeartbeatConfig) {
	defer close(done)
	config = config.normalized()
	ticker := time.NewTicker(config.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, config.renewalTimeout)
			_ = store.RenewDirectiveExecutionLease(renewCtx, operationID, ownerID, now.UTC(), directiveExecutionLease)
			cancel()
		}
	}
}

func directiveEventFromOperation(op runtimeagentcontrol.DirectiveOperation) (events.Event, error) {
	return runtimeagentcontrol.NewDirectiveEvent(runtimeagentcontrol.SendDirectiveRequest{
		AgentID:    op.AgentID,
		Directive:  op.Directive,
		RunID:      op.RequestedRunID,
		Source:     op.Source,
		OperatorID: op.OperatorID,
	}, runtimeagentcontrol.RunTargetResolution{RunID: op.ResolvedRunID, Mode: op.RunIDResolution}, op.OperationID, op.DirectiveEventID, op.CreatedAt)
}

func directiveResultFromOperation(op runtimeagentcontrol.DirectiveOperation) (runtimeagentcontrol.SendDirectiveResult, error) {
	var result runtimeagentcontrol.SendDirectiveResult
	if len(op.Response) == 0 {
		return result, fmt.Errorf("directive operation %s has no durable response", op.OperationID)
	}
	if err := json.Unmarshal(op.Response, &result); err != nil {
		return result, fmt.Errorf("decode directive operation response: %w", err)
	}
	if !result.OK || strings.TrimSpace(result.OperationID) != op.OperationID {
		return runtimeagentcontrol.SendDirectiveResult{}, fmt.Errorf("directive operation response identity mismatch")
	}
	result.AgentID = op.AgentID
	return result, nil
}

func directiveRequestHash(req runtimeagentcontrol.SendDirectiveRequest) (string, error) {
	raw, err := json.Marshal(struct {
		AgentID   string `json:"agent_id"`
		Directive string `json:"directive"`
		RunID     string `json:"run_id,omitempty"`
	}{AgentID: req.AgentID, Directive: req.Directive, RunID: req.RunID})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func (am *AgentManager) resolveAgentDirectiveRunTarget(ctx context.Context, agentID, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	agentID = strings.TrimSpace(agentID)
	explicitRunID = strings.TrimSpace(explicitRunID)
	if resolver, ok := am.store.(agentDirectiveRunTargetResolver); ok && resolver != nil {
		target, err := resolver.ResolveAgentDirectiveRunTarget(ctx, agentID, explicitRunID)
		if err != nil {
			return runtimeagentcontrol.RunTargetResolution{}, err
		}
		return target.Normalized(), nil
	}
	if explicitRunID != "" {
		return runtimeagentcontrol.RunTargetResolution{}, &runtimeagentcontrol.StateError{
			Err:     runtimeagentcontrol.ErrRunNotFound,
			AgentID: agentID,
			RunID:   explicitRunID,
		}
	}
	return runtimeagentcontrol.RunTargetResolution{
		RunID: uuid.NewString(),
		Mode:  runtimeagentcontrol.RunResolutionNewRunAllocated,
	}, nil
}

func (am *AgentManager) Run(ctx context.Context) {
	if am.shutdownAdmissionClosedLocked() {
		return
	}
	runCtx, started := am.lifecycle.beginRun(ctx, AgentRunModeStandard)
	if !started {
		return
	}
	am.runMu.Lock()
	am.authBreakerTripped = false
	am.runMu.Unlock()

	am.mu.RLock()
	agents := make([]Agent, 0, len(am.agents))
	for _, a := range am.agents {
		agents = append(agents, a)
	}
	am.mu.RUnlock()

	for _, a := range agents {
		am.startAgentLoop(runCtx, a)
	}

	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		am.retryLoop(runCtx)
	}()

	go func() {
		<-runCtx.Done()
		initiated := am.lifecycle.beginShutdownAdmission()
		if initiated {
			am.lifecycle.cancelShutdownWork()
			am.lifecycle.finishShutdown()
		}
	}()
}

// RunAuthoritativeDeliveryOnly starts agent loops with authoritative recipient
// channels only. It intentionally avoids live subscription patterns and
// retry/recovery loops so selected-fork execution can consume canonical
// recipient planning without reintroducing subscription-derived recipient truth.
func (am *AgentManager) RunAuthoritativeDeliveryOnly(ctx context.Context) {
	if am.shutdownAdmissionClosedLocked() {
		return
	}
	runCtx, started := am.lifecycle.beginRun(ctx, AgentRunModeAuthoritativeDeliveryOnly)
	if !started {
		return
	}
	am.runMu.Lock()
	am.authBreakerTripped = false
	am.runMu.Unlock()

	am.mu.RLock()
	agents := make([]Agent, 0, len(am.agents))
	for _, a := range am.agents {
		agents = append(agents, a)
	}
	am.mu.RUnlock()

	for _, a := range agents {
		am.startAgentLoopWithSubscriptions(runCtx, a, nil)
	}

	go func() {
		<-runCtx.Done()
		initiated := am.lifecycle.beginShutdownAdmission()
		if initiated {
			am.lifecycle.cancelShutdownWork()
			am.lifecycle.finishShutdown()
		}
	}()
}

func (am *AgentManager) Recover(ctx context.Context) error {
	_, err := am.recover(ctx, false)
	return err
}

func (am *AgentManager) RecoverWithStartupReplayDiagnostics(ctx context.Context) (StartupReplaySummary, error) {
	return am.recover(ctx, true)
}

func (am *AgentManager) ReconcileDirectiveOperations(ctx context.Context) error {
	if am == nil || am.bus == nil || am.bus.Store() == nil {
		return nil
	}
	operationStore, ok := am.bus.Store().(runtimeagentcontrol.DirectiveOperationStore)
	if !ok || operationStore == nil {
		return nil
	}
	if _, err := operationStore.ReconcileDirectiveOperations(ctx, time.Now().UTC(), directiveOperationTTL); err != nil {
		return fmt.Errorf("reconcile directive operations: %w", err)
	}
	return nil
}

func (am *AgentManager) projectLifecycleDiagnostics(ctx context.Context) error {
	if am == nil || am.bus == nil || am.lifecycle == nil {
		return nil
	}
	store, ok := am.lifecycle.store.(AgentLifecycleDiagnosticPersistence)
	if !ok || store == nil {
		return nil
	}
	for {
		items, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
		if err != nil {
			return err
		}
		for _, item := range items {
			detail := make(map[string]any, len(item.Payload)+3)
			for key, value := range item.Payload {
				detail[key] = value
			}
			detail["outbox_id"] = item.OutboxID
			detail["operation_id"] = item.OperationID
			detail["event_name"] = item.EventName
			if err := am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level: "info", Component: "agent-lifecycle", Action: item.EventName,
				AgentID: item.AgentID, Detail: detail,
			}); err != nil {
				return err
			}
			if err := store.MarkAgentLifecycleDiagnosticProjected(ctx, item.OutboxID, time.Now().UTC()); err != nil {
				return err
			}
		}
		if len(items) < 100 {
			return nil
		}
	}
}

func (am *AgentManager) recover(ctx context.Context, startupReplayDiagnostics bool) (StartupReplaySummary, error) {
	summary := StartupReplaySummary{}
	if am.store == nil {
		return summary, nil
	}
	if recoveryStore, ok := am.lifecycle.store.(runtimeeffects.RecoveryStore); ok && recoveryStore != nil {
		if _, err := recoveryStore.ReconcileExternalEffectAttempts(ctx, time.Now().UTC()); err != nil {
			return summary, fmt.Errorf("reconcile external effect attempts: %w", err)
		}
	}
	if err := am.projectLifecycleDiagnostics(ctx); err != nil {
		return summary, fmt.Errorf("project lifecycle diagnostics: %w", err)
	}

	agents, err := am.store.LoadAgents(ctx)
	if err != nil {
		return summary, fmt.Errorf("load agents: %w", err)
	}
	sort.SliceStable(agents, func(i, j int) bool {
		return agents[i].StartedAt.Before(agents[j].StartedAt)
	})
	for _, rec := range agents {
		if rec.Config.ID == "" {
			continue
		}
		if err := am.spawnAgentInternal(ctx, rec, false); err != nil && !errors.Is(err, ErrAgentAlreadyExists) {
			return summary, fmt.Errorf("hydrate agent %s: %w", rec.Config.ID, err)
		}
	}
	if err := am.restoreFlowInstanceRoutes(ctx); err != nil {
		return summary, err
	}
	if err := am.restoreSelectedContractRouteRecoveries(ctx); err != nil {
		return summary, err
	}
	if err := runtimepipeline.NewRecoveryManagerWith(am.bus.Store(), am.bus).Recover(ctx); err != nil {
		return summary, fmt.Errorf("recover pipeline receipts: %w", err)
	}

	if startupReplayDiagnostics {
		ctx = withStartupManagerReplayDiagnostics(ctx)
	}
	replaySummary, err := am.replayPendingEventsDetailed(ctx)
	summary.merge(replaySummary)
	return summary, err
}

type RecoverableStateSnapshot struct {
	PersistedAgentCount                         int
	PersistedFlowInstanceRouteCount             int
	PersistedSelectedContractRouteRecoveryCount int
	ReplayEligibleEventPresent                  bool
}

func (s RecoverableStateSnapshot) HasRecoverableWork() bool {
	return s.PersistedAgentCount > 0 ||
		s.PersistedFlowInstanceRouteCount > 0 ||
		s.PersistedSelectedContractRouteRecoveryCount > 0 ||
		s.ReplayEligibleEventPresent
}

func (s RecoverableStateSnapshot) Classes() []string {
	classes := make([]string, 0, 3)
	if s.PersistedAgentCount > 0 {
		classes = append(classes, "persisted agents")
	}
	if s.PersistedFlowInstanceRouteCount > 0 {
		classes = append(classes, "persisted flow instance routes")
	}
	if s.PersistedSelectedContractRouteRecoveryCount > 0 {
		classes = append(classes, "selected-contract route recoveries")
	}
	if s.ReplayEligibleEventPresent {
		classes = append(classes, "events missing pipeline receipts")
	}
	sort.Strings(classes)
	return classes
}

func (s RecoverableStateSnapshot) Detail() map[string]any {
	return map[string]any{
		"persisted_agent_count":                            s.PersistedAgentCount,
		"persisted_flow_instance_route_count":              s.PersistedFlowInstanceRouteCount,
		"persisted_selected_contract_route_recovery_count": s.PersistedSelectedContractRouteRecoveryCount,
		"replay_eligible_event_present":                    s.ReplayEligibleEventPresent,
	}
}

func (am *AgentManager) RecoverableStateSnapshot(ctx context.Context) (RecoverableStateSnapshot, error) {
	snapshot := RecoverableStateSnapshot{}
	if am == nil {
		return snapshot, nil
	}
	if am.store != nil {
		agents, err := am.store.LoadAgents(ctx)
		if err != nil {
			return RecoverableStateSnapshot{}, fmt.Errorf("load persisted agents: %w", err)
		}
		snapshot.PersistedAgentCount = len(agents)
	}
	if am.bus == nil {
		return snapshot, nil
	}
	store := am.bus.Store()
	if store == nil {
		return snapshot, nil
	}
	if routeStore, ok := store.(runtimebus.FlowInstanceRoutePersistence); ok && routeStore != nil {
		routes, err := routeStore.ListFlowInstanceRoutes(ctx)
		if err != nil {
			return RecoverableStateSnapshot{}, fmt.Errorf("list persisted flow instance routes: %w", err)
		}
		snapshot.PersistedFlowInstanceRouteCount = len(routes)
	}
	if routeRecoveryStore, ok := store.(selectedContractRouteRecoveryLister); ok && routeRecoveryStore != nil {
		recoveries, err := routeRecoveryStore.ListSelectedContractRouteRecoveryRecords(ctx)
		if err != nil {
			return RecoverableStateSnapshot{}, fmt.Errorf("list selected-contract route recoveries: %w", err)
		}
		snapshot.PersistedSelectedContractRouteRecoveryCount = len(recoveries)
	}
	replayStore, ok := store.(runtimereplayclaim.Lister)
	if ok && replayStore != nil {
		eventsToReplay, err := replayStore.ListEventsMissingPipelineReceipt(ctx, time.Now().Add(-30*24*time.Hour), 1)
		if err != nil {
			return RecoverableStateSnapshot{}, fmt.Errorf("list events missing pipeline receipts: %w", err)
		}
		snapshot.ReplayEligibleEventPresent = len(eventsToReplay) > 0
	}
	return snapshot, nil
}

func (am *AgentManager) restoreFlowInstanceRoutes(ctx context.Context) error {
	if am == nil || am.bus == nil {
		return nil
	}
	installer, ok := am.bus.(flowInstanceRouteInstaller)
	if !ok || installer == nil {
		return nil
	}
	routeStore, ok := am.bus.Store().(runtimebus.FlowInstanceRoutePersistence)
	if !ok || routeStore == nil {
		return nil
	}
	routes, err := routeStore.ListFlowInstanceRoutes(ctx)
	if err != nil {
		return fmt.Errorf("list persisted flow instance routes: %w", err)
	}
	for _, route := range routes {
		req, err := am.restoredFlowInstanceRouteMaterializationRequest(ctx, route)
		if err != nil {
			return err
		}
		if err := installer.AddFlowInstanceRoute(req); err != nil {
			return fmt.Errorf("restore flow instance route %s/%s: %w", route.ScopeKey, route.InstanceID, err)
		}
	}
	return nil
}

func (am *AgentManager) restoredFlowInstanceRouteMaterializationRequest(ctx context.Context, route runtimeflowidentity.Route) (runtimebus.FlowInstanceRouteMaterializationRequest, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return runtimebus.FlowInstanceRouteMaterializationRequest{}, fmt.Errorf("flow-instance route identity is required")
	}
	if am.workflowInstances == nil {
		return runtimebus.FlowInstanceRouteMaterializationRequest{}, fmt.Errorf("workflow instance store is required to restore flow instance route %s", route.InstancePath)
	}
	instance, ok, err := am.workflowInstances.Load(ctx, route.InstancePath)
	if err != nil {
		return runtimebus.FlowInstanceRouteMaterializationRequest{}, fmt.Errorf("load flow instance for route recovery %s: %w", route.InstancePath, err)
	}
	if !ok {
		return runtimebus.FlowInstanceRouteMaterializationRequest{}, fmt.Errorf("flow instance not found for route recovery: %s", route.InstancePath)
	}
	activationInstance := runtimepipeline.StoredFlowInstance(am.semanticSource, instance)
	vars := flowActivationVars(runtimepipeline.FlowInstanceActivationRequest{
		Instance: activationInstance,
		Config:   instance.Config,
	})
	return runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity:            route,
		ActivationVariables: vars,
	}, nil
}

func (am *AgentManager) retryLoop(ctx context.Context) {
	if am.store == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := am.replayPendingEvents(ctx); err != nil {
				if am.bus != nil {
					am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
						Level:     "error",
						Component: "agent-manager",
						Action:    "retry_replay_failed",
						Failure:   failureEnvelope(err, "agent-manager", "retry_replay"),
					})
				}
			}
		}
	}
}

func (am *AgentManager) replayPendingEvents(ctx context.Context) error {
	_, err := am.replayPendingEventsDetailed(ctx)
	return err
}

func (am *AgentManager) replayPendingEventsDetailed(ctx context.Context) (StartupReplaySummary, error) {
	summary := StartupReplaySummary{}
	if am.store == nil {
		return summary, nil
	}
	if am.isAuthBreakerTripped() {
		return summary, nil
	}

	am.mu.RLock()
	ids := make([]string, 0, len(am.agents))
	for id := range am.agents {
		ids = append(ids, id)
	}
	am.mu.RUnlock()

	for _, id := range ids {
		if am.shutdownAdmissionClosed() {
			return summary, nil
		}
		if am.isAuthBreakerTripped() {
			return summary, nil
		}
		backlogSummary, err := am.replayAgentBacklogDetailed(ctx, id)
		summary.merge(backlogSummary)
		if err != nil {
			if errors.Is(err, errRuntimeShuttingDown) {
				return summary, nil
			}
			if !startupManagerReplayDiagnosticsEnabled(ctx) && am.bus != nil {
				am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "pending_replay_failed",
					AgentID:   id,
					Failure:   failureEnvelope(err, "agent-manager", "replay_pending"),
				})
			}
		}
	}
	return summary, nil
}

func (am *AgentManager) ReplayAgentBacklog(ctx context.Context, agentID string) error {
	_, err := am.ReplayBacklog(ctx, runtimeagentcontrol.ReplayBacklogRequest{AgentID: agentID})
	return legacyAgentControlError(err)
}

func (am *AgentManager) ReplayBacklog(ctx context.Context, req runtimeagentcontrol.ReplayBacklogRequest) (runtimeagentcontrol.ReplayBacklogResult, error) {
	summary, err := am.replayAgentBacklogDetailed(ctx, req.AgentID)
	if err != nil {
		return runtimeagentcontrol.ReplayBacklogResult{}, err
	}
	return runtimeagentcontrol.ReplayBacklogResult{
		AgentID:       strings.TrimSpace(req.AgentID),
		ReplayedCount: summary.ReplayedCount,
	}, nil
}

func (am *AgentManager) replayAgentBacklogDetailed(ctx context.Context, agentID string) (StartupReplaySummary, error) {
	summary := StartupReplaySummary{}
	if am.shutdownAdmissionClosed() {
		return summary, agentControlNotRunning(agentID, runtimeagentcontrol.StatusTerminated)
	}
	if am.store == nil {
		return summary, fmt.Errorf("manager store unavailable")
	}
	if am.isAuthBreakerTripped() {
		return summary, nil
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return summary, errors.New("agent id is required")
	}
	am.mu.RLock()
	agent := am.agents[agentID]
	cfg, ok := am.agentCfg[agentID]
	since := time.Now().Add(-30 * 24 * time.Hour)
	if upAt, ok := am.agentUpAt[agentID]; ok && !upAt.IsZero() {
		since = upAt
	}
	am.mu.RUnlock()
	if !ok || agent == nil {
		return summary, agentControlNotFound(agentID)
	}
	pending, err := am.pendingEventsForAgent(ctx, agentID, cfg, agent, since)
	if err != nil {
		if startupManagerReplayDiagnosticsEnabled(ctx) {
			failure := failureEnvelope(err, "agent-manager", "load_pending_backlog")
			record := startupManagerReplayRecord{
				AgentID:    agentID,
				Outcome:    startupManagerReplayOutcomeDropped,
				ReasonCode: startupManagerReplayReasonBacklogLoadFailed,
				Failure:    failure,
			}
			summary.observe(record)
			logStartupManagerReplayAftermath(ctx, am.bus, record)
			return summary, nil
		}
		return summary, err
	}
	for _, evt := range pending {
		if am.isAuthBreakerTripped() {
			return summary, nil
		}
		result := am.processEventDetailed(ctx, agent, evt)
		summary.observe(result.record)
		if startupManagerReplayDiagnosticsEnabled(ctx) {
			logStartupManagerReplayAftermath(ctx, am.bus, result.record)
		}
		if result.err != nil {
			if !startupManagerReplayDiagnosticsEnabled(ctx) && am.bus != nil {
				evtCtx := runtimecorrelation.WithInboundEvent(ctx, evt)
				evtCtx = runtimecorrelation.WithRunID(evtCtx, strings.TrimSpace(evt.RunID()))
				am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "pending_replay_event_failed",
					EventID:   strings.TrimSpace(evt.ID()),
					EventType: strings.TrimSpace(string(evt.Type())),
					AgentID:   agentID,
					EntityID:  strings.TrimSpace(evt.EntityID()),
					Failure:   failureEnvelope(result.err, "agent-manager", "replay_pending_event"),
				})
			}
			if failure, ok := runtimefailures.As(result.err); ok && failure.Failure.Class == runtimefailures.ClassAuthenticationNeeded {
				return summary, nil
			}
		}
	}
	return summary, nil
}

func agentControlNotFound(agentID string) error {
	return &runtimeagentcontrol.StateError{
		Err:     runtimeagentcontrol.ErrAgentNotFound,
		AgentID: strings.TrimSpace(agentID),
	}
}

func agentControlNotRunning(agentID, currentStatus string) error {
	status := strings.TrimSpace(currentStatus)
	if status == "" {
		status = runtimeagentcontrol.StatusTerminated
	}
	return &runtimeagentcontrol.StateError{
		Err:           runtimeagentcontrol.ErrAgentNotRunning,
		AgentID:       strings.TrimSpace(agentID),
		CurrentStatus: status,
	}
}

func legacyAgentControlError(err error) error {
	if err == nil {
		return nil
	}
	var stateErr *runtimeagentcontrol.StateError
	if errors.As(err, &stateErr) && stateErr != nil {
		switch {
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrAgentNotFound):
			return fmt.Errorf("agent not found: %s", strings.TrimSpace(stateErr.AgentID))
		case errors.Is(stateErr.Err, runtimeagentcontrol.ErrAgentNotRunning) && strings.TrimSpace(stateErr.CurrentStatus) == runtimeagentcontrol.StatusTerminated:
			return errRuntimeShuttingDown
		}
	}
	return err
}

func (am *AgentManager) pendingEventsForAgent(
	ctx context.Context,
	agentID string,
	cfg runtimeactors.AgentConfig,
	agent Agent,
	since time.Time,
) ([]events.Event, error) {
	pending := make([]events.Event, 0, 400)
	pendingByID := make(map[string]events.Event)

	direct, err := am.store.ListPendingEventsForAgent(ctx, agentID, since, 300)
	if err != nil {
		return nil, fmt.Errorf("load pending delivered events for %s: %w", agentID, err)
	}
	for _, evt := range direct {
		pendingByID[evt.ID()] = evt
	}

	subscribed, err := am.store.ListPendingSubscribedEvents(ctx, agentID, agent.Subscriptions(), since, 300)
	if err != nil {
		return nil, fmt.Errorf("load pending subscribed events for %s: %w", agentID, err)
	}
	for _, evt := range subscribed {
		pendingByID[evt.ID()] = evt
	}

	for _, evt := range pendingByID {
		pending = append(pending, evt)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].CreatedAt().Equal(pending[j].CreatedAt()) {
			return pending[i].ID() < pending[j].ID()
		}
		return pending[i].CreatedAt().Before(pending[j].CreatedAt())
	})
	return pending, nil
}

func (am *AgentManager) ResetRuntimeState() error {
	return am.resetRuntimeState("")
}

func (am *AgentManager) ResetRuntimeStateWithSource(source string) error {
	return am.resetRuntimeState(source)
}

func platformResetSourceAuthorized(source string) bool {
	switch strings.TrimSpace(source) {
	case "admin_cli":
		return true
	default:
		return false
	}
}

func (am *AgentManager) resetRuntimeState(source string) error {
	if err := am.shutdownWithOptions(DefaultShutdownOptions(), true); err != nil {
		am.lifecycle.abortReset()
		return err
	}
	resetFinalized := false
	stateCleared := false
	defer func() {
		if resetFinalized {
			return
		}
		if stateCleared {
			am.lifecycle.finishReset()
			return
		}
		am.lifecycle.abortReset()
	}()
	if killer, ok := am.workspaces.(workspace.OrphanKiller); ok && killer != nil {
		if err := killer.KillOrphanProcesses(am.runtimeContext()); err != nil {
			return fmt.Errorf("kill workspace orphan processes: %w", err)
		}
	}
	if resetter, ok := am.sessions.(sessions.Resetter); ok && resetter != nil {
		summary, err := resetter.ResetAll(sessions.NormalizeConversationRuntimeMode(am.runtimeMode), sessions.ResetMetadata{
			Source: source,
		})
		if err != nil {
			if am.bus != nil {
				am.bus.LogRuntime(am.runtimeContext(), runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "session_reset_failed",
					Failure:   failureEnvelope(err, "agent-manager", "reset_sessions"),
				})
			}
		} else if summary.OrphanedCount() > 0 && am.bus != nil {
			am.bus.LogRuntime(am.runtimeContext(), runtimepipeline.RuntimeLogEntry{
				Level:     "warn",
				Component: "runtime",
				Action:    "reset_orphaned_sessions",
				Message:   "Runtime reset orphaned live sessions",
				Detail:    resetOrphanedSessionsDetail(summary, source, sessions.NormalizeConversationRuntimeMode(am.runtimeMode)),
			})
		}
	}
	if am.bus != nil {
		if err := am.bus.ResetInMemoryState(); err != nil {
			return fmt.Errorf("reset event bus state: %w", err)
		}
	}
	source = strings.TrimSpace(source)
	var platformResetEvent events.Event
	hasPlatformResetEvent := false
	if platformResetSourceAuthorized(source) && am.bus != nil {
		payload, err := json.Marshal(map[string]any{
			"source":    source,
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return fmt.Errorf("marshal platform.reset payload: %w", err)
		}
		platformResetEvent = events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("platform.reset"), "runtime", "", payload, 0, "", "", events.EventEnvelope{}, time.Now())
		hasPlatformResetEvent = true
	}

	entities := map[string]struct{}{}
	am.mu.Lock()
	for _, cfg := range am.agentCfg {
		if entityID := cfg.EffectiveEntityID(); entityID != "" {
			entities[entityID] = struct{}{}
		}
	}
	am.agents = make(map[string]Agent)
	am.agentCfg = make(map[string]runtimeactors.AgentConfig)
	am.agentUpAt = make(map[string]time.Time)
	am.inFlight = make(map[string]struct{})
	am.mu.Unlock()
	am.poisonMu.Lock()
	am.poisonPanicCounts = make(map[string]int)
	am.poisonMu.Unlock()
	stateCleared = true
	if am.resetRuntimeOwnedState != nil {
		am.resetRuntimeOwnedState()
	}

	for entityID := range entities {
		if am.workspaces != nil {
			_ = am.workspaces.StopEntityWorkspace(am.runtimeContext(), entityID)
		}
	}
	if hasPlatformResetEvent {
		if err := am.bus.Publish(am.runtimeContext(), platformResetEvent); err != nil {
			return fmt.Errorf("publish platform.reset: %w", err)
		}
	}
	am.lifecycle.finishReset()
	resetFinalized = true
	return nil
}

func resetOrphanedSessionsDetail(summary sessions.ResetSummary, source string, runtimeMode sessions.RuntimeMode) map[string]any {
	detail := map[string]any{
		"orphaned_session_count": summary.OrphanedCount(),
		"orphaned_sessions":      make([]map[string]any, 0, len(summary.OrphanedSessions)),
	}
	if runtimeMode != "" {
		detail["runtime_mode"] = runtimeMode.String()
	}
	if source = strings.TrimSpace(source); source != "" {
		detail["source"] = source
	}
	records := detail["orphaned_sessions"].([]map[string]any)
	for _, item := range summary.OrphanedSessions {
		record := map[string]any{
			"session_id":         strings.TrimSpace(item.SessionID),
			"agent_id":           strings.TrimSpace(item.AgentID),
			"scope_key":          strings.TrimSpace(item.ScopeKey),
			"runtime_mode":       item.RuntimeMode.String(),
			"previous_status":    strings.TrimSpace(item.PreviousStatus),
			"termination_reason": strings.TrimSpace(item.TerminationReason),
		}
		if terminationDetail := strings.TrimSpace(item.TerminationDetail); terminationDetail != "" {
			record["termination_detail"] = terminationDetail
		}
		records = append(records, record)
	}
	detail["orphaned_sessions"] = records
	return detail
}

func (am *AgentManager) startAgentLoop(parent context.Context, agent Agent) {
	_ = am.startAgentLoopTransition(parent, agent, agent.Subscriptions(), "start", "", nil)
}

func (am *AgentManager) startAgentLoopWithSubscriptions(parent context.Context, agent Agent, subscriptions []events.EventType) {
	_ = am.startAgentLoopTransition(parent, agent, subscriptions, "start", "", nil)
}

func (am *AgentManager) startAgentLoopTransition(parent context.Context, agent Agent, subscriptions []events.EventType, trigger, operationID string, rec *PersistedAgent) error {
	loopCtx, token, done, err := am.lifecycle.replaceLoop(parent, agent.ID(), trigger, operationID, rec)
	if err != nil {
		return err
	}
	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(parent))
	if loopCtx == nil {
		return nil
	}

	ch := am.bus.Subscribe(agent.ID(), subscriptions...)
	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		defer func() {
			if releaseErr := am.lifecycle.releaseLoop(token, done); releaseErr != nil && am.bus != nil {
				failure := runtimefailures.FromError(releaseErr, "agent-manager", "release_agent_loop")
				_ = am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
					Level: "error", Component: "agent-manager", Action: "agent_loop_release_failed",
					AgentID: agent.ID(), Failure: &failure.Failure,
				})
			}
			_ = am.projectLifecycleDiagnostics(context.Background())
		}()
		consecutivePanics := 0
		for {
			panicked := false
			panicCtx := loopCtx
			panicText := ""
			lastEventType := ""
			stackTrace := ""
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
						panicText = fmt.Sprint(r)
						stackTrace = strings.TrimSpace(string(debug.Stack()))
					}
				}()
				for {
					select {
					case <-loopCtx.Done():
						return
					case evt, ok := <-ch:
						if !ok {
							return
						}
						if am.shutdownAdmissionClosed() {
							return
						}
						evtCtx := runtimecorrelation.WithInboundEvent(loopCtx, evt)
						evtCtx = runtimecorrelation.WithRunID(evtCtx, strings.TrimSpace(evt.RunID()))
						err, evtPanicked, evtPanicText, evtStackTrace := am.safeProcessEvent(evtCtx, agent, evt)
						if evtPanicked {
							panicCount := am.incrementPoisonPanicCount(agent.ID(), evt.ID())
							panicFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_event_panic", "agent-manager", "process_event", map[string]any{
								"agent_id": agent.ID(), "event_id": evt.ID(), "event_type": evt.Type(),
							}), "agent-manager", "process_event")
							am.writeReceipt(evtCtx, evt.ID(), agent.ID(), ReceiptStatusError, &panicFailure.Failure)
							if am.bus != nil {
								am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
									Level:     "error",
									Component: "agent-manager",
									Action:    "agent_event_panic",
									EventID:   strings.TrimSpace(evt.ID()),
									EventType: strings.TrimSpace(string(evt.Type())),
									AgentID:   agent.ID(),
									EntityID:  strings.TrimSpace(evt.EntityID()),
									Detail: map[string]any{
										"stack_trace": evtStackTrace,
									},
									Failure: &panicFailure.Failure,
								})
							}
							if panicCount >= poisonPanicQuarantineAt {
								am.quarantinePoisonEvent(evtCtx, agent.ID(), evt, panicCount, panicFailure.Failure)
								am.clearPoisonPanicCount(agent.ID(), evt.ID())
								consecutivePanics = 0
								continue
							}
							panicked = true
							panicCtx = evtCtx
							panicText = evtPanicText
							stackTrace = evtStackTrace
							lastEventType = strings.TrimSpace(string(evt.Type()))
							return
						}
						am.clearPoisonPanicCount(agent.ID(), evt.ID())
						consecutivePanics = 0
						if err != nil {
							if am.bus != nil {
								am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
									Level:     "error",
									Component: "agent-manager",
									Action:    "agent_event_failed",
									EventID:   strings.TrimSpace(evt.ID()),
									EventType: strings.TrimSpace(string(evt.Type())),
									AgentID:   agent.ID(),
									EntityID:  strings.TrimSpace(evt.EntityID()),
									Failure:   failureEnvelope(err, "agent-manager", "process_agent_event"),
								})
							}
						}
					}
				}
			}()
			if !panicked {
				return
			}
			consecutivePanics++
			am.handleAgentLoopPanic(panicCtx, agent, consecutivePanics, lastEventType, panicText, stackTrace)
			if consecutivePanics >= 5 {
				return
			}
			wait := panicBackoff(consecutivePanics)
			select {
			case <-loopCtx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
	return nil
}

func panicBackoff(consecutivePanics int) time.Duration {
	switch {
	case consecutivePanics <= 1:
		return 1 * time.Second
	case consecutivePanics == 2:
		return 5 * time.Second
	case consecutivePanics == 3:
		return 30 * time.Second
	case consecutivePanics == 4:
		return 2 * time.Minute
	default:
		return 10 * time.Minute
	}
}

func (am *AgentManager) handleAgentLoopPanic(ctx context.Context, agent Agent, consecutivePanics int, lastEventType, panicText, stackTrace string) {
	panicText = strings.TrimSpace(panicText)
	if panicText == "" {
		panicText = "unknown panic"
	}

	entityID := ""
	flowInstance := ""
	am.mu.RLock()
	cfg, ok := am.agentCfg[agent.ID()]
	am.mu.RUnlock()
	if ok {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if am.bus != nil {
		panicFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_loop_panic", "agent-manager", "agent_loop", map[string]any{
			"agent_id": agent.ID(), "count": consecutivePanics, "last_event_type": strings.TrimSpace(lastEventType),
		}), "agent-manager", "agent_loop")
		am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
			Level:     "error",
			Component: "agent-manager",
			Action:    "agent_loop_panic",
			AgentID:   agent.ID(),
			EntityID:  entityID,
			Detail: map[string]any{
				"count":           consecutivePanics,
				"last_event_type": strings.TrimSpace(lastEventType),
				"stack_trace":     stackTrace,
			},
			Failure: &panicFailure.Failure,
		})
	}
	panicFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_loop_panic", "agent-manager", "agent_loop", map[string]any{
		"agent_id": agent.ID(), "count": consecutivePanics, "last_event_type": strings.TrimSpace(lastEventType),
	}), "agent-manager", "agent_loop")

	eventCtx := am.runtimePlatformControlEventContext(ctx)
	if err := am.bus.Publish(eventCtx, events.NewRuntimeDiagnosticEvent(uuid.NewString(), events.EventType("platform.agent_panic"), "runtime", "", mustJSON(map[string]any{
		"agent_id":        agent.ID(),
		"flow_instance":   flowInstance,
		"entity_id":       entityID,
		"failure":         panicFailure.Failure,
		"stack_trace":     stackTrace,
		"conversation_id": "",
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
	}), 0, "", "", events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, time.Now().UTC())); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_panic_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Failure:   failureEnvelope(err, "agent-manager", "publish_agent_panic"),
			})
		}
	}

	if consecutivePanics < 5 {
		return
	}

	if ok && am.store != nil {
		_ = am.store.UpsertAgent(ctx, PersistedAgent{
			Config:          cfg,
			ParentAgentID:   cfg.ParentAgent,
			CoordinatorID:   am.resolveManagerAgentID(agent.ID()),
			Status:          "failed",
			HiredBy:         "runtime",
			TemplateVersion: "",
			StartedAt:       time.Now(),
		})
	}

	if err := am.bus.Publish(eventCtx, events.NewRuntimeDiagnosticEvent(uuid.NewString(), events.EventType("platform.agent_failed"), "runtime", "", mustJSON(map[string]any{
		"agent_id":        agent.ID(),
		"flow_instance":   flowInstance,
		"entity_id":       entityID,
		"failure":         panicFailure.Failure,
		"retry_count":     consecutivePanics,
		"last_event_type": strings.TrimSpace(lastEventType),
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
	}), 0, "", "", events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, time.Now().UTC())); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_failed_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Failure:   failureEnvelope(err, "agent-manager", "publish_agent_failed"),
			})
		}
	}
}
