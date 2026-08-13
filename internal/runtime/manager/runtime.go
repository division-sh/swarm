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
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type AgentDirectiveRunTargetResolver interface {
	ResolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error)
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

func (am *AgentManager) Restart(ctx context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	if am.shutdownAdmissionClosed() {
		return runtimeagentcontrol.RestartResult{}, errRuntimeShuttingDown
	}
	agentID := strings.TrimSpace(req.AgentID)
	if agentID == "" {
		return runtimeagentcontrol.RestartResult{}, errors.New("agent id is required")
	}
	identity, err := am.lifecycle.resolveAgentTarget(agentID, req.FlowInstance, false)
	if err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	if _, ok := am.lifecycle.executionSnapshotByIdentity(identity); !ok {
		return runtimeagentcontrol.RestartResult{}, agentControlNotFound(identity.Description())
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, err = am.bindRuntimeOperationContext(ctx)
	if err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	operationID := strings.TrimSpace(req.OperationID)
	if operationID == "" {
		operationID = uuid.NewString()
	}
	if _, err := am.replaceExecutionIdentityConfigWithTopology(ctx, identity, "restart", operationID, nil, am.semanticSource, false, nil, nil); err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	token, _ := am.lifecycle.tokenIdentity(identity)
	return runtimeagentcontrol.RestartResult{
		AgentID: agentID, FlowInstance: identity.FlowInstance(), OperationID: operationID, Generation: token.Generation,
	}, nil
}

func (am *AgentManager) Shutdown() error {
	return am.ShutdownWithOptions(DefaultShutdownOptions())
}

func (am *AgentManager) ShutdownWithOptions(opts ShutdownOptions) error {
	grace, err := ResolveShutdownGrace(opts.Grace)
	if err != nil {
		return err
	}
	transition := am.lifecycle.requestShutdownTransition()
	executor, claimed, err := am.lifecycle.claimUnwatchedTransition(transition, runtimeLifecycleTransitionShutdown)
	if err != nil {
		return err
	}
	if claimed {
		go am.completeClaimedShutdownTransition(transition, executor)
	}
	return waitForRuntimeLifecycleTransition(transition, grace, "shutdown drain")
}

func waitForRuntimeLifecycleTransition(transition *runtimeLifecycleTransition, grace time.Duration, operation string) error {
	if transition == nil {
		return nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	timedOut := false
	select {
	case <-transition.done:
	case <-timer.C:
		timedOut = true
		<-transition.done
	}
	if !timedOut {
		return transition.result
	}
	timeoutErr := fmt.Errorf("agent manager %s timed out after %s", strings.TrimSpace(operation), grace)
	if transition.result != nil {
		return errors.Join(timeoutErr, transition.result)
	}
	return timeoutErr
}

func (am *AgentManager) Count() int {
	return len(am.lifecycle.executionIdentities())
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

func (am *AgentManager) ResolveAgentConfig(agentID, flowInstance string) (runtimeactors.AgentConfig, error) {
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		return runtimeactors.AgentConfig{}, err
	}
	execution, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	if !ok {
		return runtimeactors.AgentConfig{}, fmt.Errorf("%w: %s", ErrAgentNotFound, identity.Description())
	}
	return execution.Config, nil
}

func (am *AgentManager) getAgentConfigIdentity(identity runtimeagentidentity.Identity) (runtimeactors.AgentConfig, bool) {
	execution, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	return execution.Config, ok
}

func (am *AgentManager) ListAgentConfigs() []runtimeactors.AgentConfig {
	return am.lifecycle.executionConfigs()
}

type poisonPanicKey struct {
	Agent   runtimeagentidentity.Identity
	EventID string
}

func (am *AgentManager) poisonKey(identity runtimeagentidentity.Identity, eventID string) (poisonPanicKey, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return poisonPanicKey{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return poisonPanicKey{}, fmt.Errorf("poison event id is required")
	}
	return poisonPanicKey{Agent: identity, EventID: eventID}, nil
}

func (am *AgentManager) incrementPoisonPanicCount(identity runtimeagentidentity.Identity, eventID string) (int, error) {
	key, err := am.poisonKey(identity, eventID)
	if err != nil {
		return 0, err
	}
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	am.poisonPanicCounts[key]++
	return am.poisonPanicCounts[key], nil
}

func (am *AgentManager) clearPoisonPanicCount(identity runtimeagentidentity.Identity, eventID string) {
	key, err := am.poisonKey(identity, eventID)
	if err != nil {
		return
	}
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
	quarantined, constructErr := newPlatformCausalRuntimeControlEvent(events.LineageFromEvent(evt), events.EventType("platform.event_quarantined"), mustJSON(payload), events.EventEnvelope{}, time.Now().UTC())
	if constructErr != nil {
		return
	}
	if err := am.bus.Publish(eventCtx, quarantined); err != nil {
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

func deterministicOutputEventID(inbound events.Event, identity runtimeagentidentity.Identity, index int, out events.Event) (string, error) {
	return DeterministicOutputEventID(inbound, identity, index, out)
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

type EventExistenceReader interface {
	EventExists(ctx context.Context, eventID string) (bool, error)
}

func (am *AgentManager) shouldSkipAlreadyPublishedOutput(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}
	reader := am.roles.EventExistence
	if reader == nil {
		return false
	}
	exists, err := reader.EventExists(ctx, eventID)
	if err != nil {
		return false
	}
	return exists
}

func (am *AgentManager) safeProcessEventOwned(ctx context.Context, agent Agent, evt events.Event) (err error, panicked bool, panicText string, stackTrace string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicText = fmt.Sprint(r)
			stackTrace = strings.TrimSpace(string(debug.Stack()))
		}
	}()
	err = am.processEventDetailedOwned(ctx, agent, evt).err
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
	var err error
	ctx, err = am.bindRuntimeOperationContext(ctx)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	if am.bus == nil {
		return runtimeagentcontrol.SendDirectiveResult{}, errors.New("event bus is not configured")
	}
	ctx, err = am.bus.AdmitBundleSourceFact(ctx)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	if am.shutdownAdmissionClosed() {
		return runtimeagentcontrol.SendDirectiveResult{}, agentControlNotRunning(req.AgentID, runtimeagentcontrol.StatusTerminated)
	}
	agentID := strings.TrimSpace(req.AgentID)
	req.AgentID = agentID
	req.FlowInstance = strings.Trim(strings.TrimSpace(req.FlowInstance), "/")
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
	identity, err := am.lifecycle.resolveAgentTarget(agentID, req.FlowInstance, false)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	req.FlowInstance = identity.FlowInstance()
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
	if _, err := am.directiveBoardAgentIdentity(identity); err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	target, err := am.resolveAgentDirectiveRunTarget(ctx, identity, req.RunID)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	now := time.Now().UTC()
	operationID := uuid.NewString()
	eventID := uuid.NewString()
	directiveEvent, err := runtimeagentcontrol.NewDirectiveEvent(req, target, operationID, eventID, now, am.executionPosture)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	admittedDirective, err := events.AdmitForPersistence(directiveEvent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, fmt.Errorf("admit directive event: %w", err)
	}
	reservationCtx := runtimecorrelation.WithRunID(am.runtimePlatformControlEventContext(ctx), target.RunID)
	reservation, err := operationStore.ReserveDirectiveOperation(reservationCtx, runtimeagentcontrol.ReserveDirectiveOperationRequest{
		Operation: runtimeagentcontrol.DirectiveOperation{
			OperationID:      operationID,
			Method:           runtimeagentcontrol.DirectiveOperationMethod,
			ActorTokenID:     req.ActorTokenID,
			IdempotencyKey:   req.IdempotencyKey,
			RequestHash:      req.RequestHash,
			AgentIdentity:    identity,
			Directive:        req.Directive,
			RequestedRunID:   req.RunID,
			ResolvedRunID:    target.RunID,
			RunIDResolution:  target.Mode,
			Source:           req.Source,
			OperatorID:       req.OperatorID,
			DirectiveEventID: eventID,
			State:            runtimeagentcontrol.DirectiveOperationPrepared,
		},
		Event: admittedDirective,
		Now:   now,
	})
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	return am.continueDirectiveOperation(ctx, operationStore, reservation.Operation)
}

func (am *AgentManager) directiveOperationStore() (runtimeagentcontrol.DirectiveOperationStore, error) {
	if am == nil || am.roles.DirectiveOperations == nil {
		return nil, errors.New("directive operation store is required")
	}
	return am.roles.DirectiveOperations, nil
}

func (am *AgentManager) directiveBoardAgentIdentity(identity runtimeagentidentity.Identity) (BoardInteractiveAgent, error) {
	execution, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	if !ok {
		return nil, agentControlNotFound(identity.Description())
	}
	chatAgent, ok := execution.Agent.(BoardInteractiveAgent)
	if !ok {
		return nil, agentControlNotRunning(identity.Description(), runtimeagentcontrol.StatusIdle)
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
	lease, err := am.lifecycle.acquireExecutionIdentity(ctx, op.AgentIdentity, "execute_directive", false)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	defer lease.Release()
	chatAgent, ok := lease.Agent.(BoardInteractiveAgent)
	if !ok {
		return runtimeagentcontrol.SendDirectiveResult{}, agentControlNotRunning(op.AgentIdentity.Description(), runtimeagentcontrol.StatusIdle)
	}
	ownerID := uuid.NewString()
	admission, err := store.AdmitDirectiveExecution(ctx, runtimeagentcontrol.DirectiveExecutionAdmissionRequest{
		OperationID: op.OperationID, OwnerID: ownerID, Now: time.Now().UTC(), Lease: directiveExecutionLease,
		ExecutionPosture: am.executionPosture,
	})
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	admitted := admission.Operation
	directiveEvent := admission.Event.Event()
	directiveCtx := runtimecorrelation.WithRunID(lease.Context, strings.TrimSpace(directiveEvent.RunID()))
	directiveCtx = runtimebus.WithInboundEvent(directiveCtx, directiveEvent)
	heartbeatConfig := am.directiveHeartbeat.normalized()
	heartbeatLease, err := am.beginWork(ctx, "directive heartbeat")
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(heartbeatLease.Context())
	heartbeatDone := make(chan struct{})
	go func() {
		defer func() { _ = heartbeatLease.Done() }()
		runDirectiveExecutionHeartbeat(heartbeatCtx, heartbeatDone, store, admitted.OperationID, ownerID, heartbeatConfig)
	}()
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
		AgentID:            admitted.AgentID(),
		FlowInstance:       admitted.FlowInstance(),
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
	result.AgentID = op.AgentID()
	result.FlowInstance = op.FlowInstance()
	return result, nil
}

func directiveRequestHash(req runtimeagentcontrol.SendDirectiveRequest) (string, error) {
	raw, err := json.Marshal(struct {
		AgentID      string `json:"agent_id"`
		FlowInstance string `json:"flow_instance,omitempty"`
		Directive    string `json:"directive"`
		RunID        string `json:"run_id,omitempty"`
	}{
		AgentID: req.AgentID, FlowInstance: req.FlowInstance, Directive: req.Directive, RunID: req.RunID,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func (am *AgentManager) resolveAgentDirectiveRunTarget(ctx context.Context, identity runtimeagentidentity.Identity, explicitRunID string) (runtimeagentcontrol.RunTargetResolution, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	explicitRunID = strings.TrimSpace(explicitRunID)
	resolver := am.roles.DirectiveTargets
	if resolver == nil {
		return runtimeagentcontrol.RunTargetResolution{}, errors.New("directive run target resolver is required")
	}
	target, err := resolver.ResolveAgentDirectiveRunTarget(ctx, identity, explicitRunID)
	if err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, err
	}
	return target.Normalized(), nil
}

func (am *AgentManager) Run(ctx context.Context) error {
	if _, err := managedexecution.Require(ctx); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "managed_execution_admission_missing", "agent-manager", "start_loops", nil, err)
	}
	if am.shutdownAdmissionClosedLocked() {
		return errRuntimeShuttingDown
	}
	if am.workOwner == nil {
		return errors.New("agent manager requires a runtime work occurrence")
	}
	runCtx, started, err := am.lifecycle.beginRun(ctx, AgentRunModeStandard, am.workOwner)
	if err != nil {
		return fmt.Errorf("admit manager run occurrence: %w", err)
	}
	if !started {
		return fmt.Errorf("agent manager is already running")
	}
	am.runMu.Lock()
	am.authBreakerTripped = false
	am.runMu.Unlock()
	if err := am.startShutdownWatcher(runCtx, "manager shutdown watcher"); err != nil {
		return errors.Join(err, am.lifecycle.abortRunStart(err))
	}

	for _, identity := range am.lifecycle.executionIdentities() {
		if _, err := am.replaceExecutionIdentityConfigWithTopology(runCtx, identity, "start", "", nil, am.semanticSource, false, nil, nil); err != nil {
			transition := am.lifecycle.requestShutdownTransition()
			grace, _ := ResolveShutdownGrace(DefaultShutdownOptions().Grace)
			return errors.Join(err, waitForRuntimeLifecycleTransition(transition, grace, "failed agent start shutdown drain"))
		}
	}

	retryLease, err := am.beginStandingWork(runCtx, "manager readiness reconciliation")
	if err != nil {
		transition := am.lifecycle.requestShutdownTransition()
		grace, _ := ResolveShutdownGrace(DefaultShutdownOptions().Grace)
		_ = waitForRuntimeLifecycleTransition(transition, grace, "failed-start shutdown drain")
		return err
	}
	retryDone := make(chan struct{})
	if !am.lifecycle.setRetryDone(retryDone) {
		_ = retryLease.Done()
		transition := am.lifecycle.requestShutdownTransition()
		grace, _ := ResolveShutdownGrace(DefaultShutdownOptions().Grace)
		_ = waitForRuntimeLifecycleTransition(transition, grace, "failed-start shutdown drain")
		return errRuntimeShuttingDown
	}
	go func() {
		defer close(retryDone)
		defer func() { _ = retryLease.Done() }()
		am.readinessReconciliationLoop(retryLease.Context())
	}()
	return nil
}

// RunAuthoritativeDeliveryOnly starts agent loops with authoritative recipient
// channels only. It intentionally avoids live subscription patterns and
// retry/recovery loops so selected-fork execution can consume canonical
// recipient planning without reintroducing subscription-derived recipient truth.
func (am *AgentManager) RunAuthoritativeDeliveryOnly(ctx context.Context) error {
	if _, err := managedexecution.Require(ctx); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "managed_execution_admission_missing", "agent-manager", "start_authoritative_loops", nil, err)
	}
	if am.shutdownAdmissionClosedLocked() {
		return errRuntimeShuttingDown
	}
	if am.workOwner == nil {
		return errors.New("agent manager requires a runtime work occurrence")
	}
	runCtx, started, err := am.lifecycle.beginRun(ctx, AgentRunModeAuthoritativeDeliveryOnly, am.workOwner)
	if err != nil {
		return fmt.Errorf("admit authoritative manager run occurrence: %w", err)
	}
	if !started {
		return fmt.Errorf("agent manager is already running")
	}
	am.runMu.Lock()
	am.authBreakerTripped = false
	am.runMu.Unlock()
	if err := am.startShutdownWatcher(runCtx, "authoritative manager shutdown watcher"); err != nil {
		return errors.Join(err, am.lifecycle.abortRunStart(err))
	}

	for _, identity := range am.lifecycle.executionIdentities() {
		if _, err := am.replaceExecutionIdentityConfigWithTopology(runCtx, identity, "start", "", nil, am.semanticSource, false, nil, nil); err != nil {
			transition := am.lifecycle.requestShutdownTransition()
			grace, _ := ResolveShutdownGrace(DefaultShutdownOptions().Grace)
			return errors.Join(err, waitForRuntimeLifecycleTransition(transition, grace, "failed authoritative agent start shutdown drain"))
		}
	}
	return nil
}

func (am *AgentManager) startShutdownWatcher(runCtx context.Context, kind string) error {
	watchLease, err := am.lifecycle.takeShutdownWatcherExecutor()
	if err != nil {
		return fmt.Errorf("admit %s: %w", strings.TrimSpace(kind), err)
	}
	go func() {
		<-runCtx.Done()
		transition := am.lifecycle.requestShutdownTransition()
		if !am.lifecycle.claimTransition(transition, runtimeLifecycleTransitionShutdown) {
			_ = watchLease.Done()
			return
		}
		am.completeClaimedShutdownTransition(transition, watchLease)
	}()
	return nil
}

func (am *AgentManager) completeClaimedShutdownTransition(transition *runtimeLifecycleTransition, executor *worklifetime.Lease) {
	_, done := am.lifecycle.cancelShutdownWork()
	for _, wait := range done {
		<-wait
	}
	runSettleErr := am.lifecycle.retireRunOwner(context.Background())
	settleErr := executor.Done()
	am.lifecycle.completeShutdownTransition(transition, errors.Join(runSettleErr, settleErr))
}

func (am *AgentManager) Recover(ctx context.Context) error {
	if _, err := am.HydrateForStartup(ctx); err != nil {
		return err
	}
	_, err := am.RecoverAfterStartupAdmission(ctx)
	return err
}

func (am *AgentManager) RecoverWithStartupReplayDiagnostics(ctx context.Context) (StartupReplaySummary, error) {
	if _, err := am.HydrateForStartup(ctx); err != nil {
		return StartupReplaySummary{}, err
	}
	return am.RecoverAfterStartupAdmission(ctx)
}

func (am *AgentManager) ReconcileDirectiveOperations(ctx context.Context) error {
	if am == nil || am.roles.DirectiveOperations == nil {
		return nil
	}
	operationStore := am.roles.DirectiveOperations
	if _, err := operationStore.ReconcileDirectiveOperations(ctx, time.Now().UTC(), directiveOperationTTL); err != nil {
		return fmt.Errorf("reconcile directive operations: %w", err)
	}
	return nil
}

func (am *AgentManager) projectLifecycleDiagnostics(ctx context.Context) error {
	if am == nil || am.bus == nil || am.lifecycle == nil {
		return nil
	}
	store := am.roles.LifecycleDiagnostics
	if store == nil {
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

func (am *AgentManager) HydrateForStartup(ctx context.Context) (StartupReplaySummary, error) {
	summary := StartupReplaySummary{}
	if am.store == nil {
		return summary, nil
	}
	if recoveryStore := am.roles.EffectsRecovery; recoveryStore != nil {
		request := runtimeeffects.NewRecoveryRequest(time.Now().UTC(), am.executionPosture)
		if _, err := recoveryStore.ReconcileExternalEffectAttempts(ctx, request); err != nil {
			return summary, fmt.Errorf("reconcile external effect attempts: %w", err)
		}
	}
	if am.budget != nil {
		if err := am.budget.ProjectRecoveryBudgetState(ctx); err != nil {
			return summary, fmt.Errorf("project recovered budget state: %w", err)
		}
	}
	if err := am.projectLifecycleDiagnostics(ctx); err != nil {
		return summary, fmt.Errorf("project lifecycle diagnostics: %w", err)
	}

	am.mu.RLock()
	hydrated := am.startupAgentsHydrated
	am.mu.RUnlock()
	if !hydrated {
		return summary, errors.New("static declaration reconciliation must hydrate agents before startup recovery")
	}
	if err := am.reconcileDynamicFlowRuntimeReadinessForStartup(ctx); err != nil {
		return summary, &dynamicFlowRuntimeReadinessFinalizationError{cause: err}
	}
	if err := am.restoreFlowInstanceRoutes(ctx); err != nil {
		return summary, err
	}
	if err := am.restoreSelectedContractRouteRecoveries(ctx); err != nil {
		return summary, err
	}
	return summary, nil
}

func (am *AgentManager) RecoverAfterStartupAdmission(ctx context.Context) (StartupReplaySummary, error) {
	if _, err := managedexecution.Require(ctx); err != nil {
		return StartupReplaySummary{}, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "managed_execution_admission_missing", "agent-manager", "startup_replay", nil, err)
	}
	summary := StartupReplaySummary{}
	if am == nil || am.bus == nil {
		return summary, nil
	}
	if err := runtimepipeline.NewRecoveryManagerWith(am.bus).Recover(ctx); err != nil {
		return summary, fmt.Errorf("recover pipeline receipts: %w", err)
	}
	return summary, nil
}

type RecoverableStateSnapshot struct {
	PersistedAgentCount                         int
	PersistedFlowInstanceRouteCount             int
	PersistedSelectedContractRouteRecoveryCount int
	PendingDynamicFlowRuntimeReadinessCount     int
	ReplayEligibleEventPresent                  bool
}

func (s RecoverableStateSnapshot) HasRecoverableWork() bool {
	return s.PersistedAgentCount > 0 ||
		s.PersistedFlowInstanceRouteCount > 0 ||
		s.PersistedSelectedContractRouteRecoveryCount > 0 ||
		s.PendingDynamicFlowRuntimeReadinessCount > 0 ||
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
	if s.PendingDynamicFlowRuntimeReadinessCount > 0 {
		classes = append(classes, "pending dynamic flow runtime readiness")
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
		"pending_dynamic_flow_runtime_readiness_count":     s.PendingDynamicFlowRuntimeReadinessCount,
		"replay_eligible_event_present":                    s.ReplayEligibleEventPresent,
	}
}

func (am *AgentManager) RecoverableStateSnapshot(ctx context.Context) (RecoverableStateSnapshot, error) {
	snapshot := RecoverableStateSnapshot{}
	if am == nil {
		return snapshot, nil
	}
	if am.workflowInstances != nil {
		items, err := am.workflowInstances.ListDynamicFlowRuntimeReadiness(ctx)
		if err != nil {
			return RecoverableStateSnapshot{}, fmt.Errorf("list dynamic flow runtime readiness: %w", err)
		}
		for _, item := range items {
			if item.Pending() {
				snapshot.PendingDynamicFlowRuntimeReadinessCount++
			}
		}
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
	presence, err := am.bus.PipelineWorkPresence(ctx)
	if err != nil {
		return RecoverableStateSnapshot{}, fmt.Errorf("load pipeline work presence: %w", err)
	}
	snapshot.ReplayEligibleEventPresent = presence.Any()
	return snapshot, nil
}

func (am *AgentManager) restoreFlowInstanceRoutes(ctx context.Context) error {
	if am == nil || am.bus == nil {
		return nil
	}
	restorer := am.roles.RouteRestorer
	if restorer == nil {
		return nil
	}
	routeStore := am.roles.FlowRoutes
	if routeStore == nil {
		return nil
	}
	routes, err := routeStore.ListFlowInstanceRoutes(ctx)
	if err != nil {
		return fmt.Errorf("list persisted flow instance routes: %w", err)
	}
	readinessPaths := map[string]struct{}{}
	if am.workflowInstances != nil {
		keys, err := am.workflowInstances.ListDynamicFlowRuntimeReadinessKeys(ctx)
		if err != nil {
			return fmt.Errorf("list dynamic flow runtime readiness route owners: %w", err)
		}
		for _, key := range keys {
			readinessPaths[strings.Trim(strings.TrimSpace(key.InstancePath), "/")] = struct{}{}
		}
	}
	for _, route := range routes {
		if _, owned := readinessPaths[strings.Trim(strings.TrimSpace(route.InstancePath), "/")]; owned {
			continue
		}
		req, err := am.restoredFlowInstanceRouteMaterializationRequest(ctx, route)
		if err != nil {
			return err
		}
		if err := restorer.PublishPersistedFlowInstanceRoute(req); err != nil {
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
	projection, err := am.workflowInstances.LoadRouteRecoveryProjection(ctx, route)
	if err != nil {
		return runtimebus.FlowInstanceRouteMaterializationRequest{}, fmt.Errorf("load flow instance route recovery projection %s: %w", route.InstancePath, err)
	}
	vars := flowActivationVars(runtimepipeline.FlowInstanceActivationRequest{
		Instance: projection.Identity,
		Config:   projection.Config,
	})
	return runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity:            route,
		ActivationVariables: vars,
	}, nil
}

func (am *AgentManager) readinessReconciliationLoop(ctx context.Context) {
	if am.store == nil && am.workflowInstances == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-am.dynamicFlowReadinessSignal:
			if err := am.reconcilePendingDynamicFlowRuntimeReadiness(ctx); err != nil && am.bus != nil {
				_ = am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "dynamic_flow_runtime_readiness_retry_failed",
					Failure:   failureEnvelope(err, "agent-manager", "dynamic_flow_runtime_readiness_retry"),
				})
			}
		}
	}
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
	shutdown, reset := am.lifecycle.requestResetTransition()
	executor, claimed, err := am.lifecycle.claimUnwatchedTransition(shutdown, runtimeLifecycleTransitionShutdown)
	if err != nil {
		return err
	}
	if claimed {
		go am.completeClaimedShutdownTransition(shutdown, executor)
	}
	if shutdown != nil {
		grace, _ := ResolveShutdownGrace(DefaultShutdownOptions().Grace)
		if err := waitForRuntimeLifecycleTransition(shutdown, grace, "reset shutdown drain"); err != nil {
			if am.lifecycle.claimTransition(reset, runtimeLifecycleTransitionReset) {
				am.lifecycle.completeResetTransition(reset, err, false)
			} else if reset != nil {
				<-reset.done
			}
			return err
		}
	}
	if !am.lifecycle.claimTransition(reset, runtimeLifecycleTransitionReset) {
		if reset == nil {
			return nil
		}
		<-reset.done
		return reset.result
	}
	stateCleared, err := am.executeResetRuntimeState(source)
	am.lifecycle.completeResetTransition(reset, err, stateCleared)
	return err
}

func (am *AgentManager) executeResetRuntimeState(source string) (bool, error) {
	if killer, ok := am.workspaces.(workspace.OrphanKiller); ok && killer != nil {
		if err := killer.KillOrphanProcesses(am.runtimeContext()); err != nil {
			return false, fmt.Errorf("kill workspace orphan processes: %w", err)
		}
	}
	if am.sessionResetter == nil {
		return false, fmt.Errorf("session reset owner is required")
	}
	{
		summary, err := am.sessionResetter.ResetAll(sessions.ResetMetadata{
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
				Detail:    resetOrphanedSessionsDetail(summary, source),
			})
		}
	}
	if am.bus != nil {
		if err := am.bus.ResetInMemoryState(); err != nil {
			return false, fmt.Errorf("reset event bus state: %w", err)
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
			return false, fmt.Errorf("marshal platform.reset payload: %w", err)
		}
		platformResetEvent, err = newPlatformStandaloneRuntimeControlEvent(am.executionPosture, events.EventType("platform.reset"), payload, events.EventEnvelope{}, time.Now())
		if err != nil {
			return false, err
		}
		hasPlatformResetEvent = true
	}

	entities := map[string]struct{}{}
	for _, cfg := range am.lifecycle.executionConfigs() {
		if entityID := cfg.EffectiveEntityID(); entityID != "" {
			entities[entityID] = struct{}{}
		}
	}
	am.poisonMu.Lock()
	am.poisonPanicCounts = make(map[poisonPanicKey]int)
	am.poisonMu.Unlock()
	stateCleared := true
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
			return stateCleared, fmt.Errorf("publish platform.reset: %w", err)
		}
	}
	return stateCleared, nil
}

func resetOrphanedSessionsDetail(summary sessions.ResetSummary, source string) map[string]any {
	detail := map[string]any{
		"orphaned_session_count": summary.OrphanedCount(),
		"orphaned_sessions":      make([]map[string]any, 0, len(summary.OrphanedSessions)),
	}
	if source = strings.TrimSpace(source); source != "" {
		detail["source"] = source
	}
	records := detail["orphaned_sessions"].([]map[string]any)
	for _, item := range summary.OrphanedSessions {
		record := map[string]any{
			"session_id":         strings.TrimSpace(item.SessionID),
			"agent_id":           strings.TrimSpace(item.AgentID),
			"run_id":             strings.TrimSpace(item.RunID),
			"flow_instance":      strings.TrimSpace(item.FlowInstance),
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

type replaceExecutionResult struct {
	previous     runtimeactors.AgentConfig
	config       runtimeactors.AgentConfig
	transitioned bool
}

func (am *AgentManager) replaceExecutionIdentityConfigWithTopology(
	parent context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	operationID string,
	patch *runtimeactors.AgentConfig,
	source semanticview.Source,
	exact bool,
	topology *runtimeagenttopology.Admission,
	expected *runtimeactors.AgentConfig,
) (replaceExecutionResult, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return replaceExecutionResult{}, err
	}
	return am.replaceExecutionTargetConfigWithTopology(parent, identity, trigger, operationID, patch, source, exact, topology, expected)
}

func (am *AgentManager) replaceExecutionTargetConfigWithTopology(
	parent context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	operationID string,
	patch *runtimeactors.AgentConfig,
	source semanticview.Source,
	exact bool,
	topology *runtimeagenttopology.Admission,
	expected *runtimeactors.AgentConfig,
) (replaceExecutionResult, error) {
	am.lifecycle.executionPublishMu.Lock()
	defer am.lifecycle.executionPublishMu.Unlock()
	cell, err := am.lifecycle.lockIdentityOperation(identity)
	if err != nil {
		return replaceExecutionResult{}, err
	}
	defer cell.opMu.Unlock()
	identity = cell.identity
	agentID := identity.AgentID()

	am.lifecycle.mu.Lock()
	execution := cell.execution
	if execution == nil || execution.agent == nil || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		am.lifecycle.mu.Unlock()
		return replaceExecutionResult{}, fmt.Errorf("%w: %s", ErrAgentNotFound, strings.TrimSpace(agentID))
	}
	current := snapshotExecution(execution)
	currentPhase := cell.phase
	currentRevision := cell.configRevision
	currentTopology := cell.topology
	currentLoopLive := execution.loopDone != nil && execution.routeToken.Valid()
	am.lifecycle.mu.Unlock()
	if expected != nil {
		expectedRevision, revisionErr := lifecycleConfigRevision(PersistedAgent{Config: *expected})
		if revisionErr != nil {
			return replaceExecutionResult{}, fmt.Errorf("validate expected agent configuration: %w", revisionErr)
		}
		if expectedRevision != currentRevision {
			return replaceExecutionResult{}, runtimefailures.New(
				runtimefailures.ClassLifecycleConflict,
				"agent_config_changed",
				"agent-lifecycle",
				trigger,
				map[string]any{"agent_id": agentID},
			)
		}
	}
	if trigger == "start" && currentPhase == AgentLifecycleRunning && currentLoopLive {
		return replaceExecutionResult{previous: current.Config, config: current.Config}, nil
	}

	candidate := current
	candidateAdmission := current.Admission
	var rec *PersistedAgent
	subordinate := sessions.LifecycleMutationPlan{}
	if patch != nil {
		updated := *patch
		if !exact {
			updated = mergeAgentConfig(current.Config, *patch)
		}
		if updated.ID == "" {
			updated.ID = strings.TrimSpace(agentID)
		}
		if updated.ID != strings.TrimSpace(agentID) {
			return replaceExecutionResult{}, fmt.Errorf("agent id mismatch: target=%s config.id=%s", strings.TrimSpace(agentID), updated.ID)
		}
		updatedIdentity, err := updated.ConcreteIdentity()
		if err != nil {
			return replaceExecutionResult{}, err
		}
		if updatedIdentity != identity {
			return replaceExecutionResult{}, fmt.Errorf(
				"agent identity mismatch: target=%s config=%s",
				identity.Description(),
				updatedIdentity.Description(),
			)
		}
		if err := am.resolveAgentModel(&updated); err != nil {
			return replaceExecutionResult{}, err
		}
		if err := am.executionPosture.Admit(updated.ExecutionMode, "agent lifecycle replacement"); err != nil {
			return replaceExecutionResult{}, err
		}
		if err := bindCanonicalAgentPrompt(source, &updated); err != nil {
			return replaceExecutionResult{}, err
		}
		subscriptionAdmission, err := admitAgentConfigSubscriptions(source, &updated, nil)
		if err != nil {
			return replaceExecutionResult{}, err
		}
		if err := am.validateNativeToolAdmission(am.runtimeContext(), updated); err != nil {
			return replaceExecutionResult{}, err
		}
		if err := agentmemory.ValidateFlowOwnership(updated.Memory, updated.CanonicalFlowPath()); err != nil {
			return replaceExecutionResult{}, fmt.Errorf("invalid agent memory plan: %w", err)
		}
		candidateRecord := PersistedAgent{Config: updated, Status: "active", HiredBy: "reconfigure"}
		revision, err := lifecycleConfigRevision(candidateRecord)
		if err != nil {
			return replaceExecutionResult{}, err
		}
		replacementTopology := currentTopology
		if topology != nil {
			replacementTopology = *topology
		}
		if lifecycleProjectionEqual(revision, replacementTopology, currentRevision, currentTopology) {
			return replaceExecutionResult{previous: current.Config, config: current.Config}, nil
		}
		candidateAgent, err := am.buildAgent(updated)
		if err != nil {
			return replaceExecutionResult{}, err
		}
		candidate = agentExecutionSnapshot{
			Agent: candidateAgent, Config: updated,
			Subscriptions: admittedSubscriptionEventTypes(subscriptionAdmission),
			Admission:     subscriptionAdmission,
			StartedAt:     current.StartedAt,
		}
		candidateAdmission = subscriptionAdmission
		rec = &candidateRecord
		subordinate = reconfigureSessionMutationPlan(current.Config, updated)
	}
	if patch == nil {
		if err := am.executionPosture.Admit(candidate.Config.ExecutionMode, "agent lifecycle replacement"); err != nil {
			return replaceExecutionResult{}, err
		}
	}

	runCtx, runMode, runtimeRunning := am.lifecycle.runSnapshot()
	routeBus := am.lifecycle.routes
	if runtimeRunning {
		if routeBus == nil {
			return replaceExecutionResult{}, errors.New("event bus does not support generation-owned agent routes")
		}
	}
	var loopWorkLease *worklifetime.Lease
	var loopWorkOwner worklifetime.Occurrence
	var loopWorkOwnerFound bool
	var preparedRoute runtimebus.AgentRoutePreparation
	var proposedToken runtimeeffects.LifecycleToken
	if runtimeRunning {
		loopWorkLease, err = am.beginStandingWork(runCtx, "agent execution loop")
		if err != nil {
			return replaceExecutionResult{}, fmt.Errorf("admit agent execution loop: %w", err)
		}
		proposedToken, err = am.lifecycle.prepareLoopTokenLocked(identity, cell)
		if err != nil {
			_ = loopWorkLease.Done()
			return replaceExecutionResult{}, err
		}
		routeAdmission := candidateAdmission
		if runMode == AgentRunModeAuthoritativeDeliveryOnly {
			routeAdmission = routeAdmission.CarrierOnly()
		}
		preparedRoute = routeBus.PrepareAgentRoute(proposedToken, routeAdmission)
		if preparedRoute == nil {
			_ = loopWorkLease.Done()
			return replaceExecutionResult{}, errors.New("failed to prepare generation-owned agent route")
		}
	}
	cleanupPrepared := func() error {
		var cleanupErr error
		if preparedRoute != nil {
			cleanupErr = errors.Join(cleanupErr, preparedRoute.Discard())
			preparedRoute = nil
		}
		if loopWorkLease != nil {
			cleanupErr = errors.Join(cleanupErr, loopWorkLease.Done())
			loopWorkLease = nil
		}
		return cleanupErr
	}
	loopCtx, token, done, err := am.lifecycle.replaceLoopLocked(
		parent,
		strings.TrimSpace(agentID),
		trigger,
		operationID,
		rec,
		subordinate,
		topology,
		cell,
		proposedToken,
	)
	if err != nil {
		return replaceExecutionResult{}, errors.Join(err, cleanupPrepared())
	}
	if token == current.Token && loopCtx == nil && done == nil {
		if err := cleanupPrepared(); err != nil {
			return replaceExecutionResult{}, err
		}
		return replaceExecutionResult{previous: current.Config, config: current.Config}, nil
	}
	if loopCtx != nil && token != proposedToken {
		transitionErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "prepared_execution_token_mismatch", "agent-lifecycle", trigger, map[string]any{"agent_id": strings.TrimSpace(agentID)})
		abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
		return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
	}

	am.lifecycle.mu.Lock()
	successor := cell.execution
	if successor == nil || successor.token != token {
		am.lifecycle.mu.Unlock()
		transitionErr := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": strings.TrimSpace(agentID)})
		abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
		return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
	}
	successor.agent = candidate.Agent
	successor.config = candidate.Config
	successor.subscriptions = append([]events.EventType(nil), candidate.Subscriptions...)
	successor.admission = candidateAdmission
	successor.startedAt = candidate.StartedAt
	successor.standingOwner = candidate.StandingOwner
	am.lifecycle.mu.Unlock()

	if loopCtx != nil {
		if loopWorkLease == nil || preparedRoute == nil {
			transitionErr := errors.New("running agent transition has no pre-admitted work and route authority")
			abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
			return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
		}
		if loopWorkLease.Context().Err() != nil {
			transitionErr := fmt.Errorf("agent execution owner retired before publication: %w", loopWorkLease.Context().Err())
			abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
			return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
		}
		loopWorkOwner, loopWorkOwnerFound = worklifetime.OccurrenceFromContext(loopWorkLease.Context())
		if !loopWorkOwnerFound {
			transitionErr := errors.New("agent execution loop work omitted its exact occurrence")
			abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
			return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
		}
		if err := preparedRoute.Publish(); err != nil {
			abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
			return replaceExecutionResult{}, errors.Join(fmt.Errorf("publish generation-owned agent route: %w", err), abortErr, cleanupPrepared())
		}
		if loopWorkLease.Context().Err() != nil {
			transitionErr := fmt.Errorf("agent execution owner retired during route publication: %w", loopWorkLease.Context().Err())
			abortErr := am.lifecycle.abortUnlaunchedLoopLocked(parent, identity, token, done, cell)
			return replaceExecutionResult{}, errors.Join(transitionErr, abortErr, cleanupPrepared())
		}
		ch := preparedRoute.Deliveries()
		am.lifecycle.mu.Lock()
		if cell.execution == successor && successor.token == token {
			successor.route = ch
			successor.routeToken = token
		}
		am.lifecycle.mu.Unlock()
		preparedRoute = nil
		am.launchExecutionLoop(parent, successor, loopCtx, done, loopWorkLease, loopWorkOwner)
		loopWorkLease = nil
	}
	return replaceExecutionResult{previous: current.Config, config: candidate.Config, transitioned: true}, nil
}

func (am *AgentManager) launchExecutionLoop(parent context.Context, execution *agentExecutionProjection, loopCtx context.Context, done chan struct{}, workLease *worklifetime.Lease, executionOwner worklifetime.Occurrence) {
	agent := execution.agent
	ch := execution.route
	token := execution.token
	eventWorkOwner := executionOwner
	if execution.standingOwner != nil {
		eventWorkOwner = execution.standingOwner
	}
	_ = am.projectLifecycleDiagnostics(context.WithoutCancel(parent))
	go func() {
		executionCtx, cancelExecution := context.WithCancel(workLease.Context())
		stopGenerationCancel := context.AfterFunc(loopCtx, cancelExecution)
		defer func() {
			stopGenerationCancel()
			cancelExecution()
			if execution.loopSettled != nil {
				defer close(execution.loopSettled)
			}
			if releaseErr := am.lifecycle.releaseLoop(token, done); releaseErr != nil && am.bus != nil {
				failure := runtimefailures.FromError(releaseErr, "agent-manager", "release_agent_loop")
				_ = am.bus.LogRuntime(context.Background(), runtimepipeline.RuntimeLogEntry{
					Level: "error", Component: "agent-manager", Action: "agent_loop_release_failed",
					AgentID: agent.ID(), Failure: &failure.Failure,
				})
			}
			_ = am.projectLifecycleDiagnostics(context.Background())
			if settleErr := workLease.Done(); settleErr != nil {
				diaglog.ProcessLog(diaglog.LevelError, "agent-manager", "agent execution loop work settlement failed",
					"agent_id", agent.ID(),
					"error", settleErr.Error(),
				)
			}
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
					case <-executionCtx.Done():
						return
					case delivery, ok := <-ch:
						if !ok {
							return
						}
						evt := delivery.Event()
						stop := func() (stop bool) {
							carrier, carrierErr := worklifetime.NewEventDeliveryCarrierGuard(delivery)
							if carrierErr != nil {
								return true
							}
							reportCarrierFailure := func(err error) {
								if am.bus != nil {
									am.bus.LogRuntime(loopCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_carrier_transfer_failed",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(err, "agent-manager", "settle_delivery_carrier"),
									})
									return
								}
								diaglog.ProcessLog(diaglog.LevelError, "agent-manager", "delivery carrier transfer failed",
									"agent_id", agent.ID(), "event_id", evt.ID(), "error", err.Error())
							}
							defer func() {
								if _, completionErr := carrier.Complete(reportCarrierFailure); completionErr != nil {
									stop = true
								}
							}()
							if am.shutdownAdmissionClosed() {
								return true
							}
							eventWork, err := am.lifecycle.beginWork(delivery.Context(), eventWorkOwner)
							if err != nil {
								return true
							}
							defer func() { _ = eventWork.Done() }()
							// The carrier owns this item's queue-to-completion lifetime. The
							// receiver generation still owns execution authority and must
							// retain its lifecycle token and effect controller.
							deliveryOwner, ok := worklifetime.OccurrenceFromContext(eventWork.Context())
							if !ok {
								if am.bus != nil {
									am.bus.LogRuntime(loopCtx, runtimepipeline.RuntimeLogEntry{
										Level:     "error",
										Component: "agent-manager",
										Action:    "delivery_work_owner_missing",
										EventID:   strings.TrimSpace(evt.ID()),
										EventType: strings.TrimSpace(string(evt.Type())),
										AgentID:   agent.ID(),
									})
								}
								return true
							}
							route := delivery.HandoffRoute()
							evtCtx, closeEvtCtx, contextErr := agentDeliveryExecutionContext(
								eventWork.Context(), loopCtx, token, deliveryOwner, evt, route, am.receiverExecution,
								am.bus.AdmitBundleSourceFact,
							)
							if contextErr != nil {
								if am.bus != nil {
									am.bus.LogRuntime(loopCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_receiver_context_rejected",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(contextErr, "agent-manager", "construct_delivery_receiver_context"),
									})
								}
								return true
							}
							defer closeEvtCtx()
							if am.deliveryStore == nil {
								if am.bus != nil {
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_lifecycle_owner_missing",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
									})
								}
								return true
							}
							if err := am.executionPosture.Admit(evt.ExecutionMode(), "agent delivery claim"); err != nil {
								if am.bus != nil {
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_posture_rejected",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(err, "agent-manager", "admit_delivery_execution_posture"),
									})
								}
								return true
							}
							releaseLane, laneErr := am.acquireClaimedAttemptLane(evtCtx, route.AgentIdentity)
							if laneErr != nil {
								if am.bus != nil {
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_executor_admission_failed",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(laneErr, "agent-manager", "acquire_claimed_attempt_lane"),
									})
								}
								return true
							}
							defer releaseLane()
							authorityProvider := am.roles.DeliveryRuntime
							if authorityProvider == nil {
								return true
							}
							authority, authorityErr := authorityProvider.DeliveryAuthority()
							if authorityErr != nil {
								return true
							}
							claimResult, claimErr := am.deliveryStore.ClaimDelivery(evtCtx, authority, evt, route)
							if claimErr != nil {
								if am.bus != nil {
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_claim_failed",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(claimErr, "agent-manager", "claim_delivery"),
									})
								}
								return false
							}
							switch claimResult.Disposition {
							case runtimedelivery.ClaimDeferred, runtimedelivery.ClaimBusy:
								return false
							case runtimedelivery.ClaimTerminal:
								if _, completionErr := carrier.Complete(reportCarrierFailure); completionErr != nil {
									return true
								}
								releaser := am.roles.DeliveryRuntime
								if releaser == nil || releaser.ReleaseDeliveryContinuation(claimResult.Snapshot.DeliveryID) != nil {
									return true
								}
								return false
							case runtimedelivery.ClaimWrongAuthority, runtimedelivery.ClaimAbsent, runtimedelivery.ClaimInvariantInvalid:
								if am.bus != nil {
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level: "error", Component: "agent-manager", Action: "delivery_claim_invalid",
										EventID: strings.TrimSpace(evt.ID()), EventType: strings.TrimSpace(string(evt.Type())), AgentID: agent.ID(),
										Failure: failureEnvelope(claimResult.Invariant, "agent-manager", "claim_delivery"),
									})
								}
								return true
							case runtimedelivery.ClaimAcquired:
							default:
								return true
							}
							claimed, acquired := claimResult.Acquired()
							if !acquired {
								return true
							}
							resolution, consumeErr := carrier.Consume(reportCarrierFailure)
							if consumeErr != nil {
								return true
							}
							if resolution == worklifetime.DeliveryContinuationTerminal {
								releaser := am.roles.DeliveryRuntime
								if releaser == nil || releaser.ReleaseDeliveryContinuation(claimed.Claim.DeliveryID()) != nil {
									return true
								}
								return false
							}
							evtCtx = runtimedelivery.WithClaim(evtCtx, claimed.Claim)
							err, evtPanicked, evtPanicText, evtStackTrace := am.safeProcessEventOwned(evtCtx, agent, evt)
							if evtPanicked {
								panicCount, panicCountErr := am.incrementPoisonPanicCount(route.AgentIdentity, evt.ID())
								if panicCountErr != nil {
									panicCount = poisonPanicQuarantineAt
								}
								panicFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_event_panic", "agent-manager", "process_event", map[string]any{
									"agent_id": agent.ID(), "event_id": evt.ID(), "event_type": evt.Type(),
								}), "agent-manager", "process_event")
								_, settlementErr := am.writeReceipt(evtCtx, evt, ReceiptStatusError, &panicFailure.Failure)
								if am.bus != nil {
									detail := map[string]any{"stack_trace": evtStackTrace}
									if settlementErr != nil {
										detail["settlement_error"] = settlementErr.Error()
									}
									am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
										Level:     "error",
										Component: "agent-manager",
										Action:    "agent_event_panic",
										EventID:   strings.TrimSpace(evt.ID()),
										EventType: strings.TrimSpace(string(evt.Type())),
										AgentID:   agent.ID(),
										EntityID:  strings.TrimSpace(evt.EntityID()),
										Detail:    detail,
										Failure:   &panicFailure.Failure,
									})
								}
								if panicCount >= poisonPanicQuarantineAt {
									am.quarantinePoisonEvent(evtCtx, agent.ID(), evt, panicCount, panicFailure.Failure)
									am.clearPoisonPanicCount(route.AgentIdentity, evt.ID())
									consecutivePanics = 0
									return false
								}
								panicked = true
								panicCtx = evtCtx
								panicText = evtPanicText
								stackTrace = evtStackTrace
								lastEventType = strings.TrimSpace(string(evt.Type()))
								return true
							}
							am.clearPoisonPanicCount(route.AgentIdentity, evt.ID())
							consecutivePanics = 0
							if err != nil && am.bus != nil {
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
							return false
						}()
						if stop {
							return
						}
					}
				}
			}()
			if !panicked {
				return
			}
			consecutivePanics++
			am.handleAgentLoopPanic(panicCtx, token.Identity, agent, consecutivePanics, lastEventType, panicText, stackTrace)
			if consecutivePanics >= 5 {
				return
			}
			wait := panicBackoff(consecutivePanics)
			select {
			case <-executionCtx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
}

func agentDeliveryExecutionContext(
	deliveryCtx, loopCtx context.Context,
	token runtimeeffects.LifecycleToken,
	deliveryOwner worklifetime.Occurrence,
	evt events.Event,
	route events.DeliveryRoute,
	variant eventreceiver.ExecutionVariant,
	admitBundleSourceFact func(context.Context) (context.Context, error),
) (context.Context, func(), error) {
	runtimeInstanceID, _ := runtimecorrelation.RuntimeInstanceIDFromContext(deliveryCtx)
	bundleSourceFact, _ := runtimecorrelation.BundleSourceFactFromContext(deliveryCtx)
	ctx, cleanup := eventreceiver.NewContext(deliveryCtx)
	fail := func(err error) (context.Context, func(), error) {
		cleanup()
		return nil, nil, err
	}
	if admitBundleSourceFact == nil {
		return fail(errors.New("agent receiver requires the EventBus bundle source owner"))
	}
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, bundleSourceFact)
	var err error
	ctx, err = admitBundleSourceFact(ctx)
	if err != nil {
		return fail(fmt.Errorf("admit agent receiver bundle source: %w", err))
	}
	ctx = worklifetime.WithOccurrence(ctx, deliveryOwner)
	ctx = runtimeeffects.WithLifecycleToken(ctx, token)
	ctx, err = variant.Bind(ctx, evt.ExecutionMode())
	if err != nil {
		return fail(err)
	}
	if err := variant.ValidateBound(ctx, evt.ExecutionMode()); err != nil {
		return fail(err)
	}
	if variant.Kind() == eventreceiver.ExecutionNormal {
		if controller, found := runtimeeffects.ControllerFromContext(loopCtx); found {
			ctx = runtimeeffects.WithController(ctx, controller)
		}
		if admission, found := managedexecution.FromContext(loopCtx); found {
			ctx = managedexecution.WithAdmission(ctx, admission)
		}
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	deliveryContext := route.Context.Normalized()
	if deliveryContext.Empty() {
		deliveryContext = evt.DeliveryContext()
	}
	ctx = events.WithDeliveryContext(ctx, deliveryContext)
	ctx = runtimedelivery.WithRoute(ctx, route)
	return ctx, cleanup, nil
}

func (am *AgentManager) beginWork(ctx context.Context, kind string) (*worklifetime.Lease, error) {
	if am == nil || am.workOwner == nil {
		return nil, fmt.Errorf("%s requires a runtime work occurrence", strings.TrimSpace(kind))
	}
	var companion worklifetime.Occurrence
	if contextualOwner, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		if contextualOwner != am.workOwner {
			companion = contextualOwner
		}
	}
	lease, err := am.lifecycle.beginWork(ctx, companion)
	if err != nil {
		return nil, fmt.Errorf("admit %s: %w", strings.TrimSpace(kind), err)
	}
	return lease, nil
}

func (am *AgentManager) beginStandingWork(ctx context.Context, kind string) (*worklifetime.Lease, error) {
	if am == nil || am.workOwner == nil {
		return nil, fmt.Errorf("%s requires a runtime work occurrence", strings.TrimSpace(kind))
	}
	lease, err := am.lifecycle.beginStandingWork(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit %s: %w", strings.TrimSpace(kind), err)
	}
	return lease, nil
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

func (am *AgentManager) handleAgentLoopPanic(ctx context.Context, identity runtimeagentidentity.Identity, agent Agent, consecutivePanics int, lastEventType, panicText, stackTrace string) {
	panicText = strings.TrimSpace(panicText)
	if panicText == "" {
		panicText = "unknown panic"
	}

	entityID := ""
	flowInstance := ""
	execution, ok := am.lifecycle.executionSnapshotByIdentity(identity)
	cfg := runtimeactors.AgentConfig{}
	if ok {
		cfg = execution.Config
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
	panicEvent, constructErr := newPlatformContextualRuntimeDiagnosticEvent(eventCtx, am.executionPosture, events.EventType("platform.agent_panic"), mustJSON(map[string]any{
		"agent_id":        agent.ID(),
		"flow_instance":   flowInstance,
		"entity_id":       entityID,
		"failure":         panicFailure.Failure,
		"stack_trace":     stackTrace,
		"conversation_id": "",
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
	}), events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, time.Now().UTC())
	if constructErr == nil {
		constructErr = am.bus.Publish(eventCtx, panicEvent)
	}
	if constructErr != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_panic_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Failure:   failureEnvelope(constructErr, "agent-manager", "publish_agent_panic"),
			})
		}
	}

	if consecutivePanics < 5 {
		return
	}

	if ok {
		if _, err := am.lifecycle.terminateIdentityWithTopology(context.WithoutCancel(ctx), identity, "agent_loop_panic_threshold", AgentLifecycleFailed, nil); err != nil && am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "persist_agent_failure_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Failure:   failureEnvelope(err, "agent-manager", "persist_agent_failure"),
			})
		}
	}

	failedEvent, constructErr := newPlatformContextualRuntimeDiagnosticEvent(eventCtx, am.executionPosture, events.EventType("platform.agent_failed"), mustJSON(map[string]any{
		"agent_id":        agent.ID(),
		"flow_instance":   flowInstance,
		"entity_id":       entityID,
		"failure":         panicFailure.Failure,
		"retry_count":     consecutivePanics,
		"last_event_type": strings.TrimSpace(lastEventType),
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
	}), events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance}, time.Now().UTC())
	if constructErr == nil {
		constructErr = am.bus.Publish(eventCtx, failedEvent)
	}
	if constructErr != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_failed_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Failure:   failureEnvelope(constructErr, "agent-manager", "publish_agent_failed"),
			})
		}
	}
}
