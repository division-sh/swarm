package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type pipelineEmitCollectorKey struct{}
type pipelineEmitIntentCollectorKey struct{}
type PipelineCoordinator struct {
	bus Bus
	db  *sql.DB

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	module                 WorkflowModule
	workflowStore          *WorkflowInstanceStore
	expressionEval         *workflowExpressionEvaluator
	instanceActivator      FlowInstanceActivator
	instanceDeactivator    FlowInstanceDeactivator
	timerScheduler         *Scheduler
	timerScheduleStore     SchedulePersistence
	workflowTimers         *WorkflowTimerLifecycle
	mailboxMaterializer    MailboxWriteMaterializationStore
	decisionCards          decisioncard.Store
	credentials            runtimecredentials.Store
	managedCredentials     runtimemanagedcredentials.Store
	mockConnectorResponses *providerconnectors.MockResponsePlan
	channelActivityTools   map[string]ChannelActivityTarget
	artifactRoot           string
	bundleHash             string
	decisionCardCadence    decisioncard.CadencePolicy

	testEntityStateHook              func(entityID, state string)
	testWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	testLifecycleProbe               runtimelifecycleprobe.Observer
	testEngineEmitNow                func() time.Time
	workOwner                        worklifetime.Occurrence
	nodeRecoveryReady                chan struct{}
	nodeRecoveryReadyOnce            sync.Once
	testMaintenanceInterval          time.Duration
}

type WorkflowNodeHandlerStartHook func(context.Context, string, events.Event) error

type PipelineCoordinatorOptions struct {
	ShardPlanner                     any
	Module                           WorkflowModule
	WorkflowStore                    *WorkflowInstanceStore
	DeliveryStore                    runtimedelivery.Store
	PipelineObligations              runtimepipelineobligation.Store
	InstanceActivator                FlowInstanceActivator
	InstanceDeactivator              FlowInstanceDeactivator
	TimerScheduler                   *Scheduler
	TimerScheduleStore               SchedulePersistence
	MailboxMaterializer              MailboxWriteMaterializationStore
	DecisionCards                    decisioncard.Store
	Credentials                      runtimecredentials.Store
	ManagedCredentials               runtimemanagedcredentials.Store
	MockConnectorResponses           *providerconnectors.MockResponsePlan
	ChannelActivityTools             map[string]ChannelActivityTarget
	ArtifactRoot                     string
	BundleHash                       string
	DecisionCardCadence              decisioncard.CadencePolicy
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestEngineEmitNow                func() time.Time
	WorkOwner                        worklifetime.Occurrence
}

// ChannelActivityTarget is a private compiled connector target. Generation is
// persisted with every request and must match before any provider call.
type ChannelActivityTarget struct {
	Tool           runtimecontracts.ToolSchemaEntry
	PlanGeneration string
}

type runtimeMutationRunnerProvider interface {
	RuntimeMutationRunner() RuntimeMutationRunner
}

type pipelineObligationOwnerProvider interface {
	PipelineObligationOwner() runtimepipelineobligation.Store
}

func copyActivityToolEntries(in map[string]ChannelActivityTarget) map[string]ChannelActivityTarget {
	out := make(map[string]ChannelActivityTarget, len(in))
	for name, target := range in {
		out[name] = target
	}
	return out
}

func NewPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return newPipelineCoordinatorWithOptions(bus, db, opts, true)
}

func newPreviewPipelineCoordinator(bus Bus, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return newPipelineCoordinatorWithOptions(bus, nil, opts, false)
}

func newPipelineCoordinatorWithOptions(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions, requireObligationOwner bool) *PipelineCoordinator {
	if bus == nil {
		return nil
	}
	module := opts.Module
	if module == nil {
		panic("pipeline: workflow module is required")
	}
	workflowStore := opts.WorkflowStore
	if workflowStore == nil {
		workflowStore = NewWorkflowInstanceStore(db)
	}
	if provider, ok := bus.(runtimeMutationRunnerProvider); ok {
		workflowStore.ConfigureRuntimeMutationRunner(provider.RuntimeMutationRunner())
	}
	if opts.PipelineObligations == nil {
		if provider, ok := bus.(pipelineObligationOwnerProvider); ok {
			opts.PipelineObligations = provider.PipelineObligationOwner()
		}
	}
	if opts.DeliveryStore != nil {
		workflowStore.ConfigureDeliveryLifecycleStore(opts.DeliveryStore)
	}
	if requireObligationOwner && opts.PipelineObligations == nil {
		panic("pipeline: durable pipeline obligation owner is required")
	}
	if opts.PipelineObligations != nil {
		workflowStore.ConfigurePipelineObligationStore(opts.PipelineObligations)
	}
	if publisher, ok := bus.(workflowGateMutationPublisher); ok {
		workflowStore.ConfigureDecisionCardLifecycle(opts.DecisionCards, publisher)
	} else {
		workflowStore.ConfigureDecisionCardLifecycle(opts.DecisionCards)
	}
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	coordinator := &PipelineCoordinator{
		bus:                              bus,
		db:                               db,
		module:                           module,
		workflowStore:                    workflowStore,
		expressionEval:                   newWorkflowExpressionEvaluator(),
		instanceActivator:                opts.InstanceActivator,
		instanceDeactivator:              opts.InstanceDeactivator,
		timerScheduler:                   opts.TimerScheduler,
		timerScheduleStore:               opts.TimerScheduleStore,
		mailboxMaterializer:              opts.MailboxMaterializer,
		decisionCards:                    opts.DecisionCards,
		credentials:                      credentials,
		managedCredentials:               opts.ManagedCredentials,
		mockConnectorResponses:           opts.MockConnectorResponses,
		channelActivityTools:             copyActivityToolEntries(opts.ChannelActivityTools),
		artifactRoot:                     strings.TrimSpace(opts.ArtifactRoot),
		bundleHash:                       strings.TrimSpace(opts.BundleHash),
		decisionCardCadence:              opts.DecisionCardCadence.Normalize(),
		testEntityStateHook:              opts.TestEntityStateHook,
		testWorkflowNodeHandlerStartHook: opts.TestWorkflowNodeHandlerStartHook,
		testLifecycleProbe:               opts.TestLifecycleProbe,
		testEngineEmitNow:                opts.TestEngineEmitNow,
		workOwner:                        opts.WorkOwner,
		nodeRecoveryReady:                make(chan struct{}),
		entityLocks:                      make(map[string]*sync.Mutex),
	}
	coordinator.workflowTimers = newWorkflowTimerLifecycle(coordinator)
	return coordinator
}

func (pc *PipelineCoordinator) SetTestMaintenanceInterval(interval time.Duration) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testMaintenanceInterval = interval
	pc.mu.Unlock()
}

func NewPipelineCoordinator(bus Bus, db *sql.DB) *PipelineCoordinator {
	panic("pipeline: workflow module is required")
}

func (pc *PipelineCoordinator) SetTestEntityStateHook(fn func(entityID, state string)) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testEntityStateHook = fn
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) SetTestWorkflowNodeHandlerStartHook(fn WorkflowNodeHandlerStartHook) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testWorkflowNodeHandlerStartHook = fn
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) SetTestLifecycleProbe(probe runtimelifecycleprobe.Observer) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	pc.testLifecycleProbe = probe
	pc.mu.Unlock()
}

// RecoverNodeDeliveries performs the required startup pass, then authorizes
// the standing maintenance loop to recover obligations as they become eligible.
func (pc *PipelineCoordinator) RecoverNodeDeliveries(ctx context.Context) error {
	if pc == nil || pc.workflowStore == nil {
		return nil
	}
	if err := pc.recoverNodeDeliveriesOnce(ctx); err != nil {
		return err
	}
	pc.nodeRecoveryReadyOnce.Do(func() { close(pc.nodeRecoveryReady) })
	return nil
}

func (pc *PipelineCoordinator) recoverNodeDeliveriesOnce(ctx context.Context) error {
	owner := pc.workflowStore.DeliveryLifecycleStore()
	if owner == nil {
		return fmt.Errorf("workflow node delivery lifecycle owner is required")
	}
	for _, node := range pc.WorkflowNodes() {
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" || strings.TrimSpace(node.ExecutionType) != runtimecontracts.SystemNodeExecutionType {
			continue
		}
		for {
			executions, err := owner.ClaimNodeBacklog(ctx, nodeID, 1)
			if err != nil {
				return fmt.Errorf("claim pending deliveries for node %s: %w", nodeID, err)
			}
			for _, execution := range executions {
				executionCtx := withWorkflowNodeDeliveryRoute(ctx, execution.Snapshot.Route)
				executionCtx = runtimedelivery.WithClaim(executionCtx, execution.Claim)
				if _, err := pc.executeNodeHandlerPlanResult(executionCtx, nodeID, execution.Event); err != nil {
					return fmt.Errorf("recover delivery %s for node %s: %w", execution.Snapshot.DeliveryID, nodeID, err)
				}
			}
			if len(executions) == 0 {
				break
			}
		}
	}
	return nil
}

func (pc *PipelineCoordinator) RunMaintenance(ctx context.Context) {
	draftExpiry, hasDraftExpiry := pc.decisionCards.(interface {
		ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error)
	})
	humanTaskExpiry, hasHumanTaskExpiry := pc.decisionCards.(interface {
		ExpireHumanTaskCardsInMutation(context.Context, time.Time, int) ([]events.Event, error)
	})
	if (!hasDraftExpiry || draftExpiry == nil) && (!hasHumanTaskExpiry || humanTaskExpiry == nil) && pc.workflowStore == nil {
		return
	}
	run := func() {
		now := time.Now().UTC()
		if hasDraftExpiry && draftExpiry != nil {
			if _, err := draftExpiry.ExpireDecisionCardInputDrafts(ctx, now); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_decision_card_input_drafts", "", "", runtimeWorkflowID, "", nil, err)
			}
		}
		if hasHumanTaskExpiry && humanTaskExpiry != nil {
			if err := pc.expireHumanTaskCards(ctx, humanTaskExpiry, now, 200); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_human_task_cards", "", "", runtimeWorkflowID, "", nil, err)
			}
		}
		select {
		case <-pc.nodeRecoveryReady:
			if err := pc.recoverNodeDeliveriesOnce(ctx); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "recover_node_deliveries", "", "", runtimeWorkflowID, "", nil, err)
			}
		default:
		}
	}
	run()
	interval := time.Minute
	pc.mu.Lock()
	if pc.testMaintenanceInterval > 0 {
		interval = pc.testMaintenanceInterval
	}
	pc.mu.Unlock()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (pc *PipelineCoordinator) expireHumanTaskCards(ctx context.Context, expiry interface {
	ExpireHumanTaskCardsInMutation(context.Context, time.Time, int) ([]events.Event, error)
}, now time.Time, limit int) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return errors.New("human-task expiry requires the selected workflow store")
	}
	publisher, ok := pc.bus.(workflowGateMutationPublisher)
	if !ok || publisher == nil {
		return errors.New("human-task expiry requires transactional event publication")
	}
	return pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		expiredEvents, err := expiry.ExpireHumanTaskCardsInMutation(txctx, now, limit)
		if err != nil {
			return err
		}
		for _, evt := range expiredEvents {
			if err := publisher.PublishInMutation(txctx, evt); err != nil {
				return fmt.Errorf("publish human-task expiry event: %w", err)
			}
		}
		return nil
	})
}

func (pc *PipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return pc.intercept(ctx, evt, false)
}

func (pc *PipelineCoordinator) intercept(ctx context.Context, evt events.Event, exactDeliveryBoundary bool) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if pc == nil {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	eventType := strings.TrimSpace(string(evt.Type()))
	if eventType == "" {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	if evt.Type() == workflowGateDecisionEventType {
		emitted, outcome, err := pc.handleWorkflowGateDecisionEvent(ctx, evt)
		return false, emitted, outcome, err
	}
	if evt.Type() == decisionCardDeferredEventType {
		emitted, err := pc.handleDecisionCardDeferredEvent(ctx, evt)
		return false, emitted, runtimepipelineobligation.Continue(), err
	}
	if evt.Type() == decisionCardExpiredEventType {
		emitted, err := pc.handleDecisionCardExpiredEvent(ctx, evt)
		return false, emitted, runtimepipelineobligation.Continue(), err
	}
	stageTimer, firedStageTimer, err := pc.handleWorkflowStageTimerFire(ctx, evt)
	if err != nil {
		return false, nil, runtimepipelineobligation.Continue(), err
	}
	if stageTimer && (!firedStageTimer || eventType == runtimecontracts.WorkflowStageTimerInternalEvent) {
		return false, nil, runtimepipelineobligation.Continue(), nil
	}
	consume, handled, err := pc.interceptPolicy(ctx, eventType, evt)
	if err != nil {
		return false, nil, runtimepipelineobligation.Continue(), err
	}
	if !handled {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	emitted := make([]events.Event, 0, 4)
	ictx := context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)
	handled, outcome, err := pc.handleEventResult(ictx, evt)
	if !outcome.ContinueDispatch() {
		// A non-consuming event-wide policy has no node-delivery authority.
		// Only an exact node route or a consuming platform policy may settle
		// the event's pipeline obligation.
		if !exactDeliveryBoundary && !consume {
			return true, emitted, runtimepipelineobligation.Continue(), nil
		}
		return false, emitted, outcome, nil
	}
	if evt.Type() == activityRequestEventType && err != nil {
		return false, emitted, runtimepipelineobligation.Continue(), err
	}
	if err != nil {
		if exactDeliveryBoundary {
			return false, emitted, runtimepipelineobligation.Continue(), err
		}
		if consume {
			return false, emitted, outcome, nil
		}
		return true, emitted, outcome, nil
	}
	if !handled {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return !consume, emitted, outcome, nil
}

func (pc *PipelineCoordinator) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	evt := delivery.Event()
	route = route.Normalized()
	if route.SubscriberType != "node" || route.SubscriberID == "" {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	if route.Target.Normalized() != evt.TargetRoute().Normalized() {
		return true, nil, runtimepipelineobligation.Continue(), fmt.Errorf("workflow node delivery route target mismatch for %s: route=%#v event=%#v", route.SubscriberID, route.Target.Normalized(), evt.TargetRoute().Normalized())
	}
	ctx = runtimedelivery.WithoutClaim(ctx)
	return pc.intercept(withWorkflowNodeDeliveryRoute(ctx, route), evt, true)
}

func (pc *PipelineCoordinator) interceptPolicy(ctx context.Context, eventType string, evt events.Event) (consume bool, handled bool, err error) {
	if strings.TrimSpace(eventType) == "" {
		return false, false, nil
	}
	if evt.Type() == activityRequestEventType {
		return true, true, nil
	}
	return pc.workflowNodeInterceptPolicy(ctx, eventType, evt)
}

func (pc *PipelineCoordinator) handleEvent(ctx context.Context, evt events.Event) bool {
	handled, _, _ := pc.handleEventResult(ctx, evt)
	return handled
}

func (pc *PipelineCoordinator) handleEventResult(ctx context.Context, evt events.Event) (bool, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() == activityRequestEventType {
		return pc.handleActivityRequestEvent(ctx, evt)
	}
	handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
	if err == nil {
		return handled, runtimepipelineobligation.Continue(), nil
	}
	failure := runtimefailures.Normalize(err, runtimeWorkflowID, "execute_handler")
	return handled, runtimepipelineobligation.DeadLetterExecution("handler_terminal_failure", &failure), nil
}

func (pc *PipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, nodeID string, evt events.Event) bool {
	handled, _ := pc.executeNodeHandlerPlanResult(ctx, nodeID, evt)
	return handled
}

func (pc *PipelineCoordinator) executeNodeHandlerPlanResult(ctx context.Context, nodeID string, evt events.Event) (bool, error) {
	if pc == nil {
		return false, nil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false, nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return false, nil
	}
	trigger := strings.TrimSpace(string(evt.Type()))
	if trigger == "" {
		return false, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, nodeID, evt)
	if resolved.Failure != "" {
		return false, fmt.Errorf("resolve workflow handler for node %s: %s", nodeID, resolved.Failure)
	}
	handler := resolved.Handler
	handlerEventKey := resolved.HandlerEventKey
	ok := resolved.Matched
	if !ok && isJoinLifecycleEvent(events.EventType(trigger)) {
		if ref, _, refOK := timeridentity.ParseJoinRef(parsePayloadMap(evt.Payload())); refOK && ref.NodeID == nodeID {
			handler, ok = findJoinHandlerForRef(source, ref)
			handlerEventKey = ref.HandlerEvent
		}
	}
	if !ok {
		return false, nil
	}
	deliveryStore := pc.workflowStore.DeliveryLifecycleStore()
	if deliveryStore == nil {
		return false, fmt.Errorf("workflow node delivery lifecycle owner is required")
	}
	route, routeOK := runtimedelivery.RouteFromContext(ctx)
	if !routeOK || route.SubscriberType != string(runtimedelivery.SubscriberNode) || strings.TrimSpace(route.SubscriberID) != nodeID {
		return false, fmt.Errorf("workflow node %s requires its exact admitted delivery route", nodeID)
	}
	claim, claimed := runtimedelivery.ClaimFromContext(ctx)
	if claimed && (claim.SubscriberClass() != runtimedelivery.SubscriberNode || claim.SubscriberID() != nodeID) {
		return false, fmt.Errorf("workflow node %s received a claim for %s/%s", nodeID, claim.SubscriberClass(), claim.SubscriberID())
	}
	recoveryClaim := claimed
	for {
		if !claimed {
			owned, err := deliveryStore.ClaimNodeDelivery(ctx, evt, route)
			if errors.Is(err, runtimedelivery.ErrIneligible) {
				return true, nil
			}
			if err != nil {
				return false, fmt.Errorf("claim workflow node delivery: %w", err)
			}
			claim = owned.Claim
		}
		attemptCtx := runtimedelivery.WithClaim(ctx, claim)
		pc.notifyTestLifecycleDeliveryStatus(attemptCtx, nodeID, evt, string(runtimedelivery.StatusInProgress))
		attemptCtx = withPipelineFlowScope(attemptCtx, workflowNodeFlowID(source, nodeID))
		if err := pc.notifyTestWorkflowNodeHandlerStarting(attemptCtx, nodeID, evt); err != nil {
			return false, err
		}
		pc.notifyTestLifecycleHandlerStarted(attemptCtx, nodeID, evt)
		started := time.Now()
		heartbeat, heartbeatErr := runtimedelivery.StartClaimHeartbeat(attemptCtx, pc.workOwner, deliveryStore, claim)
		if heartbeatErr != nil {
			return false, fmt.Errorf("renew workflow node delivery claim: %w", heartbeatErr)
		}
		executionCtx := heartbeat.Context()
		result, err := pc.executeNodeContractHandler(executionCtx, nodeID, handler, workflowTriggerContext{
			Event:           evt,
			HandlerEventKey: handlerEventKey,
			State:           pc.currentWorkflowState(executionCtx, workflowEventEntityID(evt)),
		}, false)
		if err == nil {
			pc.notifyTestLifecycleHandlerCompleted(executionCtx, nodeID, evt, "completed")
			pc.recordInterceptedEmitDeadLetters(executionCtx, evt, nodeID, result.Outcome)
			sideEffects := []string{"handler_completed"}
			settlementGuard, settleErr := heartbeat.BeginSettlement()
			if settleErr != nil {
				_ = heartbeat.Stop()
				return false, fmt.Errorf("prepare workflow node delivery settlement: %w", settleErr)
			}
			_, settleErr = deliveryStore.SettleSuccess(executionCtx, claim, sideEffects, time.Since(started))
			finishErr := settlementGuard.Finish(settleErr == nil)
			if err := errors.Join(settleErr, finishErr); err != nil {
				return false, fmt.Errorf("settle workflow node delivery: %w", err)
			}
			pc.convergeWorkflowNodeNormalRunCompletion(attemptCtx, nodeID, evt)
			pc.notifyTestLifecycleDeliveryStatus(attemptCtx, nodeID, evt, "delivered")
			return result.Handled, nil
		}
		pc.notifyTestLifecycleHandlerCompleted(executionCtx, nodeID, evt, "failed")
		failure := runtimefailures.FromError(err, runtimeWorkflowID, "execute_handler")
		disposition := runtimedelivery.FailureRetry
		reason := "handler_failure"
		if errors.Is(err, runtimeengine.ErrChainDepthExceeded) || runtimeengine.FailureDispositionFor(failure) != runtimeengine.FailureDispositionRetry {
			disposition = runtimedelivery.FailureDeadLetter
			reason = "handler_terminal_failure"
			if errors.Is(err, runtimeengine.ErrChainDepthExceeded) {
				reason = "chain_depth_exceeded"
			}
		}
		settlementGuard, settleErr := heartbeat.BeginSettlement()
		if settleErr != nil {
			_ = heartbeat.Stop()
			return false, fmt.Errorf("prepare failed workflow node delivery settlement: %w", settleErr)
		}
		snapshot, settleErr := deliveryStore.SettleFailure(executionCtx, claim, runtimedelivery.Settlement{
			Disposition: disposition, ReasonCode: reason, Failure: &failure.Failure,
			Duration: time.Since(started), RetryBase: semanticview.HandlerRetryBase(source),
		})
		finishErr := settlementGuard.Finish(settleErr == nil)
		if err := errors.Join(settleErr, finishErr); err != nil {
			return false, fmt.Errorf("settle failed workflow node delivery: %w", err)
		}
		pc.notifyTestLifecycleDeliveryStatus(attemptCtx, nodeID, evt, string(snapshot.Status))
		if snapshot.Status == runtimedelivery.StatusDeadLetter {
			pc.recordWorkflowHandlerFailure(attemptCtx, evt, nodeID, err)
			pc.convergeWorkflowNodeNormalRunCompletion(attemptCtx, nodeID, evt)
			if recoveryClaim {
				// The recovered handler failure is now durable terminal evidence.
				// Only claim or settlement failures make readiness unsafe.
				return true, nil
			}
			return true, err
		}
		if err := waitForWorkflowNodeRetry(ctx, snapshot.NextEligibleAt); err != nil {
			return true, err
		}
		claimed = false
	}
}

func waitForWorkflowNodeRetry(ctx context.Context, nextEligibleAt time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}
	wait := time.Until(nextEligibleAt)
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (pc *PipelineCoordinator) recordWorkflowHandlerFailure(ctx context.Context, evt events.Event, nodeID string, err error) {
	if pc == nil || err == nil {
		return
	}
	failure := runtimefailures.FromError(err, runtimeWorkflowID, "execute_handler")
	if pc.bus != nil {
		pc.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   "Workflow handler execution failed",
			Component: runtimeWorkflowID,
			Action:    "handler_error",
			EventID:   strings.TrimSpace(evt.ID()),
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
			Detail: map[string]any{
				"node_id": nodeID,
				"error":   err.Error(),
			},
			Failure: &failure.Failure,
		})
	}
}

func (pc *PipelineCoordinator) recordInterceptedEmitDeadLetters(ctx context.Context, trigger events.Event, nodeID string, outcome *handlerExecutionOutcome) {
	if pc == nil || outcome == nil || len(outcome.InterceptedEmits) == 0 {
		return
	}
	entityID := workflowEventEntityID(trigger)
	nodeID = strings.TrimSpace(nodeID)
	for _, intercepted := range outcome.InterceptedEmits {
		if strings.TrimSpace(intercepted.DeadLetterHint) != "chain_depth_exceeded" {
			continue
		}
		eventType := strings.TrimSpace(string(intercepted.Event.Type()))
		failure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", runtimeWorkflowID, "emit", map[string]any{
			"event_type": eventType, "chain_depth": intercepted.ChainDepth,
		}), runtimeWorkflowID, "emit")
		rec := runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(trigger.ID()),
			OriginalEvent:   eventType,
			OriginalPayload: intercepted.Event.Payload(),
			EntityID:        entityID,
			FlowInstance:    "runtime",
			Failure:         failure.Failure,
			ChainDepth:      intercepted.ChainDepth,
			HandlerNode:     firstNonEmptyString(nodeID+":"+eventType, nodeID),
		}
		if pc.db != nil {
			if err := recordPipelineDeadLetter(ctx, pc.db, rec); err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_persist_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			}
		}
		deadLetterPayload := map[string]any{
			"original_event":   eventType,
			"original_payload": json.RawMessage(intercepted.Event.Payload()),
			"entity_id":        entityID,
			"flow_instance":    "runtime",
			"failure":          failure.Failure,
			"retry_count":      0,
			"chain_depth":      intercepted.ChainDepth,
			"handler_node":     nodeID,
			"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
		}
		if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
			emitted, err := newPipelineRuntimeDiagnostic(events.LineageFromEvent(trigger), "platform.dead_letter", entityID, "", deadLetterPayload)
			if err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_construct_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, nil, err)
				continue
			}
			*collector = append(*collector, emitted)
			continue
		}
		publishDeadLetter := func(actionCtx context.Context) {
			if err := pc.publish(actionCtx, "platform.dead_letter", entityID, deadLetterPayload); err != nil {
				pc.logRuntimeWarn(actionCtx, "workflow-runtime", "intercepted_emit_dead_letter_publish_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, publishDeadLetter) {
			publishDeadLetter(ctx)
		}
	}
}

func (pc *PipelineCoordinator) logRuntimeWarn(ctx context.Context, component, action, eventID, eventType, agentID, entityID string, detail any, err error) {
	if pc != nil && pc.bus != nil {
		pc.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "warn",
			Message:   "Workflow runtime warning was recorded",
			Component: strings.TrimSpace(component),
			Action:    strings.TrimSpace(action),
			EventID:   strings.TrimSpace(eventID),
			EventType: strings.TrimSpace(eventType),
			AgentID:   strings.TrimSpace(agentID),
			EntityID:  strings.TrimSpace(entityID),
			Detail:    detail,
			Failure:   pipelineRuntimeFailure(err, strings.TrimSpace(component), strings.TrimSpace(action)),
		})
		return
	}
	processWarn(component, "%s", strings.TrimSpace(errText(err)))
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (pc *PipelineCoordinator) notifyTestEntityStateUpdated(entityID, state string) {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	hook := pc.testEntityStateHook
	pc.mu.Unlock()
	if hook != nil {
		hook(strings.TrimSpace(entityID), strings.TrimSpace(state))
	}
}

func (pc *PipelineCoordinator) notifyTestWorkflowNodeHandlerStarting(ctx context.Context, nodeID string, evt events.Event) error {
	if pc == nil {
		return nil
	}
	pc.mu.Lock()
	hook := pc.testWorkflowNodeHandlerStartHook
	pc.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, strings.TrimSpace(nodeID), evt)
}

func (pc *PipelineCoordinator) publish(ctx context.Context, eventType, entityID string, payload map[string]any) error {
	if pc == nil {
		return nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("pipeline runtime diagnostic %q requires an inbound event", strings.TrimSpace(eventType))
	}
	emitted, err := newPipelineRuntimeDiagnostic(events.LineageFromEvent(inbound), eventType, entityID, flowInstance, payload)
	if err != nil {
		return err
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return nil
	}
	if pc.bus != nil {
		if err := pc.bus.Publish(ctx, emitted); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) publishDirect(ctx context.Context, eventType, entityID string, payload map[string]any, recipients []string) error {
	if pc == nil {
		return nil
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return pc.publish(ctx, eventType, entityID, payload)
	}
	flowInstance := strings.Trim(strings.TrimSpace(asString(payload["flow_instance"])), "/")
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("pipeline direct runtime diagnostic %q requires an inbound event", strings.TrimSpace(eventType))
	}
	emitted, err := newPipelineRuntimeDiagnostic(events.LineageFromEvent(inbound), eventType, entityID, flowInstance, payload)
	if err != nil {
		return err
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, emitted)
		return nil
	}
	if pc.bus != nil {
		if err := pc.bus.PublishDirect(ctx, emitted, recipients); err != nil {
			return err
		}
	}
	return nil
}

func newPipelineRuntimeDiagnostic(lineage events.EventLineage, eventType, entityID, flowInstance string, payload map[string]any) (events.Event, error) {
	return events.NewCausalRuntimeDiagnosticEvent(events.CausalRuntimeEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: events.EventType(strings.TrimSpace(eventType)),
		Producer:  events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeWorkflowID},
		Payload:   mustJSON(payload),
		Envelope:  events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance},
		CreatedAt: time.Now().UTC(), ExecutionMode: lineage.ExecutionMode,
	}, Lineage: lineage})
}
