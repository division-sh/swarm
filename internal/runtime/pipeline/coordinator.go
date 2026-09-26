package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

type RunBundleAvailabilityReader interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type ScenarioExecutionProfileReader interface {
	LoadScenarioExecutionProfile(context.Context, string) (scenarioexecution.Profile, bool, error)
}

type PipelineCoordinator struct {
	previewState *runtimeengine.StateSnapshot
	bus          Bus

	mu sync.Mutex

	entityLockMu sync.Mutex
	entityLocks  map[string]*sync.Mutex

	module                       WorkflowModule
	workflowStore                *workflowInstanceStore
	expressionEval               *workflowExpressionEvaluator
	instanceDeactivationPreparer FlowInstanceDeactivationPreparer
	timerScheduler               *Scheduler
	genericSchedules             GenericScheduleWakeupOwner
	workflowTimers               *WorkflowTimerLifecycle
	timerCancellations           *runtimetimercancellation.Reconciler
	decisionCards                decisioncard.Store
	proposedEffects              decisioncard.ProposedEffectStore
	humanTasks                   decisioncard.HumanTaskStore
	decisionDraftExpiry          DecisionCardDraftExpiry
	humanTaskExpiry              HumanTaskExpiry
	deliveryStore                runtimedelivery.Store
	deadLetters                  runtimedeadletters.AcknowledgedRecorder
	deliveryRuntime              WorkflowDeliveryRuntime
	flowRoutes                   FlowInstanceRouteOwner
	credentials                  runtimecredentials.Store
	managedCredentials           runtimemanagedcredentials.Store
	mockConnectorResponses       *providerconnectors.MockResponsePlan
	scenarioProfiles             ScenarioExecutionProfileReader
	effectiveSource              scenarioexecution.EffectiveSourceIdentity
	channelActivations           *runtimechannelactivation.Owner
	sourceArtifactFact           runtimecorrelation.SourceArtifactFact
	runBundleAvailability        RunBundleAvailabilityReader
	decisionCardCadence          decisioncard.CadencePolicy
	executionPosture             executionposture.Posture

	testEntityStateHook              func(entityID, state string)
	testWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	testLifecycleProbe               runtimelifecycleprobe.Observer
	testEngineEmitNow                func() time.Time
	workOwner                        worklifetime.Occurrence
	receiverExecution                eventreceiver.ExecutionVariant
	runtimeReceiver                  bool
	testMaintenanceInterval          time.Duration
	fanOutOwnerID                    string
	fanOutNotifier                   FanOutWorkNotifier
}

type WorkflowNodeHandlerStartHook func(context.Context, string, events.Event) error

type PipelineCoordinatorOptions struct {
	ExecutionPosture                 executionposture.Posture
	ShardPlanner                     any
	Module                           WorkflowModule
	Persistence                      WorkflowPersistence
	DeliveryStore                    runtimedelivery.Store
	DeadLetters                      runtimedeadletters.AcknowledgedRecorder
	PipelineObligations              runtimepipelineobligation.Store
	InstanceDeactivationPreparer     FlowInstanceDeactivationPreparer
	TimerScheduler                   *Scheduler
	GenericSchedules                 GenericScheduleWakeupOwner
	TimerObligationReader            runtimetimerobligation.Reader
	DecisionCards                    decisioncard.Store
	ProposedEffects                  decisioncard.ProposedEffectStore
	HumanTasks                       decisioncard.HumanTaskStore
	DecisionCardDraftExpiry          DecisionCardDraftExpiry
	HumanTaskExpiry                  HumanTaskExpiry
	DeliveryRuntime                  WorkflowDeliveryRuntime
	FlowRoutes                       FlowInstanceRouteOwner
	RunLifecycle                     runtimerunlifecycle.OperationOwner
	Credentials                      runtimecredentials.Store
	ManagedCredentials               runtimemanagedcredentials.Store
	MockConnectorResponses           *providerconnectors.MockResponsePlan
	ScenarioExecutionProfiles        ScenarioExecutionProfileReader
	EffectiveSourceIdentity          scenarioexecution.EffectiveSourceIdentity
	ChannelActivations               *runtimechannelactivation.Owner
	SourceArtifactFact               runtimecorrelation.SourceArtifactFact
	RunBundleAvailability            RunBundleAvailabilityReader
	DecisionCardCadence              decisioncard.CadencePolicy
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestEngineEmitNow                func() time.Time
	WorkOwner                        worklifetime.Occurrence
	ReceiverExecution                eventreceiver.ExecutionVariant
}

type ChannelActivityTarget struct {
	value *channelActivityTargetValue
}

type channelActivityTargetValue struct {
	tool           runtimecontracts.ToolSchemaEntry
	generation     plangeneration.Generation
	credentialKeys map[string]string
}

func NewChannelActivityTarget(tool runtimecontracts.ToolSchemaEntry, generation plangeneration.Generation) (ChannelActivityTarget, error) {
	return NewChannelActivityTargetWithCredentials(tool, generation, nil)
}

func NewChannelActivityTargetWithCredentials(tool runtimecontracts.ToolSchemaEntry, generation plangeneration.Generation, credentialKeys map[string]string) (ChannelActivityTarget, error) {
	if tool.IsZero() {
		return ChannelActivityTarget{}, fmt.Errorf("private channel activity tool is missing")
	}
	if !generation.Valid() {
		return ChannelActivityTarget{}, fmt.Errorf("private channel activity plan generation is missing")
	}
	keys := make(map[string]string, len(credentialKeys))
	for logical, rawKey := range credentialKeys {
		keys[strings.TrimSpace(logical)] = strings.TrimSpace(rawKey)
	}
	return ChannelActivityTarget{value: &channelActivityTargetValue{tool: tool, generation: generation, credentialKeys: keys}}, nil
}

func (t ChannelActivityTarget) Tool() (runtimecontracts.ToolSchemaEntry, bool) {
	if t.value == nil {
		return runtimecontracts.ToolSchemaEntry{}, false
	}
	return t.value.tool, true
}

func (t ChannelActivityTarget) Generation() plangeneration.Generation {
	if t.value == nil {
		return plangeneration.Generation{}
	}
	return t.value.generation
}

func (pc *PipelineCoordinator) channelActivityTarget(ctx context.Context, toolID string, generation channelonboarding.ChannelActivationGeneration) (ChannelActivityTarget, bool, *runtimechannelactivation.Lease, error) {
	if pc == nil {
		return ChannelActivityTarget{}, false, nil, nil
	}
	if pc.channelActivations == nil {
		return ChannelActivityTarget{}, false, nil, nil
	}
	var lease *runtimechannelactivation.Lease
	var ok bool
	if admitted, inherited := runtimechannelactivation.ExecutionLeaseFromContext(ctx); inherited {
		lease, ok = pc.channelActivations.BorrowActivityOperation(admitted, toolID, generation)
	} else {
		lease, ok = pc.channelActivations.AcquireActivityOperation(toolID, generation)
	}
	if !ok {
		return ChannelActivityTarget{}, false, nil, nil
	}
	operation := lease.Operation()
	identity, err := operation.Binding.RuntimeActivityTarget(operation.Name)
	if err != nil {
		lease.Release()
		return ChannelActivityTarget{}, false, nil, err
	}
	tool, err := operation.Binding.OperationTool(operation.Name)
	if err != nil {
		lease.Release()
		return ChannelActivityTarget{}, false, nil, err
	}
	target, err := NewChannelActivityTargetWithCredentials(tool, identity.Generation(), identity.CredentialStoreKeys())
	if err != nil {
		lease.Release()
		return ChannelActivityTarget{}, false, nil, err
	}
	return target, true, lease, nil
}

func (t ChannelActivityTarget) CredentialStoreKey(logical string) (string, bool) {
	if t.value == nil {
		return "", false
	}
	value, ok := t.value.credentialKeys[strings.TrimSpace(logical)]
	return value, ok
}

type DecisionCardDraftExpiry interface {
	ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error)
}

type WorkflowDeliveryRuntime interface {
	DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error)
	AcquireDeliveryContinuation(string) (worklifetime.DeliveryAcquisition, error)
	ReleaseDeliveryContinuation(string) error
	RetainDeliveryContinuation(runtimedelivery.Snapshot) error
}

func NewPipelineCoordinatorWithOptions(bus Bus, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return newPipelineCoordinatorWithOptions(bus, opts, true)
}

func newPreviewPipelineCoordinator(bus Bus, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	return newPipelineCoordinatorWithOptions(bus, opts, false)
}

func newPipelineCoordinatorWithOptions(bus Bus, opts PipelineCoordinatorOptions, requireObligationOwner bool) *PipelineCoordinator {
	if bus == nil {
		return nil
	}
	if requireObligationOwner {
		if err := opts.ReceiverExecution.Validate(); err != nil {
			return nil
		}
		if !opts.ExecutionPosture.Valid() {
			return nil
		}
	}
	module := opts.Module
	if module == nil {
		panic("pipeline: workflow module is required")
	}
	storeTemplate := opts.Persistence.store
	if requireObligationOwner {
		if !opts.Persistence.Valid() {
			return nil
		}
		if opts.DeliveryStore == nil || opts.DeadLetters == nil || opts.PipelineObligations == nil || opts.DecisionCards == nil ||
			opts.ProposedEffects == nil || opts.HumanTasks == nil ||
			opts.DecisionCardDraftExpiry == nil || opts.HumanTaskExpiry == nil ||
			opts.DeliveryRuntime == nil || opts.RunLifecycle == nil {
			return nil
		}
	}
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	coordinator := &PipelineCoordinator{
		bus:                              bus,
		module:                           module,
		expressionEval:                   newWorkflowExpressionEvaluator(),
		instanceDeactivationPreparer:     opts.InstanceDeactivationPreparer,
		timerScheduler:                   opts.TimerScheduler,
		genericSchedules:                 opts.GenericSchedules,
		decisionCards:                    opts.DecisionCards,
		proposedEffects:                  opts.ProposedEffects,
		humanTasks:                       opts.HumanTasks,
		decisionDraftExpiry:              opts.DecisionCardDraftExpiry,
		humanTaskExpiry:                  opts.HumanTaskExpiry,
		deliveryStore:                    opts.DeliveryStore,
		deadLetters:                      opts.DeadLetters,
		deliveryRuntime:                  opts.DeliveryRuntime,
		flowRoutes:                       opts.FlowRoutes,
		credentials:                      credentials,
		managedCredentials:               opts.ManagedCredentials,
		mockConnectorResponses:           opts.MockConnectorResponses,
		scenarioProfiles:                 opts.ScenarioExecutionProfiles,
		effectiveSource:                  opts.EffectiveSourceIdentity,
		channelActivations:               opts.ChannelActivations,
		sourceArtifactFact:               opts.SourceArtifactFact,
		runBundleAvailability:            opts.RunBundleAvailability,
		decisionCardCadence:              opts.DecisionCardCadence.Normalize(),
		executionPosture:                 opts.ExecutionPosture,
		testEntityStateHook:              opts.TestEntityStateHook,
		testWorkflowNodeHandlerStartHook: opts.TestWorkflowNodeHandlerStartHook,
		testLifecycleProbe:               opts.TestLifecycleProbe,
		testEngineEmitNow:                opts.TestEngineEmitNow,
		workOwner:                        opts.WorkOwner,
		receiverExecution:                opts.ReceiverExecution,
		runtimeReceiver:                  requireObligationOwner,
		entityLocks:                      make(map[string]*sync.Mutex),
		fanOutOwnerID:                    uuid.NewString(),
	}
	var workflowStore *workflowInstanceStore
	if storeTemplate != nil {
		workflowStore = &workflowInstanceStore{
			entityQuery:            storeTemplate.entityQuery,
			routeRecovery:          storeTemplate.routeRecovery,
			activityResults:        storeTemplate.activityResults,
			activityJournal:        storeTemplate.activityJournal,
			gateRoutes:             storeTemplate.gateRoutes,
			timerObligations:       storeTemplate.timerObligations,
			engineMutations:        storeTemplate.engineMutations,
			fanOutObligations:      storeTemplate.fanOutObligations,
			cardMutations:          storeTemplate.cardMutations,
			timerOccurrences:       storeTemplate.timerOccurrences,
			timerActivations:       storeTemplate.timerActivations,
			readiness:              storeTemplate.readiness,
			standingServices:       storeTemplate.standingServices,
			decisionRoutes:         storeTemplate.decisionRoutes,
			instanceReader:         storeTemplate.instanceReader,
			entityStateReader:      storeTemplate.entityStateReader,
			entityCollectionReader: storeTemplate.entityCollectionReader,
			targetReader:           storeTemplate.targetReader,
			initialCommits:         storeTemplate.initialCommits,
			deliveryStore:          opts.DeliveryStore,
			pipelineStore:          opts.PipelineObligations,
			decisionCards:          opts.DecisionCards,
			lifecycleOwner:         pipelineWorkflowLifecycleOwner{coordinator: coordinator},
			runLifecycle:           opts.RunLifecycle,
			deliverySignals:        make(map[runtimedelivery.ExecutionAuthority]func()),
		}
	}
	coordinator.workflowStore = workflowStore
	if workflowStore != nil {
		coordinator.workflowTimers = newWorkflowTimerLifecycle(workflowStore, coordinator.SemanticSource(), bus, opts.WorkOwner, opts.TimerScheduler, opts.ExecutionPosture)
	}
	coordinator.timerCancellations = runtimetimercancellation.NewReconciler(coordinator.genericSchedules, coordinator.workflowTimers)
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

// FinalizeSelectedReceiverAdmission installs provider-preflight evidence before
// the selected Pipeline begins receiving EventBus work.
func (pc *PipelineCoordinator) FinalizeSelectedReceiverAdmission(admission managedexecution.Admission) error {
	if pc == nil {
		return errors.New("pipeline coordinator is required")
	}
	variant, err := pc.receiverExecution.WithSelectedAdmission(admission)
	if err != nil {
		return fmt.Errorf("finalize selected pipeline receiver admission: %w", err)
	}
	pc.receiverExecution = variant
	return nil
}

func NewPipelineCoordinator(bus Bus) *PipelineCoordinator {
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

func (pc *PipelineCoordinator) RunMaintenance(ctx context.Context) {
	draftExpiry := pc.decisionDraftExpiry
	humanTaskExpiry := pc.humanTaskExpiry
	if draftExpiry == nil && humanTaskExpiry == nil {
		return
	}
	run := func() {
		if pc.workOwner == nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "admit_pipeline_maintenance", "", "", runtimeWorkflowID, "", nil, errors.New("pipeline maintenance requires its runtime work owner"))
			return
		}
		lease, err := pc.workOwner.Begin(ctx)
		if err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "admit_pipeline_maintenance", "", "", runtimeWorkflowID, "", nil, err)
			return
		}
		defer lease.Done()
		ctx := lease.Context()
		now := time.Now().UTC()
		if draftExpiry != nil {
			if _, err := draftExpiry.ExpireDecisionCardInputDrafts(ctx, now); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_decision_card_input_drafts", "", "", runtimeWorkflowID, "", nil, err)
			}
		}
		if humanTaskExpiry != nil {
			if err := pc.expireHumanTaskCards(ctx, humanTaskExpiry, now, 200); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "expire_human_task_cards", "", "", runtimeWorkflowID, "", nil, err)
			}
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

func (pc *PipelineCoordinator) signalFanOutWork() {
	if pc == nil {
		return
	}
	pc.mu.Lock()
	notifier := pc.fanOutNotifier
	pc.mu.Unlock()
	if notifier != nil {
		notifier.Wake()
	}
}

type FanOutWorkNotifier interface{ Wake() }

func (pc *PipelineCoordinator) InstallFanOutWorkNotifier(notifier FanOutWorkNotifier) {
	pc.mu.Lock()
	pc.fanOutNotifier = notifier
	pc.mu.Unlock()
}

func (pc *PipelineCoordinator) expireHumanTaskCards(ctx context.Context, expiry HumanTaskExpiry, now time.Time, limit int) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return errors.New("human-task expiry requires the selected workflow store")
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return errors.New("human-task expiry requires the publication planner")
	}
	expiredEvents, err := expiry.ListDueHumanTaskExpiryEvents(ctx, now, limit)
	if err != nil || len(expiredEvents) == 0 {
		return err
	}
	intents := make([]runtimeengine.EmitIntent, 0, len(expiredEvents))
	for _, event := range expiredEvents {
		intents = append(intents, runtimeengine.EmitIntent{Event: event})
	}
	plans, err := planner.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return err
	}
	if len(plans) != len(intents) {
		releaseErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
		return errors.Join(fmt.Errorf("human-task expiry planner returned %d plans for %d events", len(plans), len(intents)), releaseErr)
	}
	committed, err := expiry.CommitHumanTaskExpirations(ctx, HumanTaskExpiryCommand{ObservedAt: now, Limit: limit, Publications: plans})
	if !committed.Acknowledged {
		if err == nil {
			err = errors.New("human-task expiry was not acknowledged")
		}
		return errors.Join(err, planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	if validateErr := committed.Validate(); validateErr != nil {
		return errors.Join(err, validateErr)
	}
	err = errors.Join(err, planner.FinalizeEnginePublications(ctx, committed.Publications))
	dispatcher := pc.bus.EngineDispatcher()
	if dispatcher == nil {
		return errors.Join(err, errors.New("human-task expiry requires post-commit dispatcher"))
	}
	committedIntents := make([]runtimeengine.EmitIntent, 0, len(committed.Publications))
	for _, publication := range committed.Publications {
		committedIntents = append(committedIntents, publication.CommittedDurablePublicationIntent())
	}
	return errors.Join(err, dispatcher.DispatchPostCommit(context.WithoutCancel(ctx), committedIntents))
}

func (pc *PipelineCoordinator) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	return pc.intercept(ctx, evt, false)
}

func (pc *PipelineCoordinator) intercept(ctx context.Context, evt events.Event, exactDeliveryBoundary bool) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if pc == nil {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	if pc.runtimeReceiver {
		if err := pc.receiverExecution.ValidateBound(ctx, evt.ExecutionMode()); err != nil {
			return false, nil, runtimepipelineobligation.Continue(), fmt.Errorf("pipeline receiver execution: %w", err)
		}
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
		emitted, outcome, err := pc.handleDecisionCardDeferredEvent(ctx, evt)
		return false, emitted, outcome, err
	}
	if evt.Type() == decisionCardExpiredEventType {
		emitted, outcome, err := pc.handleDecisionCardExpiredEvent(ctx, evt)
		return false, emitted, outcome, err
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
	emissions := &pipelineEmissionPlan{}
	handled, outcome, err := pc.handleEventResultWithEmissionPlan(ctx, evt, emissions)
	emitted := emissions.immutableEvents()
	if outcome.Committed {
		return !consume && !exactDeliveryBoundary, emitted, outcome, err
	}
	if !outcome.ContinueDispatch() {
		// A non-consuming event-wide policy has no node-delivery authority.
		// Only an exact node route or a consuming platform policy may settle
		// the event's pipeline obligation.
		if !exactDeliveryBoundary && !consume {
			return true, emitted, runtimepipelineobligation.Continue(), nil
		}
		return false, emitted, outcome, err
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
	if exactDeliveryBoundary {
		return false, emitted, outcome, nil
	}
	return !consume, emitted, outcome, nil
}

func (pc *PipelineCoordinator) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	evt := delivery.Event()
	route = route.Normalized()
	if !route.Recipient.IsNode() {
		return true, nil, runtimepipelineobligation.Continue(), nil
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

func (pc *PipelineCoordinator) handleEventResult(ctx context.Context, evt events.Event) (bool, runtimepipelineobligation.ExecutionOutcome, error) {
	return pc.handleEventResultWithEmissionPlan(ctx, evt, nil)
}

func (pc *PipelineCoordinator) handleEventResultWithEmissionPlan(ctx context.Context, evt events.Event, emissions *pipelineEmissionPlan) (bool, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() == activityRequestEventType {
		return pc.handleActivityRequestEventWithEmissionPlan(ctx, evt, emissions)
	}
	handled, committed, err := pc.dispatchWorkflowNodeEventResultWithEmissionPlan(ctx, evt, emissions)
	if committed {
		return handled, runtimepipelineobligation.ExecutionOutcome{Committed: true}, err
	}
	if err == nil {
		return handled, runtimepipelineobligation.Continue(), nil
	}
	failure := runtimefailures.Normalize(err, runtimeWorkflowID, "execute_handler")
	return handled, runtimepipelineobligation.DeadLetterExecution("handler_terminal_failure", &failure), nil
}

func (pc *PipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, node identity.ExecutableNode, evt events.Event) bool {
	handled, _ := pc.executeNodeHandlerPlanResult(ctx, node, evt)
	return handled
}

func (pc *PipelineCoordinator) executeNodeHandlerPlanResult(ctx context.Context, node identity.ExecutableNode, evt events.Event) (bool, error) {
	handled, _, err := pc.executeNodeHandlerPlanResultWithEmissionPlan(ctx, node, evt, nil)
	return handled, err
}

func (pc *PipelineCoordinator) executeNodeHandlerPlanResultWithEmissionPlan(ctx context.Context, node identity.ExecutableNode, evt events.Event, emissions *pipelineEmissionPlan) (handled, committed bool, resultErr error) {
	var probeErr error
	defer func() { resultErr = errors.Join(resultErr, probeErr) }()
	if pc == nil {
		return false, false, nil
	}
	if !node.Valid() {
		return false, false, nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return false, false, nil
	}
	trigger := strings.TrimSpace(string(evt.Type()))
	if trigger == "" {
		return false, false, nil
	}
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, node, evt)
	if resolved.Failure != "" {
		return false, false, fmt.Errorf("resolve workflow handler for node %s: %s", node.Key(), resolved.Failure)
	}
	if !resolved.Matched {
		return false, false, nil
	}
	handler := resolved.Handler
	handlerEventKey := resolved.HandlerEventKey
	deliveryStore := pc.deliveryStore
	if deliveryStore == nil {
		return false, false, fmt.Errorf("workflow node delivery lifecycle owner is required")
	}
	route, routeOK := runtimedelivery.RouteFromContext(ctx)
	recipientNode, recipientOK := route.Recipient.Node()
	if !routeOK || !recipientOK || !recipientNode.Equal(node) {
		return false, false, fmt.Errorf("workflow node %s requires its exact admitted delivery route", node.Key())
	}
	nodeFlowID := strings.TrimSpace(resolved.FlowID)
	if nodeFlowID == "" {
		nodeFlowID = node.FlowPath()
	}
	handlerFact, err := NewDeliveryTargetHandler(node)
	if err != nil {
		return false, false, fmt.Errorf("workflow node %s target handler: %w", node.Key(), err)
	}
	handlerFact = handlerFact.ForEvent(events.EventType(handlerEventKey))
	claim, claimed := runtimedelivery.ClaimFromContext(ctx)
	if claimed && (claim.SubscriberClass() != runtimedelivery.SubscriberNode || claim.SubscriberID() != node.Key()) {
		return false, false, fmt.Errorf("workflow node %s received a claim for %s/%s", node.Key(), claim.SubscriberClass(), claim.SubscriberID())
	}
	recoveryClaim := claimed
	for {
		if !claimed {
			authorityProvider := pc.deliveryRuntime
			if authorityProvider == nil {
				return false, false, fmt.Errorf("workflow node delivery continuation authority is required")
			}
			reportCarrierFailure := func(err error) {
				diaglog.ProcessLog(diaglog.LevelError, "pipeline", "workflow node delivery carrier transfer failed",
					"node_id", node.Key(), "event_id", evt.ID(), "error", err.Error())
			}
			admission, err := admitWorkflowNodeDelivery(ctx, evt, route, authorityProvider, deliveryStore, reportCarrierFailure)
			if err != nil {
				return false, false, err
			}
			if admission.postCommitErr != nil {
				defer func() { resultErr = errors.Join(resultErr, admission.postCommitErr) }()
			}
			if admission.handled {
				return true, false, nil
			}
			claim = admission.claim
		}
		attemptCtx := runtimedelivery.WithClaim(ctx, claim)
		probeErr = errors.Join(probeErr, pc.notifyTestLifecycleDeliveryStatus(attemptCtx, node.Key(), evt, string(runtimedelivery.StatusInProgress)))
		attemptCtx = withPipelineFlowScope(attemptCtx, nodeFlowID)
		probeErr = errors.Join(probeErr, pc.notifyTestLifecycleHandlerStarted(attemptCtx, node.Key(), evt))
		started := time.Now()
		heartbeat, heartbeatErr := runtimedelivery.StartClaimHeartbeat(attemptCtx, pc.workOwner, deliveryStore, claim)
		if heartbeatErr != nil {
			return false, false, fmt.Errorf("renew workflow node delivery claim: %w", heartbeatErr)
		}
		defer func() { resultErr = errors.Join(resultErr, heartbeat.Stop()) }()
		executionCtx := heartbeat.Context()
		executionCtx = runtimecorrelation.WithInboundEvent(executionCtx, evt)
		// Preparation is part of the claimed attempt. Its failure must use the
		// same settlement and continuation handoff as a handler execution failure.
		result, err := func() (contractHandlerExecutionResult, error) {
			if err := pc.notifyTestWorkflowNodeHandlerStarting(executionCtx, node.Key(), evt); err != nil {
				return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
			}
			application, err := pc.prepareDeliveryTargetApplication(executionCtx, node.Key(), handlerFact, handler, evt, route.Target)
			if err != nil {
				return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
			}
			executionCtx = withDeliveryTargetApplication(executionCtx, application)
			return pc.executeNodeContractHandler(executionCtx, node, handler, workflowTriggerContext{
				Event:           application.Event(),
				HandlerEventKey: handlerEventKey,
				State:           application.State(),
			}, false, emissions != nil)
		}()
		if result.Committed {
			err = errors.Join(err, pc.transferCommittedHandlerFollowUp(executionCtx, result.FollowUp, emissions))
		}
		if emissions != nil {
			for _, diagnostic := range result.DiagnosticEmissions {
				emissions.appendEvent(diagnostic)
			}
		}
		handled, finishErr, finishProbeErr := pc.finishClaimedNodeAttempt(claimedNodeAttempt{
			ctx: executionCtx, statusCtx: attemptCtx, node: node, event: evt,
			claim: claim, heartbeat: heartbeat, started: started,
			result: result, executionErr: err,
			retryBase: semanticview.HandlerRetryBase(source), recoveryClaim: recoveryClaim,
		})
		probeErr = errors.Join(probeErr, finishProbeErr)
		return handled, result.Committed, finishErr
	}
}

type claimedNodeAttempt struct {
	ctx, statusCtx context.Context
	node           identity.ExecutableNode
	event          events.Event
	claim          runtimedelivery.Claim
	heartbeat      *runtimedelivery.ClaimHeartbeat
	started        time.Time
	result         contractHandlerExecutionResult
	executionErr   error
	retryBase      time.Duration
	recoveryClaim  bool
}

func (pc *PipelineCoordinator) transferCommittedHandlerFollowUp(ctx context.Context, followUp handlerCommittedFollowUp, deferred *pipelineEmissionPlan) error {
	immediate := make([]runtimeengine.ActivityIntent, 0, len(followUp.ActivityIntents))
	for _, activity := range followUp.ActivityIntents {
		activity = activity.Normalized()
		if activity.ApprovalDecision == "" {
			immediate = append(immediate, activity)
		}
	}
	var activityErr error
	if len(followUp.ActivityRequests) != len(immediate) {
		activityErr = fmt.Errorf("committed activity requests = %d, want %d immediate activities", len(followUp.ActivityRequests), len(immediate))
	} else {
		for index, activity := range immediate {
			request := followUp.ActivityRequests[index].Event
			if request.ID() != activityRequestEventID(activity) || request.Type() != activityRequestEventType || request.CreatedAt().IsZero() {
				activityErr = fmt.Errorf("immediate activity %s lacks its exact committed request event", activity.ActivityID)
				break
			}
		}
	}
	if deferred != nil {
		deferred.appendIntents(followUp.Emissions)
		if activityErr == nil {
			deferred.appendIntents(followUp.ActivityRequests)
		}
		return activityErr
	}
	if len(followUp.Emissions) == 0 && len(followUp.ActivityRequests) == 0 {
		return activityErr
	}
	if pc.bus == nil || pc.bus.EngineDispatcher() == nil {
		return errors.Join(activityErr, errors.New("committed handler follow-up requires post-commit dispatcher"))
	}
	dispatcher := pc.bus.EngineDispatcher()
	postCommitCtx := context.WithoutCancel(ctx)
	var emitErr, requestErr error
	if len(followUp.Emissions) > 0 {
		emitErr = dispatcher.DispatchPostCommit(postCommitCtx, followUp.Emissions)
	}
	if activityErr == nil && len(followUp.ActivityRequests) > 0 {
		requestErr = dispatcher.DispatchPostCommit(postCommitCtx, followUp.ActivityRequests)
	}
	return errors.Join(activityErr, emitErr, requestErr)
}

// finishClaimedNodeAttempt is the single outcome handoff after execution. A
// committed result is never converted into a retry because cleanup failed.
func (pc *PipelineCoordinator) finishClaimedNodeAttempt(a claimedNodeAttempt) (handled bool, resultErr, probeErr error) {
	defer func() { resultErr = errors.Join(resultErr, a.heartbeat.Stop()) }()
	if err := consumeHandlerSettlementEvidence(a.result, a.claim); err != nil {
		return false, errors.Join(a.executionErr, err), nil
	}
	committed := a.executionErr == nil || a.result.Committed || a.result.SettledDeliveryClaim != nil
	if committed {
		probeErr = pc.notifyTestLifecycleHandlerCompleted(a.ctx, a.node.Key(), a.event, "completed")
	} else {
		probeErr = pc.notifyTestLifecycleHandlerCompleted(a.ctx, a.node.Key(), a.event, "failed")
	}

	// The engine may already have settled the exact claim in its commit. All
	// other attempts settle through one guarded store handoff below.
	if a.result.SettledDeliveryClaim != nil {
		stopErr := a.heartbeat.Stop()
		if pc.deliveryRuntime == nil {
			return a.result.Handled, errors.Join(a.executionErr, stopErr, errors.New("terminal workflow node delivery continuation owner is required")), probeErr
		}
		if err := errors.Join(stopErr, pc.deliveryRuntime.ReleaseDeliveryContinuation(a.claim.DeliveryID())); err != nil {
			return a.result.Handled, errors.Join(a.executionErr, fmt.Errorf("finish settled workflow node delivery continuation: %w", err)), probeErr
		}
		probeErr = errors.Join(probeErr, pc.notifyTestLifecycleDeliveryStatus(a.statusCtx, a.node.Key(), a.event, "delivered"))
		return a.result.Handled, a.executionErr, probeErr
	}

	var selection handlerselection.HandlerRuleSelectionFact
	if committed {
		var err error
		selection, err = a.result.RuleSelection.ResolvedFact()
		if err != nil {
			return a.result.Handled, errors.Join(a.executionErr, err), probeErr
		}
	}
	guard, err := a.heartbeat.BeginSettlement()
	if err != nil {
		if committed {
			return a.result.Handled, errors.Join(a.executionErr, fmt.Errorf("prepare workflow node delivery settlement: %w", err)), probeErr
		}
		return false, errors.Join(a.executionErr, fmt.Errorf("prepare failed workflow node delivery settlement: %w", err)), probeErr
	}

	var snapshot runtimedelivery.Snapshot
	var settleErr error
	if committed {
		snapshot, settleErr = pc.deliveryStore.SettleSuccess(a.ctx, a.claim, []string{"handler_completed"}, time.Since(a.started), selection)
	} else {
		failure := runtimefailures.FromError(a.executionErr, runtimeWorkflowID, "execute_handler")
		disposition := runtimedelivery.FailureRetry
		reason := "handler_failure"
		if admission, ok := managedexecution.FromContext(a.ctx); ok && admission.Kind == managedexecution.KindSelectedContractFork {
			disposition = runtimedelivery.FailureDeadLetter
			reason = "terminal_failure"
		}
		if errors.Is(a.executionErr, runtimeengine.ErrChainDepthExceeded) || runtimeengine.FailureDispositionFor(failure) != runtimeengine.FailureDispositionRetry {
			disposition = runtimedelivery.FailureDeadLetter
			reason = "handler_terminal_failure"
			if errors.Is(a.executionErr, runtimeengine.ErrChainDepthExceeded) {
				reason = "chain_depth_exceeded"
			}
		}
		snapshot, settleErr = pc.deliveryStore.SettleFailure(a.ctx, a.claim, runtimedelivery.Settlement{
			Disposition: disposition, ReasonCode: reason, Failure: &failure.Failure,
			Duration: time.Since(a.started), RetryBase: a.retryBase,
			RuleSelection: a.result.RuleSelection,
		})
	}
	settled := snapshot.MatchesSettlementClaim(a.claim) &&
		((committed && snapshot.Status == runtimedelivery.StatusDelivered) ||
			(!committed && (snapshot.Status == runtimedelivery.StatusFailed || snapshot.Status == runtimedelivery.StatusDeadLetter)))
	if !settled && settleErr == nil {
		if committed {
			settleErr = errors.New("workflow node success settlement returned no exact acknowledged snapshot")
		} else {
			settleErr = errors.New("workflow node failure settlement returned no exact acknowledged snapshot")
		}
	}
	finishErr := guard.Finish(settled)
	var continuationErr error
	if settled {
		switch snapshot.Status {
		case runtimedelivery.StatusDelivered, runtimedelivery.StatusDeadLetter:
			if pc.deliveryRuntime == nil {
				continuationErr = errors.New("terminal workflow node delivery continuation owner is required")
			} else {
				continuationErr = pc.deliveryRuntime.ReleaseDeliveryContinuation(snapshot.DeliveryID)
			}
		case runtimedelivery.StatusFailed:
			if pc.deliveryRuntime == nil {
				continuationErr = errors.New("workflow node retry continuation owner is required")
			} else {
				continuationErr = pc.deliveryRuntime.RetainDeliveryContinuation(snapshot)
			}
		}
	}
	settlementErr := errors.Join(settleErr, finishErr, continuationErr)
	if !settled {
		if committed {
			return a.result.Handled, errors.Join(a.executionErr, fmt.Errorf("settle workflow node delivery: %w", settlementErr)), probeErr
		}
		return false, errors.Join(a.executionErr, fmt.Errorf("settle failed workflow node delivery: %w", settlementErr)), probeErr
	}
	if continuationErr != nil && snapshot.Status == runtimedelivery.StatusFailed {
		return false, errors.Join(settleErr, finishErr, fmt.Errorf("transfer workflow node retry continuation: %w", continuationErr)), probeErr
	}
	if committed && settlementErr != nil {
		return a.result.Handled, errors.Join(a.executionErr, fmt.Errorf("settle workflow node delivery: %w", settlementErr)), probeErr
	}
	probeErr = errors.Join(probeErr, pc.notifyTestLifecycleDeliveryStatus(a.statusCtx, a.node.Key(), a.event, string(snapshot.Status)))
	if committed {
		return a.result.Handled, a.executionErr, probeErr
	}
	if snapshot.Status == runtimedelivery.StatusDeadLetter {
		pc.recordWorkflowHandlerFailure(a.statusCtx, a.event, a.node.Key(), a.executionErr)
		if a.recoveryClaim {
			return true, settlementErr, probeErr
		}
		return true, errors.Join(a.executionErr, settlementErr), probeErr
	}
	return true, settlementErr, probeErr
}

func consumeHandlerSettlementEvidence(result contractHandlerExecutionResult, claim runtimedelivery.Claim) error {
	// A malformed post-commit result is still committed; never reclassify it as a retryable write.
	if result.SettledDeliveryClaim != nil && !result.SettledDeliveryClaim.Same(claim) {
		return fmt.Errorf("workflow node engine settled a different delivery claim")
	}
	if result.SettledDeliveryClaim != nil && !result.Committed {
		return fmt.Errorf("workflow node engine reported settlement without an acknowledged commit")
	}
	if result.Committed && result.SettledDeliveryClaim == nil {
		return fmt.Errorf("workflow node engine committed without exact delivery settlement evidence")
	}
	return nil
}

type workflowNodeDeliveryAuthority interface {
	DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error)
	AcquireDeliveryContinuation(string) (worklifetime.DeliveryAcquisition, error)
	ReleaseDeliveryContinuation(string) error
}

type workflowNodeDeliveryAdmission struct {
	claim         runtimedelivery.Claim
	handled       bool
	postCommitErr error
}

func admitWorkflowNodeDelivery(
	ctx context.Context,
	evt events.Event,
	route events.DeliveryRoute,
	authorityProvider workflowNodeDeliveryAuthority,
	deliveryStore runtimedelivery.Store,
	reportCarrierFailure func(error),
) (workflowNodeDeliveryAdmission, error) {
	authority, err := authorityProvider.DeliveryAuthority()
	if err != nil {
		return workflowNodeDeliveryAdmission{}, err
	}
	deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		return workflowNodeDeliveryAdmission{}, err
	}
	carrier, borrowed, err := worklifetime.DirectDeliveryCarrier(ctx, deliveryID)
	if err != nil {
		return workflowNodeDeliveryAdmission{}, err
	}
	if !borrowed {
		acquisition, err := authorityProvider.AcquireDeliveryContinuation(deliveryID)
		if err != nil {
			return workflowNodeDeliveryAdmission{}, err
		}
		if err := acquisition.Validate(deliveryID); err != nil {
			return workflowNodeDeliveryAdmission{}, err
		}
		continuation, acquired := acquisition.Acquired()
		if !acquired {
			return workflowNodeDeliveryAdmission{handled: true}, nil
		}
		carrier, err = worklifetime.NewDeliveryContinuationGuard(ctx, continuation)
		if err != nil {
			return workflowNodeDeliveryAdmission{}, err
		}
	}
	returnCarrier := func(primary error) error {
		_, completionErr := carrier.Complete(reportCarrierFailure)
		return errors.Join(primary, completionErr)
	}
	claimResult, claimErr := deliveryStore.ClaimDelivery(ctx, authority, evt, route)
	if !claimResult.Acknowledged {
		if claimErr == nil {
			claimErr = errors.New("delivery claim was not acknowledged")
		}
		return workflowNodeDeliveryAdmission{}, returnCarrier(fmt.Errorf("claim workflow node delivery: %w", claimErr))
	}
	postCommitErr := claimErr
	switch claimResult.Disposition {
	case runtimedelivery.ClaimDeferred, runtimedelivery.ClaimBusy:
		if err := returnCarrier(nil); err != nil {
			return workflowNodeDeliveryAdmission{}, errors.Join(postCommitErr, err)
		}
		return workflowNodeDeliveryAdmission{handled: true, postCommitErr: postCommitErr}, nil
	case runtimedelivery.ClaimTerminal:
		if err := returnCarrier(nil); err != nil {
			return workflowNodeDeliveryAdmission{}, errors.Join(postCommitErr, err)
		}
		if err := authorityProvider.ReleaseDeliveryContinuation(claimResult.Snapshot.DeliveryID); err != nil {
			return workflowNodeDeliveryAdmission{}, errors.Join(postCommitErr, err)
		}
		return workflowNodeDeliveryAdmission{handled: true, postCommitErr: postCommitErr}, nil
	case runtimedelivery.ClaimWrongAuthority, runtimedelivery.ClaimAbsent, runtimedelivery.ClaimInvariantInvalid:
		return workflowNodeDeliveryAdmission{}, returnCarrier(errors.Join(postCommitErr, fmt.Errorf("claim workflow node delivery disposition %s: %w", claimResult.Disposition, claimResult.Invariant)))
	case runtimedelivery.ClaimAcquired:
	default:
		return workflowNodeDeliveryAdmission{}, returnCarrier(errors.Join(postCommitErr, fmt.Errorf("claim workflow node delivery returned unknown disposition %q", claimResult.Disposition)))
	}
	owned, acquired := claimResult.Acquired()
	if !acquired {
		return workflowNodeDeliveryAdmission{}, returnCarrier(errors.Join(postCommitErr, errors.New("workflow node delivery acquired without an exact claim")))
	}
	resolution, consumeErr := carrier.Consume(reportCarrierFailure)
	if consumeErr != nil {
		return workflowNodeDeliveryAdmission{}, returnCarrier(errors.Join(postCommitErr, fmt.Errorf("consume workflow node delivery continuation: %w", consumeErr)))
	}
	if resolution == worklifetime.DeliveryContinuationTerminal {
		if err := authorityProvider.ReleaseDeliveryContinuation(owned.Claim.DeliveryID()); err != nil {
			return workflowNodeDeliveryAdmission{}, errors.Join(postCommitErr, err)
		}
		return workflowNodeDeliveryAdmission{handled: true, postCommitErr: postCommitErr}, nil
	}
	return workflowNodeDeliveryAdmission{claim: owned.Claim, postCommitErr: postCommitErr}, nil
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

func (pc *PipelineCoordinator) recordInterceptedEmitDeadLetters(ctx context.Context, trigger events.Event, nodeID string, outcome *handlerExecutionOutcome, emissions *pipelineEmissionPlan) {
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
			Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
		}
		if pc.deadLetters == nil {
			pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_persist_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
				"intercepted_event_type": eventType,
				"handler_node":           nodeID,
			}, errors.New("workflow intercepted-emission dead-letter recorder is required"))
		} else {
			committed, err := pc.deadLetters.RecordDeadLetterOutcome(ctx, rec)
			if !committed.Acknowledged {
				if err == nil {
					err = errors.New("intercepted-emission dead-letter record was not acknowledged")
				}
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_persist_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
					"intercepted_event_type": eventType,
					"handler_node":           nodeID,
				}, err)
			} else if err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_post_commit_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
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
		if emissions != nil {
			emitted, err := newPipelineRuntimeDiagnostic(events.LineageFromEvent(trigger), "platform.dead_letter", entityID, "", deadLetterPayload)
			if err != nil {
				pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_construct_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, nil, err)
				continue
			}
			emissions.appendEvent(emitted)
			continue
		}
		if err := pc.publish(ctx, "platform.dead_letter", entityID, deadLetterPayload); err != nil {
			pc.logRuntimeWarn(ctx, "workflow-runtime", "intercepted_emit_dead_letter_publish_failed", strings.TrimSpace(trigger.ID()), strings.TrimSpace(string(trigger.Type())), runtimeWorkflowID, entityID, map[string]any{
				"intercepted_event_type": eventType,
				"handler_node":           nodeID,
			}, err)
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
