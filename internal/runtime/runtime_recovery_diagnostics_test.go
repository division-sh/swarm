package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimefanout "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/testutil"
)

type startupRecoveryPipelineOwner struct {
	mu       sync.Mutex
	issuer   *runtimepipelineobligation.ClaimIssuer
	scanner  *runtimepipelineobligation.ScanIssuer
	work     []events.PersistedReplayEvent
	claimErr error
	claims   map[string]runtimepipelineobligation.Claim
	scans    map[string]runtimepipelineobligation.ScanRequest
}

type startupRecoveryWorkflowOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	timers runtimetimerobligation.Reader
}

func (o startupRecoveryWorkflowOwner) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if o.timers != nil {
		return o.timers.ReadTimerObligations(ctx, scope, observedAt)
	}
	return runtimetimerobligation.Snapshot{ObservedAt: observedAt.UTC()}, nil
}

func (startupRecoveryWorkflowOwner) InspectDynamicFlowRuntimeReadinessForSource(context.Context, runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, nil
}

func (startupRecoveryWorkflowOwner) InspectDynamicFlowRuntimeReadinessForRun(context.Context, string, runtimecorrelation.BundleSourceFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	return nil, nil
}

func (startupRecoveryWorkflowOwner) ListWorkflowTimerActivations(context.Context, string, string, bool) ([]runtimepipeline.WorkflowTimerActivation, error) {
	return nil, nil
}

func (startupRecoveryWorkflowOwner) StandingRunRestartDisposition(context.Context, string) (runtimepipeline.StandingRestartDisposition, error) {
	return runtimepipeline.ClassifyStandingRestart(runtimepipeline.StandingRestartFact{})
}

func (startupRecoveryWorkflowOwner) ClaimFanOutIntent(context.Context, runtimepipeline.FanOutClaimRequest) (runtimefanout.Intent, runtimefanout.Claim, bool, error) {
	return runtimefanout.Intent{}, runtimefanout.Claim{}, false, nil
}

func (startupRecoveryWorkflowOwner) LoadFanOutEvaluation(context.Context, runtimefanout.Claim) (runtimepipeline.FanOutEvaluationInput, error) {
	return runtimepipeline.FanOutEvaluationInput{}, errors.New("startup recovery fixture has no fan-out work")
}

func (startupRecoveryWorkflowOwner) CommitFanOutChunk(context.Context, runtimepipeline.FanOutChunkCommand) (runtimepipeline.CommittedFanOutChunk, error) {
	return runtimepipeline.CommittedFanOutChunk{}, errors.New("startup recovery fixture has no fan-out work")
}

func (startupRecoveryWorkflowOwner) ReleaseFanOutClaim(context.Context, runtimefanout.Claim) error {
	return nil
}

func (startupRecoveryWorkflowOwner) ReleaseFanOutRetryable(context.Context, runtimepipeline.FanOutRetryableRelease) error {
	return nil
}

func (startupRecoveryWorkflowOwner) BlockFanOutClaim(context.Context, runtimepipeline.FanOutBlockRequest) error {
	return nil
}

func (startupRecoveryWorkflowOwner) CancelRunFanOut(context.Context, string, string, time.Time) error {
	return nil
}

func (startupRecoveryWorkflowOwner) FanOutRunSummary(_ context.Context, runID string, _ time.Time) (runtimefanout.RunSummary, error) {
	return runtimefanout.RunSummary{RunID: runID}, nil
}

func startupRecoveryWorkflowPersistence(db *sql.DB, timers runtimetimerobligation.Reader) runtimepipeline.WorkflowPersistence {
	return runtimepipeline.NewWorkflowPersistence(startupRecoveryWorkflowOwner{timers: timers})
}

func newStartupRecoveryPipelineOwner(work []events.PersistedReplayEvent, claimErr error) *startupRecoveryPipelineOwner {
	return &startupRecoveryPipelineOwner{
		issuer:   runtimepipelineobligation.NewClaimIssuer(),
		scanner:  runtimepipelineobligation.NewScanIssuer(),
		work:     append([]events.PersistedReplayEvent(nil), work...),
		claimErr: claimErr,
		claims:   map[string]runtimepipelineobligation.Claim{},
		scans:    map[string]runtimepipelineobligation.ScanRequest{},
	}
}

func (s *startupRecoveryPipelineOwner) issue(eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.claims[eventID]; busy {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	claim, err := s.issuer.Issue(eventID, purpose)
	if err == nil {
		s.claims[eventID] = claim
	}
	return claim, err
}

func (s *startupRecoveryPipelineOwner) ClaimPublication(_ context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.issue(eventID, runtimepipelineobligation.PurposePublication)
}

func (s *startupRecoveryPipelineOwner) ClaimEvent(_ context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range s.work {
		if item.Event.ID() == eventID {
			return runtimepipelineobligation.ClaimedWork{
				Event: item.Event, Scope: runtimepipelineobligation.ScopeSubscribed, Claim: claim,
			}, nil
		}
	}
	delete(s.claims, eventID)
	return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
}

func (s *startupRecoveryPipelineOwner) OpenScan(_ context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	scan, err := s.scanner.Issue()
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	token, err := s.scanner.Token(scan)
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	s.mu.Lock()
	s.scans[token] = request
	s.mu.Unlock()
	return scan, nil
}

func (s *startupRecoveryPipelineOwner) ClaimBatch(_ context.Context, scan runtimepipelineobligation.Scan, limit int) (runtimepipelineobligation.ScanBatch, error) {
	if s.claimErr != nil {
		return runtimepipelineobligation.ScanBatch{}, s.claimErr
	}
	token, err := s.scanner.Token(scan)
	if err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	s.mu.Lock()
	request, ok := s.scans[token]
	s.mu.Unlock()
	if !ok {
		return runtimepipelineobligation.ScanBatch{}, runtimepipelineobligation.ErrStaleScan
	}
	batch := runtimepipelineobligation.ScanBatch{}
	for phase := 0; batch.Examined < limit; phase++ {
		query, ok := request.QueryAt(phase)
		if !ok {
			batch.Exhausted = true
			return batch, nil
		}
		if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
			continue
		}
		s.mu.Lock()
		candidates := append([]events.PersistedReplayEvent(nil), s.work...)
		s.mu.Unlock()
		for _, item := range candidates {
			if query.RunID != "" && item.Event.RunID() != query.RunID {
				continue
			}
			batch.Examined++
			claim, err := s.issue(item.Event.ID(), query.Purpose)
			if errors.Is(err, runtimepipelineobligation.ErrBusy) {
				batch.LocallyBlocked = true
				continue
			}
			if err != nil {
				return batch, err
			}
			batch.Work = append(batch.Work, runtimepipelineobligation.ClaimedWork{
				Event: item.Event, Scope: runtimepipelineobligation.ScopeSubscribed, Claim: claim,
			})
			if batch.Examined == limit {
				return batch, nil
			}
		}
	}
	return batch, nil
}

func (s *startupRecoveryPipelineOwner) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	token, err := s.scanner.Token(scan)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scans[token]; !ok {
		return runtimepipelineobligation.ErrStaleScan
	}
	delete(s.scans, token)
	return nil
}

func (s *startupRecoveryPipelineOwner) verify(claim runtimepipelineobligation.Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	return nil
}

func (s *startupRecoveryPipelineOwner) MarkDecisionProcessed(_ context.Context, claim runtimepipelineobligation.Claim) error {
	return s.verify(claim)
}

func (s *startupRecoveryPipelineOwner) Settle(_ context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.SettlementOutcome{}, runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.SettlementOutcome{}, runtimepipelineobligation.ErrStaleClaim
	}
	if err := s.issuer.Verify(claim, claim.EventID(), claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	delete(s.claims, claim.EventID())
	if disposition.Kind() != runtimepipelineobligation.DispositionDeferred {
		remaining := s.work[:0]
		for _, item := range s.work {
			if item.Event.ID() != claim.EventID() {
				remaining = append(remaining, item)
			}
		}
		s.work = remaining
	}
	return runtimepipelineobligation.CommittedSettlement(disposition.Successful()), nil
}

func (s *startupRecoveryPipelineOwner) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.claims[claim.EventID()]
	if !ok {
		return runtimepipelineobligation.ErrStaleClaim
	}
	currentToken, err := s.issuer.Token(current)
	if err != nil {
		return err
	}
	claimToken, err := s.issuer.Token(claim)
	if err != nil {
		return err
	}
	if currentToken != claimToken {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := s.issuer.Verify(claim, claim.EventID(), claim.Purpose()); err != nil {
		return err
	}
	delete(s.claims, claim.EventID())
	return nil
}

func (s *startupRecoveryPipelineOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return runtimepipelineobligation.GlobalWorkPresence{ProcessingEligible: len(s.work) > 0}, nil
}

func (s *startupRecoveryPipelineOwner) SummarizeRun(_ context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}
	for _, item := range s.work {
		if item.Event.RunID() == summary.RunID {
			summary.Replayable++
		}
	}
	return summary, nil
}

func (*startupRecoveryPipelineOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, nil
}

type startupRecoveryManagerStore struct {
	loadErr error
	agents  []runtimemanager.PersistedAgent
}

func startupRecoveryLifecycleResult(req runtimemanager.AgentLifecycleTransition) runtimemanager.AgentLifecycleTransitionResult {
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: req.OperationID, Identity: req.Identity, AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
		Topology: req.Topology, ProcessBinding: req.ProcessBinding,
	}
}

func (s *startupRecoveryManagerStore) seedRuntimeTestProcessBinding(binding runtimemanager.ProcessExecutionBinding) {
	for i := range s.agents {
		if s.agents[i].ProcessBinding.IsZero() {
			s.agents[i].ProcessBinding = binding
		}
	}
}

func (startupRecoveryManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s startupRecoveryManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s startupRecoveryManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (s startupRecoveryManagerStore) ListDurableAgentLifecycleStates(context.Context) ([]runtimemanager.AgentLifecycleState, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return runtimeTestDurableAgentLifecycleStates(s.agents)
}

func (startupRecoveryManagerStore) EnsureEntitySchema(context.Context, string) error { return nil }

type startupReadinessFinalizationStore struct {
	items []runtimepipeline.DynamicFlowRuntimeReadiness
}

func (*startupReadinessFinalizationStore) MaterializeInitialEntry(
	context.Context,
	runtimepipeline.WorkflowInstance,
	time.Time,
) (runtimepipeline.WorkflowInitialMaterializationResult, error) {
	return 0, errors.New("unexpected readiness materialization")
}

func (*startupReadinessFinalizationStore) PrepareInitialEntryLifecycle(
	context.Context,
	runtimepipeline.WorkflowInstance,
	time.Time,
) (runtimepipeline.WorkflowInstance, runtimepipeline.WorkflowLifecycleMutationPlan, error) {
	return runtimepipeline.WorkflowInstance{}, runtimepipeline.WorkflowLifecycleMutationPlan{}, errors.New("unexpected readiness lifecycle preparation")
}

func (*startupReadinessFinalizationStore) FinalizeInitialEntryLifecycle(
	context.Context,
	runtimepipeline.CommittedWorkflowLifecycleMutation,
) error {
	return errors.New("unexpected readiness lifecycle finalization")
}

func (*startupReadinessFinalizationStore) ArmInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return errors.New("unexpected readiness timer arm")
}

func (*startupReadinessFinalizationStore) ReconcileInitialEntryTimers(context.Context, runtimeflowidentity.Route) error {
	return errors.New("unexpected readiness timer reconciliation")
}

func (*startupReadinessFinalizationStore) RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.Route) error {
	return errors.New("unexpected readiness timer retirement")
}

func (*startupReadinessFinalizationStore) ReconcileDynamicFlowRuntimeReadinessPlans(
	context.Context,
	[]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation,
	time.Time,
) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	return nil, errors.New("unexpected readiness plan reconciliation")
}

func (s *startupReadinessFinalizationStore) LoadDynamicFlowRuntimeReadiness(
	_ context.Context,
	runID string,
	route runtimeflowidentity.Route,
) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	for _, item := range s.items {
		if item.Plan.RunID == runID && item.InstancePath == route.InstancePath {
			return item, true, nil
		}
	}
	return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, nil
}

func (s *startupReadinessFinalizationStore) InspectDynamicFlowRuntimeReadinessForSource(_ context.Context, source runtimecorrelation.BundleSourceFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	projection := runtimepipeline.DynamicFlowRuntimeReadinessProjection{}
	for _, item := range s.items {
		if !item.OwningRunSource.Matches(source) {
			continue
		}
		planSource, err := runtimecorrelation.DecodeBundleSourceFact(item.Plan.BundleHash, item.Plan.BundleSource)
		if err != nil {
			return projection, err
		}
		if !planSource.Matches(source) {
			projection.SourceTransitionRequired = append(projection.SourceTransitionRequired, item)
			continue
		}
		if item.Pending() {
			projection.CurrentPending = append(projection.CurrentPending, item)
		} else {
			projection.CurrentCompleted = append(projection.CurrentCompleted, item)
		}
	}
	return projection, nil
}

func (s *startupReadinessFinalizationStore) InspectDynamicFlowRuntimeReadinessForRun(_ context.Context, runID string, source runtimecorrelation.BundleSourceFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	result := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(s.items))
	for _, item := range s.items {
		if item.Plan.RunID == strings.TrimSpace(runID) && item.OwningRunSource.Matches(source) {
			result = append(result, item)
		}
	}
	return result, nil
}

func (*startupReadinessFinalizationStore) MarkDynamicFlowRuntimeTopologyReady(
	context.Context,
	runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	time.Time,
) error {
	return errors.New("unexpected readiness topology completion")
}

func (*startupReadinessFinalizationStore) CommitDynamicFlowRuntimeCreationOccurrence(
	context.Context,
	runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
	runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher,
) error {
	return errors.New("unexpected readiness creation completion")
}

func (*startupReadinessFinalizationStore) MarkTerminated(context.Context, runtimeflowidentity.Route, identity.EntityID, time.Time) error {
	return errors.New("unexpected readiness termination")
}

func (*startupReadinessFinalizationStore) Load(context.Context, runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error) {
	return runtimepipeline.WorkflowInstance{}, false, errors.New("unexpected readiness workflow load")
}

func (*startupReadinessFinalizationStore) LoadRouteRecoveryProjection(
	context.Context,
	runtimeflowidentity.Route,
) (runtimepipeline.WorkflowInstanceRouteRecoveryProjection, error) {
	return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, errors.New("unexpected readiness route projection")
}

type startupManagerReplayRuntimeAgent struct{ id string }

func (a startupManagerReplayRuntimeAgent) ID() string                      { return a.id }
func (startupManagerReplayRuntimeAgent) Type() string                      { return "generic" }
func (startupManagerReplayRuntimeAgent) Subscriptions() []events.EventType { return nil }
func (startupManagerReplayRuntimeAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	switch evt.Type() {
	case events.EventType("support.replay.drop"):
		return nil, errors.New("boom")
	case events.EventType("support.replay.leased"):
		return nil, errors.New("session currently leased")
	default:
		return nil, nil
	}
}

type startupRecoveryMinimalEventStore struct{}

func (startupRecoveryMinimalEventStore) PipelineObligations() runtimepipelineobligation.Store {
	return newStartupRecoveryPipelineOwner(nil, nil)
}

func (startupRecoveryMinimalEventStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return runtimeauthoractivity.NewEventCatalogRegistry().Register(scope, descriptors)
}

func (startupRecoveryMinimalEventStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublishNoop(ctx, command)
}

func (startupRecoveryMinimalEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (startupRecoveryMinimalEventStore) SupportsPersistedReplay() bool { return false }

type startupRecoveryEventStore struct {
	missing     []events.PersistedReplayEvent
	routes      []runtimeflowidentity.Route
	claimErr    error
	obligations *startupRecoveryPipelineOwner
}

func (s *startupRecoveryEventStore) PipelineObligations() runtimepipelineobligation.Store {
	if s.obligations == nil {
		s.obligations = newStartupRecoveryPipelineOwner(s.missing, s.claimErr)
	}
	return s.obligations
}

func (startupRecoveryEventStore) RegisterAuthorActivityEventCatalog(scope runtimeauthoractivity.Scope, descriptors []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error) {
	return runtimeauthoractivity.NewEventCatalogRegistry().Register(scope, descriptors)
}

func (startupRecoveryEventStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublishNoop(ctx, command)
}

func (startupRecoveryEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (startupRecoveryEventStore) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}

func (startupRecoveryEventStore) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}

func (s startupRecoveryEventStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return append([]runtimeflowidentity.Route(nil), s.routes...), nil
}

func testRecoveryDiagnosticsConfig(recoveryOnStartup bool) *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: recoveryOnStartup,
		},
		LLM: config.LLMConfig{
			Backend: "anthropic",
		},
	}
}

func latestStartupRecoveryDecisionLog(t *testing.T, db *sql.DB) (level, message string, failure *runtimefailures.Envelope, detail map[string]any) {
	t.Helper()
	var payloadRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT payload
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND payload->'details'->>'component' = 'runtime'
		  AND payload->'details'->>'action' = 'startup_recovery_decision'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&payloadRaw); err != nil {
		t.Fatalf("load startup recovery decision runtime log: %v", err)
	}
	payload, err := DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		t.Fatalf("DecodeCanonicalRuntimeLogPayload: %v", err)
	}
	return payload.LogLevel, payload.Message, payload.Failure, payload.Detail
}

func requireFailureCode(t testing.TB, failure *runtimefailures.Envelope, code string) {
	t.Helper()
	if failure == nil || failure.Detail.Code != code {
		t.Fatalf("failure = %#v, want detail code %q", failure, code)
	}
}

func detailString(v any) string {
	switch typed := v.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(asString(v))
	}
}

func detailBool(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func detailInt(v any) int {
	switch typed := v.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}

func detailClasses(v any) []string {
	out := []string{}
	switch typed := v.(type) {
	case []string:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	case []any:
		for _, item := range typed {
			if text := detailString(item); text != "" {
				out = append(out, text)
			}
		}
	}
	return out
}

func assertContainsClass(t *testing.T, classes []string, want string) {
	t.Helper()
	for _, item := range classes {
		if item == want {
			return
		}
	}
	t.Fatalf("recoverable_work_classes = %#v, want %q present", classes, want)
}

func TestStartupRecoveryDecisionAdmissionMatrix(t *testing.T) {
	tests := []struct {
		name       string
		recovery   bool
		blocking   int
		intrinsic  int
		delivery   runtimedelivery.RecoveryInventory
		want       startupRecoveryOutcome
		wantReason startupRecoveryReasonCode
	}{
		{
			name:       "enabled generic",
			recovery:   true,
			blocking:   1,
			want:       startupRecoveryOutcomeAllowed,
			wantReason: startupRecoveryReasonEnabledWithWork,
		},
		{
			name:       "enabled workflow",
			recovery:   true,
			blocking:   1,
			want:       startupRecoveryOutcomeAllowed,
			wantReason: startupRecoveryReasonEnabledWithWork,
		},
		{
			name:       "enabled mixed",
			recovery:   true,
			blocking:   2,
			want:       startupRecoveryOutcomeAllowed,
			wantReason: startupRecoveryReasonEnabledWithWork,
		},
		{
			name:       "disabled generic",
			blocking:   1,
			want:       startupRecoveryOutcomeDenied,
			wantReason: startupRecoveryReasonDisabledWithWork,
		},
		{
			name:       "disabled workflow",
			blocking:   1,
			want:       startupRecoveryOutcomeDenied,
			wantReason: startupRecoveryReasonDisabledWithWork,
		},
		{
			name:       "disabled mixed",
			blocking:   2,
			want:       startupRecoveryOutcomeDenied,
			wantReason: startupRecoveryReasonDisabledWithWork,
		},
		{
			name:       "disabled intrinsic standing",
			intrinsic:  1,
			want:       startupRecoveryOutcomeAllowed,
			wantReason: startupRecoveryReasonDisabledWithIntrinsic,
		},
		{
			name:       "enabled delivery",
			recovery:   true,
			delivery:   runtimedelivery.RecoveryInventory{Runs: []runtimedelivery.RecoveryRunInventory{{RunID: "run-1", Pending: 1, Failed: 2, InProgress: 3}}},
			want:       startupRecoveryOutcomeAllowed,
			wantReason: startupRecoveryReasonEnabledWithWork,
		},
		{
			name:       "disabled delivery",
			delivery:   runtimedelivery.RecoveryInventory{Runs: []runtimedelivery.RecoveryRunInventory{{RunID: "run-1", Pending: 1}}},
			want:       startupRecoveryOutcomeDenied,
			wantReason: startupRecoveryReasonDisabledWithDelivery,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := newStartupRecoveryDecisionReport(startupRecoverySnapshot{
				RecoveryOnStartup:        test.recovery,
				InspectionComplete:       true,
				StartupBlockingTimers:    test.blocking,
				StandingTimerObligations: test.intrinsic,
				Delivery:                 test.delivery,
			})
			if report.Outcome != test.want || report.ReasonCode != test.wantReason {
				t.Fatalf("decision = %s/%s, want %s/%s", report.Outcome, report.ReasonCode, test.want, test.wantReason)
			}
		})
	}
}

type startupRecoveryDispositionMap map[string]runtimepipeline.StandingRestartDispositionKind

func (m startupRecoveryDispositionMap) StandingRunRestartDisposition(_ context.Context, runID string) (runtimepipeline.StandingRestartDisposition, error) {
	kind, ok := m[runID]
	if !ok {
		return runtimepipeline.StandingRestartDisposition{}, errors.New("missing test standing restart disposition")
	}
	return runtimepipeline.StandingRestartDisposition{Kind: kind}, nil
}

func TestDeliveryRecoveryInventoryPartitionsEveryRunByStandingDisposition(t *testing.T) {
	inventory := runtimedelivery.RecoveryInventory{Runs: []runtimedelivery.RecoveryRunInventory{
		{RunID: "ordinary", Pending: 1},
		{RunID: "active", Failed: 2},
		{RunID: "suspended", InProgress: 3},
		{RunID: "orphaned", Pending: 4},
		{RunID: "terminal-declared", Failed: 5},
		{RunID: "terminal-orphaned", InProgress: 6},
		{RunID: "invalid", Pending: 7},
	}}
	restarts := startupRecoveryDispositionMap{
		"ordinary":          runtimepipeline.StandingRestartOrdinary,
		"active":            runtimepipeline.StandingRestartActiveIntrinsic,
		"suspended":         runtimepipeline.StandingRestartSuspended,
		"orphaned":          runtimepipeline.StandingRestartOrphaned,
		"terminal-declared": runtimepipeline.StandingRestartTerminalDeclared,
		"terminal-orphaned": runtimepipeline.StandingRestartTerminalOrphaned,
		"invalid":           runtimepipeline.StandingRestartInvalidCurrent,
	}

	blocking, standing, err := partitionDeliveryRecoveryInventory(context.Background(), inventory, restarts)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocking.Runs) != 1 || blocking.Runs[0].RunID != "ordinary" || blocking.Total() != 1 {
		t.Fatalf("generic delivery recovery inventory = %#v, want only ordinary run", blocking)
	}
	if standing != 2 {
		t.Fatalf("intrinsic standing delivery obligations = %d, want active run's 2", standing)
	}
}

func TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recoveryDisabledScheduleStore{active: []runtimegenericschedule.Activation{recoveryGuardActivation(t, "recover-me")}}
	eventStore := startupRecoveryMinimalEventStore{}
	managerStore := &recoveryGuardManagerStore{}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, scheduleStore),
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		DeliveryStore:       deliveryStore,
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		GenericScheduleStore:  scheduleStore,
		TimerObligationReader: scheduleStore,
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") {
		t.Fatalf("Start error = %v, want explicit startup denial", err)
	}

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	requireFailureCode(t, failure, "startup_recovery_disabled_with_work")
	if got := detailString(detail["decision_outcome"]); got != "denied" {
		t.Fatalf("decision_outcome = %q, want denied", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithWork)
	}
	if got := detailInt(detail["startup_blocking_timer_count"]); got != 1 {
		t.Fatalf("startup_blocking_timer_count = %d, want 1", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "timer obligations")
}

func TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RunCreatingRootIngress(eventtest.UUID("startup-recovery-manager-work"), "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		routes: []runtimeflowidentity.Route{
			runtimeflowidentity.DeriveRoute("child", "inst-1"),
		},
	}
	managerIdentity := agentidentitytest.RootRuntime(t, "persisted-agent", "startup-recovery-manager-work")
	managerTopology, err := runtimeagenttopology.FlowReadinessAdmission(
		eventtest.UUID("startup-recovery-manager-run"),
		"review/inst-1",
		"startup-recovery-manager-readiness",
	)
	if err != nil {
		t.Fatalf("construct manager snapshot topology: %v", err)
	}
	managerStore := &startupRecoveryManagerStore{agents: []runtimemanager.PersistedAgent{{
		Config: runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: managerIdentity.AgentID(), Identity: managerIdentity,
			Role: "worker", Type: "managed", Model: "regular",
		}),
		Topology: managerTopology,
	}}}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(false),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, nil),
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		DeliveryStore:       deliveryStore,
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, message, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "warn" {
		t.Fatalf("log level = %q, want warn", level)
	}
	if message != "Runtime startup allowed with manager recovery skipped" {
		t.Fatalf("log message = %q, want manager recovery skipped", message)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonDisabledWithManagerWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonDisabledWithManagerWork)
	}
	if !detailBool(detail["manager_recoverable_work_present"]) {
		t.Fatalf("manager_recoverable_work_present = %#v, want true", detail["manager_recoverable_work_present"])
	}
	if detailBool(detail["startup_blocking_recoverable_work_present"]) {
		t.Fatalf("startup_blocking_recoverable_work_present = %#v, want false", detail["startup_blocking_recoverable_work_present"])
	}
	if got := detailInt(detail["persisted_agent_count"]); got != 1 {
		t.Fatalf("persisted_agent_count = %d, want 1", got)
	}
	if got := detailInt(detail["persisted_flow_instance_route_count"]); got != 1 {
		t.Fatalf("persisted_flow_instance_route_count = %d, want 1", got)
	}
	if !detailBool(detail["replay_eligible_event_present"]) {
		t.Fatalf("replay_eligible_event_present = %#v, want true", detail["replay_eligible_event_present"])
	}
	classes := detailClasses(detail["recoverable_work_classes"])
	assertContainsClass(t, classes, "persisted agents")
	assertContainsClass(t, classes, "persisted flow instance routes")
	assertContainsClass(t, classes, "events missing pipeline receipts")
}

func TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recoveryDisabledScheduleStore{active: []runtimegenericschedule.Activation{recoveryGuardActivation(t, "recover-me")}}
	eventStore := startupRecoveryMinimalEventStore{}
	managerStore := &recoveryGuardManagerStore{}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, scheduleStore),
		DeliveryStore:       deliveryStore,
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		GenericScheduleStore:  scheduleStore,
		TimerObligationReader: scheduleStore,
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "info" {
		t.Fatalf("log level = %q, want info", level)
	}
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if got := detailString(detail["decision_outcome"]); got != "allowed" {
		t.Fatalf("decision_outcome = %q, want allowed", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonEnabledWithWork) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonEnabledWithWork)
	}
	if !detailBool(detail["schedule_restore_attempted"]) {
		t.Fatalf("schedule_restore_attempted = %#v, want true", detail["schedule_restore_attempted"])
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 1 {
		t.Fatalf("schedule_replayed_count = %d, want 1", got)
	}
	if got := detailInt(detail["schedule_skipped_count"]); got != 0 {
		t.Fatalf("schedule_skipped_count = %d, want 0", got)
	}
	if got := detailInt(detail["schedule_dropped_count"]); got != 0 {
		t.Fatalf("schedule_dropped_count = %d, want 0", got)
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "timer obligations")
}

func TestRuntimeStart_WorkflowOnlyRecoveryUsesFamilyAwareBootAndRestorationDetail(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	const runID = "31000000-0000-0000-0000-000000000001"
	scheduleStore := &recoveryDisabledScheduleStore{
		obligations: &runtimetimerobligation.Snapshot{
			Runs: []runtimetimerobligation.RunObligations{{
				RunID: runID,
				Families: []runtimetimerobligation.FamilyObligation{{
					Family:           runtimetimerobligation.FamilyWorkflowTimer,
					ActiveCount:      1,
					RecoverableCount: 1,
				}},
			}},
		},
	}
	eventStore := startupRecoveryMinimalEventStore{}
	managerStore := &recoveryGuardManagerStore{}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, scheduleStore),
		DeliveryStore:       deliveryStore,
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		GenericScheduleStore:  scheduleStore,
		TimerObligationReader: scheduleStore,
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if failure != nil {
		t.Fatalf("log failure = %#v, want nil", failure)
	}
	if !detailBool(detail["schedule_restore_attempted"]) {
		t.Fatalf("schedule_restore_attempted = %#v, want true for workflow-only restoration", detail["schedule_restore_attempted"])
	}
	if got := detailInt(detail["schedule_replayed_count"]); got != 0 {
		t.Fatalf("generic schedule_replayed_count = %d, want 0", got)
	}
	if got := detailInt(detail["startup_blocking_timer_count"]); got != 1 {
		t.Fatalf("startup_blocking_timer_count = %d, want 1", got)
	}
	obligations, ok := detail["timer_obligations"].(map[string]any)
	if !ok {
		t.Fatalf("timer_obligations = %#v, want object", detail["timer_obligations"])
	}
	runs, ok := obligations["runs"].([]any)
	if !ok || len(runs) != 1 {
		t.Fatalf("timer_obligations.runs = %#v, want one workflow-only run", obligations["runs"])
	}

	boot := startupRecoveryDecisionReport{
		Snapshot: startupRecoverySnapshot{
			RecoveryOnStartup:     true,
			InspectionComplete:    true,
			StartupBlockingTimers: 1,
			TimerObligations:      *scheduleStore.obligations,
		},
	}.bootPayload()
	if got := detailInt(boot["startup_blocking_timer_count"]); got != 1 {
		t.Fatalf("boot startup_blocking_timer_count = %d, want 1", got)
	}
	if _, ok := boot["timer_obligations"].(runtimetimerobligation.Snapshot); !ok {
		t.Fatalf("boot timer_obligations = %#v, want typed snapshot", boot["timer_obligations"])
	}
}

func TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &startupRecoveryEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RunCreatingRootIngress(eventtest.UUID("startup-recovery-claim-failure"), "support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		claimErr: errors.New("claim failed"),
	}
	managerStore := &recoveryGuardManagerStore{}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, nil),
		DeliveryStore:       deliveryStore,
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(ctx); err == nil || !strings.Contains(err.Error(), "recover pipeline obligations before delivery enumeration") {
		t.Fatalf("Start error = %v, want boot-fatal recovery exhaustion failure", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}()

	level, _, failure, detail := latestStartupRecoveryDecisionLog(t, db)
	if level != "error" {
		t.Fatalf("log level = %q, want error", level)
	}
	requireFailureCode(t, failure, "startup_manager_recovery_failed")
	if got := detailString(detail["decision_outcome"]); got != "degraded" {
		t.Fatalf("decision_outcome = %q, want degraded", got)
	}
	if got := detailString(detail["decision_reason_code"]); got != string(startupRecoveryReasonRecoverFailed) {
		t.Fatalf("decision_reason_code = %q, want %q", got, startupRecoveryReasonRecoverFailed)
	}
	if !detailBool(detail["manager_recovery_attempted"]) {
		t.Fatalf("manager_recovery_attempted = %#v, want true", detail["manager_recovery_attempted"])
	}
	if !detailBool(detail["manager_reset_attempted"]) {
		t.Fatalf("manager_reset_attempted = %#v, want true", detail["manager_reset_attempted"])
	}
	assertContainsClass(t, detailClasses(detail["recoverable_work_classes"]), "events missing pipeline receipts")
}

func TestRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatal(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	activeWorkflowVersion := strings.TrimSpace(module.SemanticSource().WorkflowVersion())
	if activeWorkflowVersion == "" {
		t.Fatal("startup readiness test requires an active workflow version")
	}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)
	managerStore := &startupRecoveryManagerStore{}
	eventStore := startupRecoveryMinimalEventStore{}

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, nil),
		DeliveryStore:       deliveryStore,
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Manager.Shutdown(); err != nil {
		t.Fatalf("retire constructed manager before readiness failure replacement: %v", err)
	}
	runID := eventtest.UUID("startup-readiness-fatal-run")
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		t.Fatal("startup readiness context requires bundle source fact")
	}
	bundleHash, bundleSource := sourceFact.StorageValues()
	readinessStore := &startupReadinessFinalizationStore{items: []runtimepipeline.DynamicFlowRuntimeReadiness{{
		InstancePath:    "review/inst-1",
		OwningRunSource: sourceFact,
		RunStatus:       "running",
		InstanceStatus:  "active",
		Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: runtimeflowidentity.Instance{
				TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
				InstancePath: "review/inst-1", EntityID: eventtest.UUID("startup-readiness-fatal-entity"),
				HasStoredPath: true,
			},
			RunID:           runID,
			BundleHash:      bundleHash,
			BundleSource:    bundleSource,
			WorkflowVersion: activeWorkflowVersion + "-changed",
			ExecutionMode:   executionmode.Live,
		},
	}}}
	managerRoles := runtimeTestManagerBusRoles(rt.Bus)
	managerRoles.LifecycleCensus = managerStore
	managerRoles.StandingRestarts = startupRecoveryWorkflowOwner{}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return startupManagerReplayRuntimeAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{
		BaseContext:                    ctx,
		BundleSourceFact:               sourceFact,
		LifecycleStore:                 managerStore,
		DeliveryStore:                  deliveryStore,
		SemanticSource:                 module.SemanticSource(),
		WorkflowInstances:              readinessStore,
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      rt.WorkOccurrence(),
		PersistenceRoles:               managerRoles,
		ReceiverExecution:              eventreceiver.NormalExecution(),
	}, managerStore)
	installRuntimeTestManagerGeneration(t, ctx, rt.Manager, rt.startupGrant)

	err = rt.Start(ctx)
	if err == nil {
		t.Fatal("Start succeeded across dynamic flow readiness finalization failure")
	}
	if !runtimemanager.IsDynamicFlowRuntimeReadinessFinalizationError(err) {
		t.Fatalf("Start error = %v, want dynamic flow readiness finalization error", err)
	}
	if !strings.Contains(err.Error(), "workflow version changed") {
		t.Fatalf("Start error = %v, want exact readiness failure evidence", err)
	}
}

func TestRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatal(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := startupRecoveryMinimalEventStore{}
	managerStore := startupRecoveryManagerStore{loadErr: errors.New("load agents failed")}
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{Config: testRecoveryDiagnosticsConfig(true),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, nil),
		DeliveryStore:       deliveryStore,
		RuntimeLogStore:     runtimeLogPersistenceStub{db: db},
		EventStore:          eventStore,
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		PipelineObligations: eventStore.PipelineObligations(),
		ManagerStore:        managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: managerStore, StandingRestarts: startupRecoveryWorkflowOwner{},
		},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "list durable lifecycle cells for process takeover") {
		t.Fatalf("Start error = %v, want fail-closed topology readback gate", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
