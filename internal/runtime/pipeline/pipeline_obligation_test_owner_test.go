package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var errPipelineTestObligationUnavailable = errors.New("pipeline unit fixture has no selected-store obligation owner")

type unavailablePipelineTestObligationOwner struct{}

type unavailablePipelineTestFanOutOwner struct{}

func (unavailablePipelineTestFanOutOwner) ClaimFanOutIntent(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error) {
	return FanOutEvaluationInput{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) CommitFanOutChunk(context.Context, FanOutChunkCommand) (CommittedFanOutChunk, error) {
	return CommittedFanOutChunk{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) ReleaseFanOutClaim(context.Context, fanoutobligation.Claim) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) ReleaseFanOutRetryable(context.Context, FanOutRetryableRelease) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) BlockFanOutClaim(context.Context, FanOutBlockRequest) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) CancelRunFanOut(context.Context, string, string, time.Time) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestFanOutOwner) FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error) {
	return fanoutobligation.RunSummary{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) ClaimPublication(context.Context, string) (runtimepipelineobligation.Claim, error) {
	return runtimepipelineobligation.Claim{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) ClaimEvent(context.Context, string, runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	return runtimepipelineobligation.ClaimedWork{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	return runtimepipelineobligation.Scan{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) ClaimBatch(context.Context, runtimepipelineobligation.Scan, int) (runtimepipelineobligation.ScanBatch, error) {
	return runtimepipelineobligation.ScanBatch{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) CloseScan(context.Context, runtimepipelineobligation.Scan) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) Settle(context.Context, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	return runtimepipelineobligation.SettlementOutcome{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) Release(context.Context, runtimepipelineobligation.Claim) error {
	return errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) SummarizeRun(context.Context, string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{}, errPipelineTestObligationUnavailable
}

func (unavailablePipelineTestObligationOwner) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, errPipelineTestObligationUnavailable
}

func (*recordingPipelineBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

func (noopPipelineBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

func (pipelineTestBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailablePipelineTestObligationOwner{}
}

type unavailablePipelineTestDeliveryStore struct{ runtimedelivery.Store }
type unavailablePipelineTestDecisionCards struct{}

func (*unavailablePipelineTestDecisionCards) CreateDecisionCard(context.Context, decisioncard.Card) error {
	return nil
}
func (*unavailablePipelineTestDecisionCards) ListDecisionCards(context.Context, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	return nil, "", nil
}
func (*unavailablePipelineTestDecisionCards) GetDecisionCard(context.Context, string) (decisioncard.Card, error) {
	return decisioncard.Card{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestDecisionCards) DecideDecisionCard(context.Context, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error) {
	return decisioncard.DecisionOutcome{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestDecisionCards) DeferDecisionCard(context.Context, decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error) {
	return decisioncard.DecisionOutcome{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestDecisionCards) BeginDecisionCardInput(context.Context, decisioncard.BeginInputRequest) (decisioncard.InputDraft, error) {
	return decisioncard.InputDraft{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestDecisionCards) CancelDecisionCardInput(context.Context, decisioncard.CancelInputRequest) (decisioncard.InputDraft, error) {
	return decisioncard.InputDraft{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestDecisionCards) ListDecisionCardChanges(context.Context, decisioncard.SubscriptionOptions) ([]decisioncard.Change, error) {
	return nil, nil
}
func (*unavailablePipelineTestDecisionCards) SupersedeDecisionCardsForStage(context.Context, string, string, string, string, time.Time) error {
	return nil
}

type unavailablePipelineTestProposedEffects struct{}

func (*unavailablePipelineTestProposedEffects) CreateProposedEffectCard(context.Context, decisioncard.Card, decisioncard.ProposedEffectContinuation) error {
	return nil
}
func (*unavailablePipelineTestProposedEffects) LoadProposedEffectContinuation(context.Context, string) (decisioncard.ProposedEffectContinuation, error) {
	return decisioncard.ProposedEffectContinuation{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestProposedEffects) CompleteProposedEffectRoute(context.Context, string, string, time.Time) (decisioncard.ProposedEffectContinuation, error) {
	return decisioncard.ProposedEffectContinuation{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestProposedEffects) SupersedeProposedEffectsForLoopGenerations(context.Context, string, string, []attemptgeneration.Generation, string, time.Time) error {
	return nil
}
func (*unavailablePipelineTestProposedEffects) ProposedEffectReadback(context.Context, string) (decisioncard.ProposedEffectReadback, error) {
	return decisioncard.ProposedEffectReadback{}, decisioncard.ErrNotFound
}

type unavailablePipelineTestHumanTasks struct{}

func (*unavailablePipelineTestHumanTasks) CreateHumanTaskCard(context.Context, decisioncard.Card, decisioncard.HumanTaskContinuation) error {
	return nil
}
func (*unavailablePipelineTestHumanTasks) LoadHumanTaskContinuation(context.Context, string) (decisioncard.HumanTaskContinuation, error) {
	return decisioncard.HumanTaskContinuation{}, decisioncard.ErrNotFound
}
func (*unavailablePipelineTestHumanTasks) CompleteHumanTaskOutcome(context.Context, string, string, time.Time) (decisioncard.HumanTaskContinuation, error) {
	return decisioncard.HumanTaskContinuation{}, decisioncard.ErrNotFound
}

type unavailablePipelineTestDecisionCardDraftExpiry struct{}

func (*unavailablePipelineTestDecisionCardDraftExpiry) ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error) {
	return 0, nil
}

type unavailablePipelineTestHumanTaskExpiry struct{}

func (*unavailablePipelineTestHumanTaskExpiry) ListDueHumanTaskExpiryEvents(context.Context, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*unavailablePipelineTestHumanTaskExpiry) CommitHumanTaskExpirations(context.Context, HumanTaskExpiryCommand) (CommittedHumanTaskExpiry, error) {
	return CommittedHumanTaskExpiry{}, errors.New("human-task expiry is unavailable")
}

type unavailablePipelineTestDecisionCardMutations struct{ DecisionCardMutationOwner }
type unavailablePipelineTestDeliveryRuntime struct{ WorkflowDeliveryRuntime }
type unavailablePipelineTestDeadLetters struct{ runtimedeadletters.Recorder }
type unavailablePipelineTestRunLifecycle struct {
	runtimerunlifecycle.OperationOwner
}

func (*unavailablePipelineTestRunLifecycle) RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	return "", nil
}

func (*unavailablePipelineTestRunLifecycle) TransitionActiveRun(context.Context, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return "", nil
}

func (*unavailablePipelineTestRunLifecycle) MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", nil
}

// completeDurablePipelineTestOptions keeps production construction strict while
// allowing a focused unit test to state only the durable role it exercises.
// Calling an unstated role still panics through its nil embedded test interface.
func completeDurablePipelineTestOptions(bus Bus, opts PipelineCoordinatorOptions) PipelineCoordinatorOptions {
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.DeliveryStore == nil {
		opts.DeliveryStore = &unavailablePipelineTestDeliveryStore{}
	}
	if opts.DeadLetters == nil {
		opts.DeadLetters = &unavailablePipelineTestDeadLetters{}
	}
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = unavailablePipelineTestObligationOwner{}
	}
	if opts.DecisionCards == nil {
		opts.DecisionCards = &unavailablePipelineTestDecisionCards{}
	}
	if opts.ProposedEffects == nil {
		opts.ProposedEffects = &unavailablePipelineTestProposedEffects{}
	}
	if opts.HumanTasks == nil {
		opts.HumanTasks = &unavailablePipelineTestHumanTasks{}
	}
	if opts.DecisionCardDraftExpiry == nil {
		opts.DecisionCardDraftExpiry = &unavailablePipelineTestDecisionCardDraftExpiry{}
	}
	if opts.HumanTaskExpiry == nil {
		opts.HumanTaskExpiry = &unavailablePipelineTestHumanTaskExpiry{}
	}
	if opts.DeliveryRuntime == nil {
		if runtime, ok := bus.(WorkflowDeliveryRuntime); ok {
			opts.DeliveryRuntime = runtime
		} else {
			opts.DeliveryRuntime = &unavailablePipelineTestDeliveryRuntime{}
		}
	}
	if opts.RunLifecycle == nil {
		if opts.Persistence.store != nil && opts.Persistence.store.runLifecycle != nil {
			opts.RunLifecycle = opts.Persistence.store.runLifecycle
		} else {
			opts.RunLifecycle = &unavailablePipelineTestRunLifecycle{}
		}
	}
	return opts
}

func newDurablePipelineCoordinatorForTest(bus Bus, db *sql.DB, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	if opts.Persistence.Configured() && !opts.Persistence.Valid() {
		panic("pipeline test configured incomplete workflow persistence: " + strings.Join(missingWorkflowPersistenceTestRoles(opts.Persistence), ", "))
	}
	pc := NewPipelineCoordinatorWithOptions(bus, completeDurablePipelineTestOptions(bus, opts))
	if pc != nil && pc.workflowStore != nil && opts.Persistence.store != nil {
		fixture := opts.Persistence.store.testFixture()
		registerWorkflowPersistenceFixture(pc.workflowStore, fixture.db, fixture.dialect, fixture.runner)
	}
	return pc
}

func newPreviewPipelineCoordinatorForTest(bus Bus, opts PipelineCoordinatorOptions) *PipelineCoordinator {
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	return newPreviewPipelineCoordinator(bus, opts)
}

func missingWorkflowPersistenceTestRoles(p WorkflowPersistence) []string {
	if p.store == nil {
		return []string{"store"}
	}
	roles := []struct {
		name    string
		missing bool
	}{
		{"entity_query", p.store.entityQuery == nil},
		{"route_recovery", p.store.routeRecovery == nil},
		{"activity_results", p.store.activityResults == nil},
		{"activity_journal", p.store.activityJournal == nil},
		{"gate_routes", p.store.gateRoutes == nil},
		{"timer_obligations", p.store.timerObligations == nil},
		{"fan_out_obligations", p.store.fanOutObligations == nil},
		{"engine_mutations", p.store.engineMutations == nil},
		{"card_mutations", p.store.cardMutations == nil},
		{"timer_occurrences", p.store.timerOccurrences == nil},
		{"timer_activations", p.store.timerActivations == nil},
		{"readiness", p.store.readiness == nil},
		{"standing_services", p.store.standingServices == nil},
		{"decision_routes", p.store.decisionRoutes == nil},
		{"instance_reader", p.store.instanceReader == nil},
		{"entity_state_reader", p.store.entityStateReader == nil},
		{"target_reader", p.store.targetReader == nil},
		{"initial_commits", p.store.initialCommits == nil},
	}
	missing := make([]string, 0, len(roles))
	for _, role := range roles {
		if role.missing {
			missing = append(missing, role.name)
		}
	}
	return missing
}

func TestPipelineCoordinatorRequiresCanonicalObligationOwner(t *testing.T) {
	module := staticSemanticWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})}
	if preview := newPreviewPipelineCoordinator(previewBus{}, PipelineCoordinatorOptions{
		Module:           module,
		ExecutionPosture: executionposture.Live,
	}); preview == nil {
		t.Fatal("explicit preview coordinator was not constructed")
	}

	if durable := NewPipelineCoordinatorWithOptions(previewBus{}, PipelineCoordinatorOptions{
		Module:           module,
		ExecutionPosture: executionposture.Live,
		Persistence:      WorkflowPersistence{store: &workflowInstanceStore{}},
	}); durable != nil {
		t.Fatal("durable coordinator accepted incomplete persistence roles")
	}

}

func TestRuntimePipelineRejectsUnconfiguredReceiverExecution(t *testing.T) {
	module := staticSemanticWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})}
	opts := completeDurablePipelineTestOptions(previewBus{}, PipelineCoordinatorOptions{
		Module:      module,
		Persistence: WorkflowPersistence{store: &workflowInstanceStore{}},
	})
	opts.ReceiverExecution = eventreceiver.ExecutionVariant{}
	if coordinator := NewPipelineCoordinatorWithOptions(previewBus{}, opts); coordinator != nil {
		t.Fatal("runtime Pipeline accepted unconfigured receiver execution")
	}

	coordinator := &PipelineCoordinator{runtimeReceiver: true}
	evt := eventtest.RuntimeControl(
		"11111111-1111-4111-8111-111111111111",
		"pipeline.receiver_execution",
		"pipeline-test",
		"",
		[]byte(`{}`),
		0,
		"22222222-2222-4222-8222-222222222222",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	if _, _, _, err := coordinator.Intercept(context.Background(), evt); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unconfigured runtime Pipeline interception = %v", err)
	}
}
