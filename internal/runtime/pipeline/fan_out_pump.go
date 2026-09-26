package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

const fanOutClaimLease = 30 * time.Second

func (pc *PipelineCoordinator) ReportFanOutServingError(ctx context.Context, err error) {
	if pc != nil && err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "serve_fan_out_obligation", "", "", runtimeWorkflowID, "", nil, err)
	}
}

type fanOutTurnDisposition uint8

const (
	fanOutTurnAwaitScan fanOutTurnDisposition = iota
	fanOutTurnExhausted
	fanOutTurnCommitted
	fanOutTurnRetryWait
	fanOutTurnBlocked
	fanOutTurnUncertain
	fanOutTurnYielded
	fanOutTurnReleased
)

// A completed or durably deferred intent does not imply queue exhaustion.
// Cancellation and failures without a confirmed disposition await recovery.
func (d fanOutTurnDisposition) refill() bool {
	switch d {
	case fanOutTurnCommitted, fanOutTurnRetryWait, fanOutTurnBlocked, fanOutTurnUncertain:
		return true
	default:
		return false
	}
}

func (d fanOutTurnDisposition) suppressesRelease() bool {
	return d == fanOutTurnRetryWait || d == fanOutTurnBlocked || d == fanOutTurnUncertain
}

// ServeFanOutCandidate executes only the candidate selected by the process-wide
// serving owner, using its generation-bound selected-store port.
func (pc *PipelineCoordinator) ServeFanOutCandidate(ctx context.Context, owner FanOutObligationOwner, candidate fanoutobligation.IntentKey) (FanOutTurnResult, error) {
	if pc == nil || owner == nil {
		return FanOutTurnResult{}, errors.New("fan-out execution requires coordinator and admitted store owner")
	}
	if err := candidate.Validate(); err != nil {
		return FanOutTurnResult{}, err
	}
	disposition, err := pc.claimAndServeFanOutTurn(ctx, owner, &candidate, time.Now().UTC())
	return FanOutTurnResult{Refill: disposition.refill()}, err
}

func (pc *PipelineCoordinator) claimAndServeFanOutTurn(ctx context.Context, owner FanOutObligationOwner, candidate *fanoutobligation.IntentKey, now time.Time) (disposition fanOutTurnDisposition, turnErr error) {
	if err := pc.sourceArtifactFact.Validate(); err != nil {
		return fanOutTurnAwaitScan, fmt.Errorf("fan-out pump requires exact bundle source fact: %w", err)
	}
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, FanOutClaimRequest{
		Owner: pc.fanOutOwnerID, BundleHash: pc.sourceArtifactFact.BundleHash(), Candidate: candidate, Now: now.UTC(), Lease: fanOutClaimLease,
	})
	if !found {
		if err != nil {
			return fanOutTurnAwaitScan, err
		}
		return fanOutTurnExhausted, nil
	}
	if err != nil {
		// A returned claim is acknowledged ownership even when cleanup failed.
		settlement, releaseErr := owner.ReleaseFanOutClaim(context.WithoutCancel(ctx), claim)
		if settlement.Acknowledged {
			return fanOutTurnReleased, errors.Join(err, releaseErr)
		}
		return fanOutTurnAwaitScan, errors.Join(err, releaseErr)
	}
	if candidate != nil && (intent.Request.Key != *candidate || claim.Key != *candidate) {
		settlement, releaseErr := owner.ReleaseFanOutClaim(context.WithoutCancel(ctx), claim)
		if settlement.Acknowledged {
			return fanOutTurnReleased, errors.Join(errors.New("fan-out claim differs from the selected candidate"), releaseErr)
		}
		return fanOutTurnAwaitScan, errors.Join(errors.New("fan-out claim differs from the selected candidate"), releaseErr)
	}
	turnStarted := time.Now()
	release := true
	defer func() {
		if release {
			settlement, err := owner.ReleaseFanOutClaim(context.WithoutCancel(ctx), claim)
			if settlement.Acknowledged && disposition == fanOutTurnAwaitScan {
				disposition = fanOutTurnReleased
			}
			if err != nil {
				turnErr = errors.Join(turnErr, err)
			}
		}
	}()
	blockClaim := func(stage string, ordinal int, cause error, plans []runtimeengine.DurablePublicationPlan) (fanOutTurnDisposition, error) {
		releaseErr := plannerReleaseFanOutPlans(context.WithoutCancel(ctx), pc.bus, plans)
		cause = fanOutBlockedTurnCause(stage, ordinal, intent, cause)
		failure := runtimefailures.Normalize(cause, "runtime.fan_out", stage)
		settlement, blockErr := owner.BlockFanOutClaim(context.WithoutCancel(ctx), FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: failure})
		if settlement.Acknowledged {
			release = false
			return fanOutTurnBlocked, errors.Join(cause, releaseErr, blockErr)
		}
		return fanOutTurnAwaitScan, errors.Join(cause, releaseErr, blockErr)
	}

	input, err := owner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		return blockClaim("load_evaluation", -1, err, nil)
	}
	if err := input.Validate(intent); err != nil {
		return blockClaim("validate_evaluation", -1, err, nil)
	}
	planner, ok := pc.bus.(FanOutPublicationPlanner)
	if !ok {
		return blockClaim("resolve_publication_planner", -1, fmt.Errorf("fan-out pump requires the canonical engine publication planner"), nil)
	}
	executor, err := runtimeengine.NewExecutor(coordinatorEngineDependencies(pc), pipelineEngineEvaluator{evaluator: pc.expressionEval, coordinator: pc})
	if err != nil {
		return blockClaim("create_executor", -1, err, nil)
	}
	if intent.Request.Deployment != nil {
		if intent.Request.Deployment.BundleHash != pc.sourceArtifactFact.BundleHash() {
			return blockClaim("validate_plan_source", -1, fmt.Errorf("deployment feed bundle disagrees with admitted runtime source"), nil)
		}
	} else if intent.Request.PlanRef.BundleHash != pc.sourceArtifactFact.BundleHash() {
		return blockClaim("validate_plan_source", -1, fmt.Errorf("fan-out claimed plan bundle disagrees with admitted runtime source"), nil)
	}
	workCtx := runtimecorrelation.WithSourceArtifactFact(ctx, pc.sourceArtifactFact)
	workCtx = runtimecorrelation.WithRunID(workCtx, intent.Request.Key.RunID)
	if intent.Request.Deployment == nil {
		workCtx = runtimecorrelation.WithInboundEvent(workCtx, input.Trigger)
	}
	group, err := owner.BeginFanOutPublicationGroup(workCtx, claim)
	if group != nil {
		defer func() { turnErr = errors.Join(turnErr, group.Close(context.WithoutCancel(workCtx))) }()
	}
	if err != nil {
		_, failureKind := fanOutPrecommitFailure(err)
		switch failureKind {
		case fanOutFailureYield:
			return fanOutTurnYielded, err
		case fanOutFailureRetry:
			retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, nil, err, turnStarted)
			if retryReleased {
				release = false
				return fanOutTurnRetryWait, retryErr
			}
			return fanOutTurnAwaitScan, retryErr
		default:
			return blockClaim("begin_publication_group", -1, err, nil)
		}
	}
	if group == nil {
		return blockClaim("begin_publication_group", -1, errors.New("fan-out publication group is required"), nil)
	}

	end := intent.ChunkEndOrdinal()
	var evaluation *runtimeengine.FanOutEvaluation
	var preparationErr error
	if intent.Request.Deployment == nil {
		evaluation, preparationErr = executor.PrepareFanOutEvaluation(workCtx, intent, input.Trigger)
	}
	outcomes := make([]FanOutChunkOutcome, end-intent.Cursor)
	prepared := make([]runtimeengine.DurablePublicationPlan, 0, end-intent.Cursor)
	requests := make([]FanOutPublicationRequest, 0, end-intent.Cursor)
	for ordinal := intent.Cursor; ordinal < end; ordinal++ {
		outcomes[ordinal-intent.Cursor].Ordinal = ordinal
		var emit runtimeengine.EmitIntent
		evalErr := preparationErr
		if evalErr == nil {
			if intent.Request.Deployment != nil {
				emit, evalErr = deploymentFeedEmitIntent(intent, input.Items[ordinal-input.StartOrdinal], ordinal, pc.executionPosture.RootMode())
			} else {
				emit, evalErr = evaluation.EvaluateOrdinal(workCtx, input.Items[ordinal-input.StartOrdinal], ordinal)
			}
		}
		if evalErr != nil {
			failure, disposition := fanOutPrecommitFailure(evalErr)
			switch disposition {
			case fanOutFailureYield:
				return fanOutTurnYielded, errors.Join(evalErr, planner.ReleaseEnginePublications(context.WithoutCancel(workCtx), prepared))
			case fanOutFailureRetry:
				retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, prepared, evalErr, turnStarted)
				if retryReleased {
					release = false
					return fanOutTurnRetryWait, retryErr
				}
				return fanOutTurnAwaitScan, retryErr
			case fanOutFailureBlock:
				return blockClaim("evaluate_ordinal", ordinal, evalErr, prepared)
			case fanOutFailureItemSemantic:
			default:
				return blockClaim("classify_evaluation_failure", ordinal, fmt.Errorf("fan-out evaluation returned invalid failure disposition"), prepared)
			}
			outcomes[ordinal-intent.Cursor].Failure = failure
			continue
		}
		requests = append(requests, FanOutPublicationRequest{Ordinal: ordinal, Intent: emit})
	}
	preparations, acquisitionErr := planner.PrepareFanOutPublications(workCtx, group, requests)
	if acquisitionErr != nil {
		_, disposition := fanOutPrecommitFailure(acquisitionErr)
		switch disposition {
		case fanOutFailureYield:
			return fanOutTurnYielded, acquisitionErr
		case fanOutFailureRetry:
			retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, nil, acquisitionErr, turnStarted)
			if retryReleased {
				release = false
				return fanOutTurnRetryWait, retryErr
			}
			return fanOutTurnAwaitScan, retryErr
		default:
			return blockClaim("acquire_publication_claims", -1, acquisitionErr, nil)
		}
	}
	for _, preparation := range preparations {
		if preparation.Publication != nil {
			prepared = append(prepared, preparation.Publication)
		}
	}
	if len(preparations) != len(requests) {
		return blockClaim("validate_publication_cardinality", -1, errors.New("fan-out preparation omitted ordinal results"), prepared)
	}
	for i, preparation := range preparations {
		ordinal := requests[i].Ordinal
		if preparation.Ordinal != ordinal || (preparation.Publication != nil && preparation.Err != nil) {
			return blockClaim("validate_publication_identity", ordinal, errors.New("fan-out preparation substituted ordinal or ambiguous result"), prepared)
		}
		plan, prepareErr := preparation.Publication, preparation.Err
		if prepareErr != nil {
			failure, disposition := fanOutPrecommitFailure(prepareErr)
			switch disposition {
			case fanOutFailureYield:
				return fanOutTurnYielded, errors.Join(prepareErr, planner.ReleaseEnginePublications(context.WithoutCancel(workCtx), prepared))
			case fanOutFailureRetry:
				retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, prepared, prepareErr, turnStarted)
				if retryReleased {
					release = false
					return fanOutTurnRetryWait, retryErr
				}
				return fanOutTurnAwaitScan, retryErr
			case fanOutFailureBlock:
				return blockClaim("prepare_publication", ordinal, prepareErr, prepared)
			case fanOutFailureItemSemantic:
			default:
				return blockClaim("classify_publication_failure", ordinal, fmt.Errorf("fan-out planner returned invalid failure disposition"), prepared)
			}
			outcomes[ordinal-intent.Cursor].Failure = failure
			continue
		}
		if plan == nil {
			return blockClaim("validate_publication_cardinality", ordinal, fmt.Errorf("fan-out planner returned no plan for ordinal %d", ordinal), prepared)
		}
		outcomes[ordinal-intent.Cursor].Publication = plan
	}

	committed, committedDisposition, err := pc.commitFanOutRange(workCtx, owner, planner, group, claim, outcomes, now.UTC())
	if committedDisposition.suppressesRelease() {
		release = false
	}
	if err != nil {
		return committedDisposition, err
	}
	if committed.Intent.ClaimOwner == "" {
		release = false
	}
	if err := planner.FinalizeFanOutPublications(workCtx, group, committed.Publications); err != nil {
		return fanOutTurnCommitted, errors.Join(committed.PostCommitFailure, err)
	}
	for _, publication := range committed.Publications {
		if publication == nil {
			return fanOutTurnCommitted, errors.Join(committed.PostCommitFailure, fmt.Errorf("fan-out committed publication evidence is required"))
		}
	}
	if len(committed.Publications) > 0 {
		if err := planner.DispatchFanOutPublications(context.WithoutCancel(workCtx), group, committed.Publications); err != nil {
			return fanOutTurnCommitted, errors.Join(committed.PostCommitFailure, err)
		}
	}
	return fanOutTurnCommitted, committed.PostCommitFailure
}

func deploymentFeedEmitIntent(intent fanoutobligation.Intent, item any, ordinal int, mode executionmode.Mode) (runtimeengine.EmitIntent, error) {
	projection, err := fanoutobligation.PrepareDeploymentOrdinalEmission(intent, ordinal)
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	payload, err := canonicaljson.MarshalPreservingNumberKinds(item)
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	event, err := projection.NewEvent(events.EventFacts{
		Payload: payload, Envelope: events.EventEnvelope{Scope: events.EventScopeGlobal}, ExecutionMode: mode,
	})
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	return runtimeengine.EmitIntent{Event: event}, nil
}

func releaseFanOutPrecommitRetry(
	ctx context.Context,
	owner FanOutObligationOwner,
	planner EnginePublicationPlanner,
	claim fanoutobligation.Claim,
	plans []runtimeengine.DurablePublicationPlan,
	cause error,
	started time.Time,
) (bool, error) {
	releasePlansErr := planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
	settlement, releaseClaimErr := owner.ReleaseFanOutRetryable(context.WithoutCancel(ctx), FanOutRetryableRelease{
		Claim: claim, Now: time.Now().UTC(), ObservedDuration: time.Since(started),
		Failure: runtimefailures.Normalize(cause, "runtime.fan_out", "prepare_chunk"),
	})
	return settlement.Acknowledged, errors.Join(cause, releasePlansErr, releaseClaimErr)
}

func fanOutBlockedTurnCause(stage string, ordinal int, intent fanoutobligation.Intent, cause error) error {
	if _, ok := runtimefailures.As(cause); ok {
		return cause
	}
	attributes := map[string]any{
		"run_id":                 intent.Request.Key.RunID,
		"triggering_delivery_id": intent.Request.Key.TriggeringDeliveryID,
		"flow_path":              intent.Request.Key.ElementRef.FlowPath,
		"family":                 intent.Request.Key.ElementRef.Family,
		"semantic_path":          intent.Request.Key.ElementRef.SemanticPath,
		"cursor":                 intent.Cursor,
		"cause":                  cause.Error(),
	}
	if ordinal >= 0 {
		attributes["ordinal"] = ordinal
	}
	return runtimefailures.Wrap(
		runtimefailures.ClassInternalFailure,
		"fan_out_"+stage+"_failed",
		"runtime.fan_out",
		stage,
		attributes,
		cause,
	)
}

func (pc *PipelineCoordinator) commitFanOutRange(
	ctx context.Context,
	owner FanOutObligationOwner,
	planner FanOutPublicationPlanner,
	group runtimepipelineobligation.PublicationGroup,
	claim fanoutobligation.Claim,
	outcomes []FanOutChunkOutcome,
	now time.Time,
) (CommittedFanOutChunk, fanOutTurnDisposition, error) {
	if len(outcomes) == 0 {
		return CommittedFanOutChunk{}, fanOutTurnAwaitScan, errors.New("fan-out publication attempt requires ordinal outcomes")
	}
	plans := make([]runtimeengine.DurablePublicationPlan, 0, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.Publication != nil {
			plans = append(plans, outcome.Publication)
		}
	}
	if err := planner.SealFanOutPublications(ctx, group, outcomes[len(outcomes)-1].Ordinal+1, plans); err != nil {
		return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
	}
	started := time.Now()
	committed, err := owner.CommitFanOutChunk(ctx, FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: now})
	if err == nil {
		return committed, fanOutTurnCommitted, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return CommittedFanOutChunk{}, fanOutTurnYielded, errors.Join(err, releaseFanOutOutcomePlans(ctx, planner, outcomes))
	}
	if _, ok := FanOutSafeAggregateFailure(err); ok {
		if len(outcomes) == 1 {
			return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
		}
		midpoint := len(outcomes) / 2
		if releaseErr := releaseFanOutOutcomePlans(ctx, planner, outcomes[midpoint:]); releaseErr != nil {
			return CommittedFanOutChunk{}, fanOutTurnAwaitScan, errors.Join(err, releaseErr, releaseFanOutOutcomePlans(ctx, planner, outcomes[:midpoint]))
		}
		return pc.commitFanOutRange(ctx, owner, planner, group, claim, outcomes[:midpoint], now)
	}
	failure := runtimefailures.Normalize(err, "runtime.fan_out", "commit_chunk")
	if failure.Class == runtimefailures.ClassOutcomeUncertain {
		return CommittedFanOutChunk{}, fanOutTurnUncertain, err
	}
	if failure.Retryable {
		settlement, releaseErr := owner.ReleaseFanOutRetryable(context.WithoutCancel(ctx), FanOutRetryableRelease{
			Claim: claim, Now: time.Now().UTC(), ObservedDuration: time.Since(started),
			Failure: failure,
		})
		disposition := fanOutTurnAwaitScan
		if settlement.Acknowledged {
			disposition = fanOutTurnRetryWait
		}
		return CommittedFanOutChunk{}, disposition, errors.Join(
			err,
			releaseFanOutOutcomePlans(ctx, planner, outcomes),
			releaseErr,
		)
	}
	return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
}

func (pc *PipelineCoordinator) blockFanOutCommit(ctx context.Context, owner FanOutObligationOwner, planner EnginePublicationPlanner, claim fanoutobligation.Claim, outcomes []FanOutChunkOutcome, cause error, now time.Time) (CommittedFanOutChunk, fanOutTurnDisposition, error) {
	if _, typed := runtimefailures.As(cause); !typed {
		cause = runtimefailures.Wrap(runtimefailures.ClassInternalFailure, "fan_out_commit_failed", "runtime.fan_out", "commit_chunk", map[string]any{
			"run_id": claim.Key.RunID, "triggering_delivery_id": claim.Key.TriggeringDeliveryID,
			"flow_path": claim.Key.ElementRef.FlowPath, "cause": cause.Error(),
		}, cause)
	}
	failure := runtimefailures.Normalize(cause, "runtime.fan_out", "blocked_commit")
	settlement, blockErr := owner.BlockFanOutClaim(context.WithoutCancel(ctx), FanOutBlockRequest{Claim: claim, Now: now, Failure: failure})
	disposition := fanOutTurnAwaitScan
	if settlement.Acknowledged {
		disposition = fanOutTurnBlocked
	}
	return CommittedFanOutChunk{}, disposition, errors.Join(cause, releaseFanOutOutcomePlans(ctx, planner, outcomes), blockErr)
}

func releaseFanOutOutcomePlans(ctx context.Context, planner EnginePublicationPlanner, outcomes []FanOutChunkOutcome) error {
	plans := make([]runtimeengine.DurablePublicationPlan, 0, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.Publication != nil {
			plans = append(plans, outcome.Publication)
		}
	}
	return planner.ReleaseEnginePublications(context.WithoutCancel(ctx), plans)
}

type fanOutFailureDisposition uint8

const (
	fanOutFailureBlock fanOutFailureDisposition = iota + 1
	fanOutFailureYield
	fanOutFailureRetry
	fanOutFailureItemSemantic
)

func fanOutPrecommitFailure(err error) (json.RawMessage, fanOutFailureDisposition) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, fanOutFailureYield
	}
	failure := runtimeengine.NormalizeFailure(err, "runtime.fan_out", "issue_ordinal")
	if failure == nil {
		return nil, fanOutFailureBlock
	}
	if failure.Failure.Class == runtimefailures.ClassInternalFailure || failure.Failure.Class == runtimefailures.ClassOutcomeUncertain {
		return nil, fanOutFailureBlock
	}
	if failure.Failure.Retryable {
		return nil, fanOutFailureRetry
	}
	if !runtimeengine.IsEmitPayloadContractFailure(err) {
		return nil, fanOutFailureBlock
	}
	raw, marshalErr := runtimefailures.MarshalEnvelope(failure.Failure)
	if marshalErr != nil {
		return nil, fanOutFailureBlock
	}
	return raw, fanOutFailureItemSemantic
}

func plannerReleaseFanOutPlans(ctx context.Context, bus Bus, plans []runtimeengine.DurablePublicationPlan) error {
	if len(plans) == 0 {
		return nil
	}
	planner, ok := bus.(EnginePublicationPlanner)
	if !ok {
		return fmt.Errorf("fan-out pump requires the canonical engine publication planner")
	}
	return planner.ReleaseEnginePublications(ctx, plans)
}
