package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

const fanOutClaimLease = 30 * time.Second

// serveFanOutTurn advances at most one intent chunk. Re-entry is signaled when
// work remains, while the store's least-recently-served claim order provides
// cross-intent and cross-process fairness.
func (pc *PipelineCoordinator) serveFanOutTurn(ctx context.Context, now time.Time) (bool, error) {
	if pc == nil || pc.workflowStore == nil || pc.workflowStore.fanOutObligations == nil {
		return false, nil
	}
	if err := pc.sourceArtifactFact.Validate(); err != nil {
		return false, fmt.Errorf("fan-out pump requires exact bundle source fact: %w", err)
	}
	owner := pc.workflowStore.fanOutObligations
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, FanOutClaimRequest{
		Owner: pc.fanOutOwnerID, BundleHash: pc.sourceArtifactFact.BundleHash(), Now: now.UTC(), Lease: fanOutClaimLease,
	})
	if err != nil || !found {
		return false, err
	}
	turnStarted := time.Now()
	release := true
	defer func() {
		if release {
			_ = owner.ReleaseFanOutClaim(context.WithoutCancel(ctx), claim)
		}
	}()
	blockClaim := func(stage string, ordinal int, cause error, plans []runtimeengine.DurablePublicationPlan) error {
		releaseErr := plannerReleaseFanOutPlans(context.WithoutCancel(ctx), pc.bus, plans)
		cause = fanOutBlockedTurnCause(stage, ordinal, intent, cause)
		failure := runtimefailures.Normalize(cause, "runtime.fan_out", stage)
		blockErr := owner.BlockFanOutClaim(context.WithoutCancel(ctx), FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: failure})
		if blockErr == nil {
			release = false
		}
		return errors.Join(cause, releaseErr, blockErr)
	}

	input, err := owner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		return false, blockClaim("load_evaluation", -1, err, nil)
	}
	if err := input.Validate(intent); err != nil {
		return false, blockClaim("validate_evaluation", -1, err, nil)
	}
	planner, ok := pc.bus.(EnginePublicationPlanner)
	if !ok {
		return false, blockClaim("resolve_publication_planner", -1, fmt.Errorf("fan-out pump requires the canonical engine publication planner"), nil)
	}
	executor, err := runtimeengine.NewExecutor(coordinatorEngineDependencies(pc), pipelineEngineEvaluator{evaluator: pc.expressionEval, coordinator: pc})
	if err != nil {
		return false, blockClaim("create_executor", -1, err, nil)
	}
	if intent.Request.PlanRef.BundleHash != pc.sourceArtifactFact.BundleHash() {
		return false, blockClaim("validate_plan_source", -1, fmt.Errorf("fan-out claimed plan bundle disagrees with admitted runtime source"), nil)
	}
	workCtx := runtimecorrelation.WithSourceArtifactFact(ctx, pc.sourceArtifactFact)
	workCtx = runtimecorrelation.WithRunID(workCtx, intent.Request.Key.RunID)
	workCtx = runtimecorrelation.WithInboundEvent(workCtx, input.Trigger)

	end := intent.Cursor + intent.NextChunkSize
	if end > intent.Request.Cardinality {
		end = intent.Request.Cardinality
	}
	outcomes := make([]FanOutChunkOutcome, 0, end-intent.Cursor)
	prepared := make([]runtimeengine.DurablePublicationPlan, 0, end-intent.Cursor)
	for ordinal := intent.Cursor; ordinal < end; ordinal++ {
		emit, evalErr := executor.EvaluateFanOutOrdinal(workCtx, intent, input.Trigger, input.Items[ordinal-input.StartOrdinal], ordinal)
		if evalErr != nil {
			failure, disposition := fanOutPrecommitFailure(evalErr)
			switch disposition {
			case fanOutFailureYield:
				return false, errors.Join(evalErr, planner.ReleaseEnginePublications(context.WithoutCancel(workCtx), prepared))
			case fanOutFailureRetry:
				retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, prepared, evalErr, turnStarted)
				if retryReleased {
					release = false
				}
				return retryReleased, retryErr
			case fanOutFailureBlock:
				return false, blockClaim("evaluate_ordinal", ordinal, evalErr, prepared)
			case fanOutFailureItemSemantic:
			default:
				return false, blockClaim("classify_evaluation_failure", ordinal, fmt.Errorf("fan-out evaluation returned invalid failure disposition"), prepared)
			}
			outcomes = append(outcomes, FanOutChunkOutcome{Ordinal: ordinal, Failure: failure})
			continue
		}
		plans, prepareErr := planner.PrepareEnginePublications(workCtx, []runtimeengine.EmitIntent{emit})
		if prepareErr != nil {
			failure, disposition := fanOutPrecommitFailure(prepareErr)
			switch disposition {
			case fanOutFailureYield:
				return false, errors.Join(prepareErr, planner.ReleaseEnginePublications(context.WithoutCancel(workCtx), prepared))
			case fanOutFailureRetry:
				retryReleased, retryErr := releaseFanOutPrecommitRetry(workCtx, owner, planner, claim, prepared, prepareErr, turnStarted)
				if retryReleased {
					release = false
				}
				return retryReleased, retryErr
			case fanOutFailureBlock:
				return false, blockClaim("prepare_publication", ordinal, prepareErr, prepared)
			case fanOutFailureItemSemantic:
			default:
				return false, blockClaim("classify_publication_failure", ordinal, fmt.Errorf("fan-out planner returned invalid failure disposition"), prepared)
			}
			outcomes = append(outcomes, FanOutChunkOutcome{Ordinal: ordinal, Failure: failure})
			continue
		}
		if len(plans) != 1 {
			return false, blockClaim("validate_publication_cardinality", ordinal, fmt.Errorf("fan-out planner returned %d plans for ordinal %d", len(plans), ordinal), append(prepared, plans...))
		}
		prepared = append(prepared, plans[0])
		outcomes = append(outcomes, FanOutChunkOutcome{Ordinal: ordinal, Publication: plans[0]})
	}

	committed, suppressRelease, err := pc.commitFanOutRange(workCtx, owner, planner, claim, outcomes, now.UTC())
	if suppressRelease {
		release = false
	}
	if err != nil {
		return false, err
	}
	if committed.Intent.ClaimOwner == "" {
		release = false
	}
	if err := planner.FinalizeEnginePublications(workCtx, committed.Publications); err != nil {
		return true, errors.Join(committed.PostCommitFailure, err)
	}
	intents := make([]runtimeengine.EmitIntent, 0, len(committed.Publications))
	for _, publication := range committed.Publications {
		if publication == nil {
			return true, errors.Join(committed.PostCommitFailure, fmt.Errorf("fan-out committed publication evidence is required"))
		}
		intents = append(intents, publication.CommittedDurablePublicationIntent())
	}
	if len(intents) > 0 {
		dispatcher := pc.bus.EngineDispatcher()
		if dispatcher == nil {
			return true, errors.Join(committed.PostCommitFailure, fmt.Errorf("fan-out pump requires the canonical post-commit dispatcher"))
		}
		if err := dispatcher.DispatchPostCommit(context.WithoutCancel(workCtx), intents); err != nil {
			return true, errors.Join(committed.PostCommitFailure, err)
		}
	}
	return committed.Intent.Status == fanoutobligation.StatusOpen, committed.PostCommitFailure
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
	releaseClaimErr := owner.ReleaseFanOutRetryable(context.WithoutCancel(ctx), FanOutRetryableRelease{
		Claim: claim, Now: time.Now().UTC(), ObservedDuration: time.Since(started),
	})
	return releaseClaimErr == nil, errors.Join(cause, releasePlansErr, releaseClaimErr)
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
	planner EnginePublicationPlanner,
	claim fanoutobligation.Claim,
	outcomes []FanOutChunkOutcome,
	now time.Time,
) (CommittedFanOutChunk, bool, error) {
	started := time.Now()
	committed, err := owner.CommitFanOutChunk(ctx, FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: now})
	if err == nil {
		return committed, false, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return CommittedFanOutChunk{}, false, errors.Join(err, releaseFanOutOutcomePlans(ctx, planner, outcomes))
	}
	if item, ok := FanOutItemSemanticFailure(err); ok {
		index := item.Ordinal - outcomes[0].Ordinal
		if index < 0 || index >= len(outcomes) || outcomes[index].Publication == nil {
			return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
		}
		failure, marshalErr := runtimefailures.MarshalEnvelope(item.Failure)
		if marshalErr != nil {
			return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, errors.Join(err, marshalErr), now)
		}
		if releaseErr := releaseFanOutOutcomePlans(ctx, planner, outcomes[index:index+1]); releaseErr != nil {
			return CommittedFanOutChunk{}, false, errors.Join(err, releaseErr)
		}
		outcomes[index] = FanOutChunkOutcome{Ordinal: item.Ordinal, Failure: failure}
		return pc.commitFanOutRange(ctx, owner, planner, claim, outcomes, now)
	}
	if _, ok := FanOutSafeAggregateFailure(err); ok {
		if len(outcomes) == 1 {
			return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
		}
		midpoint := len(outcomes) / 2
		if releaseErr := releaseFanOutOutcomePlans(ctx, planner, outcomes[midpoint:]); releaseErr != nil {
			return CommittedFanOutChunk{}, false, errors.Join(err, releaseErr, releaseFanOutOutcomePlans(ctx, planner, outcomes[:midpoint]))
		}
		return pc.commitFanOutRange(ctx, owner, planner, claim, outcomes[:midpoint], now)
	}
	failure := runtimefailures.Normalize(err, "runtime.fan_out", "commit_chunk")
	if failure.Class == runtimefailures.ClassOutcomeUncertain {
		return CommittedFanOutChunk{}, true, err
	}
	if failure.Retryable {
		releaseErr := owner.ReleaseFanOutRetryable(context.WithoutCancel(ctx), FanOutRetryableRelease{
			Claim: claim, Now: time.Now().UTC(), ObservedDuration: time.Since(started),
		})
		return CommittedFanOutChunk{}, releaseErr == nil, errors.Join(
			err,
			releaseFanOutOutcomePlans(ctx, planner, outcomes),
			releaseErr,
		)
	}
	return pc.blockFanOutCommit(ctx, owner, planner, claim, outcomes, err, now)
}

func (pc *PipelineCoordinator) blockFanOutCommit(ctx context.Context, owner FanOutObligationOwner, planner EnginePublicationPlanner, claim fanoutobligation.Claim, outcomes []FanOutChunkOutcome, cause error, now time.Time) (CommittedFanOutChunk, bool, error) {
	failure := runtimefailures.Normalize(cause, "runtime.fan_out", "blocked_commit")
	blockErr := owner.BlockFanOutClaim(context.WithoutCancel(ctx), FanOutBlockRequest{Claim: claim, Now: now, Failure: failure})
	return CommittedFanOutChunk{}, blockErr == nil, errors.Join(cause, releaseFanOutOutcomePlans(ctx, planner, outcomes), blockErr)
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
	if failure.Failure.Retryable || !failure.Failure.Deterministic {
		return nil, fanOutFailureRetry
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
