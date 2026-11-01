package apiv1

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type liveRunRowsPublisher interface {
	PublishDeploymentFeedsAcknowledged(context.Context, durabledata.RunCreationCommand, apiidempotency.Request, runlifecycle.LiveRun) (bus.CommittedDeploymentRunCreation, error)
}

func rowsPublicationID(req Request, request apiidempotency.Request) string {
	if request.IdempotencyKey == "" {
		return uuid.NewString()
	}
	return durabledata.RowsPublicationID(req.ActorTokenID, request.IdempotencyKey)
}

func executeLiveRunRows(ctx context.Context, req Request, opts EventPublicationOptions, now time.Time) (any, error) {
	completion, replay, request, err := lookupPublicPublication(ctx, req, opts, now)
	if err != nil || replay {
		if err != nil {
			return nil, publicationApplicationError("", runStartIdempotencyError(err))
		}
		return recordedPublicCompletionResult(completion)
	}
	data, present, err := runCreationDataEnvelopeParam(req.Method, req.Params)
	if err != nil {
		return nil, err
	}
	if !present || len(data.Imports)+len(data.Pins) == 0 {
		return nil, NewInvalidParamsError(map[string]any{"field": "data", "reason": "rows publication requires at least one import or pin"})
	}
	var scope runlifecycle.LiveRunScope
	for _, item := range appendRowsDeclarations(data) {
		if scope.FlowPath != "" && scope.FlowPath != item.FlowPath {
			return nil, NewInvalidParamsError(map[string]any{"field": "data", "reason": "rows publication must select one flow"})
		}
		scope.FlowPath = item.FlowPath
	}
	runID, provided, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return nil, err
	}
	if provided {
		parsed, err := uuid.Parse(runID)
		if err != nil || parsed == uuid.Nil || parsed.String() != runID {
			return nil, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must be a canonical non-zero UUID"})
		}
	}
	identity, err := bundleIdentityInputParam(req.Params)
	if err != nil {
		return nil, err
	}
	selector, err := scenarioExecutionSelectorParam(req.Params)
	if err != nil {
		return nil, err
	}
	params := eventPublicationParams{RunID: runID, RunIDProvided: provided, Data: data, DataPresent: true, ScenarioExecution: selector}
	ctx, opts, params, _, err = bindPrivateScenarioPublicationScope(ctx, opts, params, identity, true)
	if err != nil {
		return nil, err
	}
	ctx, selected, params, live, err := selectRowsLiveRun(ctx, opts, params, identity, scope, data, runID, provided)
	if err != nil {
		return nil, err
	}
	ctx, err = admitScenarioExecutionSelector(ctx, selected, params.RunID, false, selector)
	if err != nil {
		return nil, err
	}
	owner, ok := selected.Acknowledged.(liveRunRowsPublisher)
	if !ok {
		return nil, fmt.Errorf("rows publication requires its acknowledged native owner")
	}
	command := durabledata.RunCreationCommand{PublicationID: rowsPublicationID(req, request), RunID: params.RunID,
		Actor: req.ActorTokenID, BundleHash: params.SourceArtifactFact.BundleHash(), Data: data}
	committed, err := owner.PublishDeploymentFeedsAcknowledged(ctx, command, request, live)
	if err != nil {
		if errors.Is(err, worklifetime.ErrPublicCompletionCapacity) {
			return nil, NewApplicationError(EventPublishFailedCode, true, map[string]any{"reason": err.Error()})
		}
		return nil, eventCatalogPublishError("", runStartIdempotencyError(err))
	}
	if committed.Record.Summary.Outcome != "created" {
		code := durabledata.CodeRunDataRejected
		if committed.Record.Summary.Outcome == "head_conflict" {
			code = durabledata.CodeRunHeadConflict
		}
		return nil, dataApplicationError(durabledata.NewDomainErrorWithDetails(code,
			map[string]any{"operation": committed.Record.Summary}, "rows publication %s", committed.Record.Summary.Outcome))
	}
	return recordedPublicCompletionResult(committed.Completion)
}

func selectRowsLiveRun(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, identity bundleIdentityParam, scope runlifecycle.LiveRunScope, data durabledata.RunCreationDataEnvelope, runID string, provided bool) (context.Context, EventPublicationOptions, eventPublicationParams, runlifecycle.LiveRun, error) {
	if opts.LiveRuns == nil || opts.Runs == nil {
		return ctx, opts, params, runlifecycle.LiveRun{}, fmt.Errorf("rows publication requires native live-run readers")
	}
	live, found, err := opts.LiveRuns.LoadLiveRun(ctx, scope)
	if err != nil {
		return ctx, opts, params, live, err
	}
	if !found {
		return ctx, opts, params, live, eventPublicationNoLiveRun(ctx, opts.Runs, scope, "")
	}
	if provided && runID != live.RunID {
		return ctx, opts, params, live, NewApplicationError(EventPublishRejectedCode, false, map[string]any{"run_id": runID, "live_run_id": live.RunID, "reason": "live_run_mismatch"})
	}
	params.RunID, params.RunIDProvided, params.LiveRun = live.RunID, true, &live
	ctx, selected, params, err := resolveEventPublicationBundleScope(ctx, opts, params, identity, eventPublicationConfig{rootInputOnly: true, requireExistingExplicitRun: true})
	if err != nil {
		return ctx, selected, params, live, err
	}
	if _, err := admitRunStartDeploymentFeeds(selected.Source, data); err != nil {
		return ctx, selected, params, live, err
	}
	return ctx, selected, params, live, nil
}

func appendRowsDeclarations(data durabledata.RunCreationDataEnvelope) []durabledata.DeclarationRef {
	refs := make([]durabledata.DeclarationRef, 0, len(data.Imports)+len(data.Pins))
	for _, item := range data.Imports {
		refs = append(refs, item.Declaration)
	}
	for _, item := range data.Pins {
		refs = append(refs, item.Declaration)
	}
	return refs
}
