package apiv1

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func bindPrivateScenarioPublicationScope(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, identity bundleIdentityParam, rootInput bool) (context.Context, EventPublicationOptions, eventPublicationParams, bool, error) {
	if !rootInput || !scenarioexecution.HasPrivateHostScope(ctx) || params.RunID == "" {
		return ctx, opts, params, false, nil
	}
	ctx, opts, params, err := resolveEventPublicationBundleScope(ctx, opts, params, identity, eventPublicationConfig{requireExistingExplicitRun: true})
	if err != nil {
		return ctx, opts, params, false, err
	}
	ctx, err = admitScenarioExecutionSelector(ctx, opts, params.RunID, false, params.ScenarioExecution)
	if err != nil {
		return ctx, opts, params, false, err
	}
	header, err := opts.Runs.LoadRunHeader(ctx, params.RunID)
	if err != nil || header.Origin.Kind() == runlifecycle.OriginDeployment {
		return ctx, opts, params, false, err
	}
	ctx, err = scenarioexecution.WithPrivateRunScope(ctx, params.RunID, flowidentity.ScopeKey(opts.Source, semanticview.RootExecutionFlowID(opts.Source)))
	return ctx, opts, params, err == nil, err
}

// StartPrivateScenario is handed directly to swarm test by its private host.
// It is deliberately absent from the public RPC method registry.
func StartPrivateScenario(ctx context.Context, opts EventPublicationOptions, input map[string]any, now time.Time) (string, error) {
	if !scenarioexecution.HasPrivateHostScope(ctx) {
		return "", fmt.Errorf("scenario start requires its private test host")
	}
	ctx, uses := beginRuntimeUseCollection(ctx)
	defer uses.done()
	params := make(map[string]any, len(input)+1)
	for key, value := range input {
		params[key] = value
	}
	if stringParam(params, "run_id") != "" {
		return "", fmt.Errorf("scenario start requires an empty run context")
	}
	params["run_id"] = uuid.NewString()
	cfg := eventPublicationConfig{
		sourceAgent:   func(Request) string { return "api.v1" },
		rootInputOnly: true, injectRunIDEntityIDWhenMissing: true,
	}
	publication, identity, err := eventPublicationParamsFromRequest(Request{Method: "test.scenario.start", Params: params}, cfg)
	if err != nil {
		return "", err
	}
	if publication.DataPresent {
		return "", fmt.Errorf("scenario start does not admit run data bindings")
	}
	// This UUID was minted by the private start, not supplied by a public caller.
	publication.RunIDProvided = false
	ctx, opts, publication, err = resolveEventPublicationBundleScope(ctx, opts, publication, identity, cfg)
	if err != nil {
		return "", err
	}
	ctx, err = admitScenarioExecutionSelector(ctx, opts, publication.RunID, true, publication.ScenarioExecution)
	if err != nil {
		return "", err
	}
	profile, admitted := scenarioexecution.AdmittedProfileFromContext(ctx)
	publication.EventName, err = resolveEventPublicationEventName(opts.Source, publication.EventName)
	if err != nil {
		return "", err
	}
	if _, err := validateEventPublication(ctx, opts, publication, cfg); err != nil {
		return "", err
	}
	if _, err := checkEventPublicationRecipientPlan(ctx, opts, publication, cfg); err != nil {
		return "", err
	}
	ctx, err = scenarioexecution.WithPrivateRunScope(ctx, publication.RunID, flowidentity.ScopeKey(opts.Source, semanticview.RootExecutionFlowID(opts.Source)))
	if err != nil {
		return "", err
	}
	owner, ok := opts.Acknowledged.(TestSetupStore)
	if !ok || owner == nil {
		return "", fmt.Errorf("scenario start requires its native setup owner")
	}
	initialEventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.run.initial-event.v1\x00"+publication.RunID)).String()
	origin, err := runlifecycle.EventRunOrigin(initialEventID, publication.EventName)
	if err != nil {
		return "", err
	}
	request := pipeline.ScenarioSetupRequest{RunID: publication.RunID, CreatedAt: now, Origin: origin}
	if admitted {
		request.ScenarioExecutionProfile = &profile
	}
	result, err := owner.SetupScenarioEntities(ctx, request)
	if err != nil {
		return "", err
	}
	if !result.Acknowledged || result.RunID != publication.RunID {
		return "", fmt.Errorf("scenario start did not acknowledge its exact run")
	}
	return result.RunID, nil
}

func scenarioExecutionSelectorParam(params map[string]any) (*scenarioexecution.Selector, error) {
	raw, ok := params["scenario_execution"]
	if !ok || raw == nil {
		return nil, nil
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "scenario_execution", "reason": "must be an object"})
	}
	for key := range object {
		switch key {
		case "profile_id", "profile_digest", "effective_source_digest":
		default:
			return nil, NewInvalidParamsError(map[string]any{"field": "scenario_execution." + key, "reason": "unknown field"})
		}
	}
	profileID, err := requiredStringParam(object, "profile_id")
	if err != nil {
		return nil, err
	}
	profileDigest, err := requiredStringParam(object, "profile_digest")
	if err != nil {
		return nil, err
	}
	effectiveDigest, err := requiredStringParam(object, "effective_source_digest")
	if err != nil {
		return nil, err
	}
	selector := scenarioexecution.Selector{
		ProfileID: profileID, ProfileDigest: profileDigest, EffectiveSourceDigest: effectiveDigest,
	}
	if err := selector.Validate(); err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": "scenario_execution", "reason": err.Error()})
	}
	return &selector, nil
}

func admitScenarioExecutionSelector(ctx context.Context, opts EventPublicationOptions, runID string, createsRun bool, selector *scenarioexecution.Selector) (context.Context, error) {
	var persisted scenarioexecution.Profile
	var persistedFound bool
	if !createsRun {
		if opts.EffectiveSourceIdentity.Validate() != nil && opts.ScenarioProfileCatalog == nil {
			return ctx, nil
		}
		if opts.ScenarioExecutionProfiles == nil {
			return ctx, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
				"cause": "scenario_execution_profile_reader_unavailable",
			})
		}
		var err error
		persisted, persistedFound, err = opts.ScenarioExecutionProfiles.LoadScenarioExecutionProfile(ctx, strings.TrimSpace(runID))
		if err != nil {
			return ctx, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
				"cause": "scenario_execution_profile_read_failed", "run_id": strings.TrimSpace(runID),
			})
		}
	}
	if selector == nil {
		if !persistedFound {
			return ctx, nil
		}
		return admitPersistedScenarioExecutionProfile(ctx, opts, strings.TrimSpace(runID), persisted)
	}
	if opts.ExecutionPosture != executionposture.MockOnly {
		return ctx, NewInvalidParamsError(map[string]any{
			"field": "scenario_execution", "reason": "scenario execution profiles require command-selected mock execution",
		})
	}
	if opts.ScenarioProfileCatalog == nil || opts.EffectiveSourceIdentity.Validate() != nil {
		return ctx, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"cause": "scenario_execution_catalog_unavailable",
		})
	}
	if strings.TrimSpace(selector.EffectiveSourceDigest) != opts.EffectiveSourceIdentity.Digest() {
		return ctx, NewApplicationError(BundleMismatchCode, false, map[string]any{
			"cause":     "scenario_execution_effective_source_mismatch",
			"requested": selector.EffectiveSourceDigest, "runtime": opts.EffectiveSourceIdentity.Digest(),
		})
	}
	profile, err := opts.ScenarioProfileCatalog.Resolve(*selector)
	if err != nil {
		return ctx, NewInvalidParamsError(map[string]any{"field": "scenario_execution", "reason": err.Error()})
	}
	if !createsRun {
		if !persistedFound {
			return ctx, NewInvalidParamsError(map[string]any{
				"field": "scenario_execution", "reason": fmt.Sprintf("existing run %s cannot install a scenario execution profile", strings.TrimSpace(runID)),
			})
		}
		if persisted.ID() != profile.ID() || persisted.Digest() != profile.Digest() ||
			!persisted.EffectiveSourceIdentity().Equal(profile.EffectiveSourceIdentity()) ||
			!bytes.Equal(persisted.CanonicalBytes(), profile.CanonicalBytes()) {
			return ctx, NewInvalidParamsError(map[string]any{
				"field": "scenario_execution", "reason": fmt.Sprintf("run %s already has a different scenario execution profile", strings.TrimSpace(runID)),
			})
		}
	}
	admitted, err := scenarioexecution.WithAdmittedProfile(ctx, profile)
	if err != nil {
		return ctx, fmt.Errorf("admit scenario execution profile: %w", err)
	}
	return admitted, nil
}

func admitPersistedScenarioExecutionProfile(ctx context.Context, opts EventPublicationOptions, runID string, profile scenarioexecution.Profile) (context.Context, error) {
	if opts.ExecutionPosture != executionposture.MockOnly {
		return ctx, NewApplicationError(BundleMismatchCode, false, map[string]any{
			"cause": "scenario_execution_runtime_posture_mismatch", "run_id": runID,
		})
	}
	if err := opts.EffectiveSourceIdentity.Validate(); err != nil {
		return ctx, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"cause": "scenario_execution_effective_source_unavailable", "run_id": runID,
		})
	}
	if !profile.EffectiveSourceIdentity().Equal(opts.EffectiveSourceIdentity) {
		return ctx, NewApplicationError(BundleMismatchCode, false, map[string]any{
			"cause": "scenario_execution_persisted_runtime_mismatch", "run_id": runID,
			"persisted": profile.EffectiveSourceIdentity().Digest(), "runtime": opts.EffectiveSourceIdentity.Digest(),
		})
	}
	admitted, err := scenarioexecution.WithAdmittedProfile(ctx, profile)
	if err != nil {
		return ctx, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"cause": "scenario_execution_profile_invalid", "run_id": runID,
		})
	}
	return admitted, nil
}
