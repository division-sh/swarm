package apiv1

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
)

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
			"field": "scenario_execution", "reason": "scenario execution profiles require runtime.execution_posture=mock_only",
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
