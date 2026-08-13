package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

type RunBundleContextStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type BundleSourceAdmitter interface {
	AdmitBundleSourceFact(context.Context) (context.Context, error)
}

func resolveEventPublicationBundleScope(
	ctx context.Context,
	opts EventPublicationOptions,
	params eventPublicationParams,
	identity bundleIdentityParam,
	cfg eventPublicationConfig,
) (context.Context, EventPublicationOptions, eventPublicationParams, error) {
	requestedHash := strings.TrimSpace(identity.BundleHash)
	runAvailability, hasRunContext, err := eventPublicationRunBundleContext(ctx, opts, params, cfg)
	if err != nil {
		return ctx, opts, params, err
	}
	if cfg.rootInputOnly {
		params.NewRunCreated = !hasRunContext
	}

	resolvedHash := requestedHash
	if hasRunContext {
		if requestedHash != "" && runAvailability.BundleHash != "" && requestedHash != runAvailability.BundleHash {
			return ctx, opts, params, NewApplicationError(BundleMismatchCode, false, bundleMismatchDetails(params.RunID, requestedHash, runAvailability))
		}
		if runtimeContextManager(opts.RuntimeContexts) != nil {
			var selected selectedRuntimeContext
			ctx, selected, runAvailability, err = runtimeBundleContextByRun(ctx, opts.RuntimeContexts, params.RunID)
			if err != nil {
				return ctx, opts, params, err
			}
			selectedOpts, selectionErr := eventPublicationOptionsForRuntime(opts, selected)
			if selectionErr != nil {
				return ctx, opts, params, selectionErr
			}
			runFact, decodeErr := runtimecorrelation.DecodeBundleSourceFact(runAvailability.BundleHash, runAvailability.BundleSource.String())
			currentFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
			if decodeErr != nil || !ok || !currentFact.Matches(runFact) {
				return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
					"run_id": params.RunID, "bundle_hash": runAvailability.BundleHash, "cause": "runtime_source_fact_mismatch",
				})
			}
			var publisherFact runtimecorrelation.BundleSourceFact
			var hasPublisherFact bool
			ctx, publisherFact, hasPublisherFact, err = eventPublicationRuntimeSourceContext(ctx, selectedOpts.BundleSource)
			if err != nil || !hasPublisherFact || !publisherFact.Matches(runFact) {
				return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
					"run_id": params.RunID, "bundle_hash": runAvailability.BundleHash, "cause": "runtime_source_fact_mismatch",
				})
			}
			params.BundleSourceFact = runFact
			return ctx, selectedOpts, params, nil
		}
		if err := eventPublicationRunBundleAvailable(runAvailability); err != nil {
			return ctx, opts, params, err
		}
		resolvedHash = runAvailability.BundleHash
		currentFact, decodeErr := runtimecorrelation.DecodeBundleSourceFact(runAvailability.BundleHash, runAvailability.BundleSource.String())
		if decodeErr != nil {
			return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(runAvailability))
		}
		params.BundleSourceFact = currentFact
		ctx, err = eventPublicationSourceContext(ctx, currentFact)
		if err != nil {
			return ctx, opts, params, err
		}
		var publisherFact runtimecorrelation.BundleSourceFact
		var hasPublisherFact bool
		ctx, publisherFact, hasPublisherFact, err = eventPublicationRuntimeSourceContext(ctx, opts.BundleSource)
		if err != nil || !hasPublisherFact || !publisherFact.Matches(currentFact) {
			return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
				"run_id": params.RunID, "bundle_hash": runAvailability.BundleHash, "cause": "runtime_source_fact_mismatch",
			})
		}
		return ctx, opts, params, err
	}
	var currentFact runtimecorrelation.BundleSourceFact
	var hasCurrentFact bool
	if runtimeContextManager(opts.RuntimeContexts) == nil {
		ctx, currentFact, hasCurrentFact, err = eventPublicationRuntimeSourceContext(ctx, opts.BundleSource)
		if err != nil {
			return ctx, opts, params, err
		}
		if resolvedHash == "" && hasCurrentFact {
			resolvedHash = currentFact.BundleHash()
		}
	}
	if resolvedHash == "" {
		details := map[string]any{
			"field":  "bundle_hash",
			"reason": "bundle_hash is required when no existing run bundle context is available",
		}
		if params.RunIDProvided {
			details["run_id"] = strings.TrimSpace(params.RunID)
		}
		return ctx, opts, params, NewApplicationError(BundleScopeRequiredCode, false, details)
	}
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		var selected selectedRuntimeContext
		ctx, selected, err = runtimeBundleContextByHash(ctx, opts.RuntimeContexts, resolvedHash, params.RunID)
		if err != nil {
			return ctx, opts, params, err
		}
		if selected.BundleContext == nil {
			return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
				"bundle_hash": resolvedHash,
				"run_id":      strings.TrimSpace(params.RunID),
				"cause":       "runtime_context_not_loaded",
			})
		}
		selectedOpts, selectionErr := eventPublicationOptionsForRuntime(opts, selected)
		if selectionErr != nil {
			return ctx, opts, params, selectionErr
		}
		currentFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
		if !ok {
			return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
				"bundle_hash": resolvedHash, "run_id": strings.TrimSpace(params.RunID), "cause": "runtime_source_fact_mismatch",
			})
		}
		var publisherFact runtimecorrelation.BundleSourceFact
		var hasPublisherFact bool
		ctx, publisherFact, hasPublisherFact, err = eventPublicationRuntimeSourceContext(ctx, selectedOpts.BundleSource)
		if err != nil || !hasPublisherFact || !publisherFact.Matches(currentFact) {
			return ctx, opts, params, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
				"bundle_hash": resolvedHash, "run_id": strings.TrimSpace(params.RunID), "cause": "runtime_source_fact_mismatch",
			})
		}
		params.BundleSourceFact = currentFact
		return ctx, selectedOpts, params, nil
	}
	if !hasCurrentFact {
		return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash": resolvedHash,
			"run_id":      strings.TrimSpace(params.RunID),
			"cause":       "runtime_source_fact_missing",
		})
	}
	if currentFact.BundleHash() != resolvedHash {
		return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash":        resolvedHash,
			"run_id":             strings.TrimSpace(params.RunID),
			"active_bundle_hash": currentFact.BundleHash(),
			"cause":              "single_active_runtime_unavailable",
		})
	}

	params.BundleSourceFact = currentFact
	ctx, err = eventPublicationSourceContext(ctx, currentFact)
	return ctx, opts, params, err
}

func eventPublicationOptionsForRuntime(opts EventPublicationOptions, selected selectedRuntimeContext) (EventPublicationOptions, error) {
	if selected.BundleContext == nil || selected.Runtime == nil || selected.Runtime.Bus == nil {
		return EventPublicationOptions{}, fmt.Errorf("event publication owner is required for the selected runtime")
	}
	opts.Source = selected.BundleContext.Source
	opts.Events = selected.Runtime.Bus
	opts.Acknowledged = selected.Runtime.Bus
	opts.RecipientPlans = selected.Runtime.Bus
	opts.BundleSource = selected.Runtime.Bus
	opts.ExecutionPosture = selected.Runtime.ExecutionPosture
	opts.EffectiveSourceIdentity = selected.Runtime.EffectiveSourceIdentity
	opts.ScenarioProfileCatalog = selected.Runtime.ScenarioProfileCatalog
	return opts, nil
}

func eventPublicationSourceContext(ctx context.Context, fact runtimecorrelation.BundleSourceFact) (context.Context, error) {
	if err := fact.Validate(); err != nil {
		return ctx, fmt.Errorf("resolve event publication bundle source fact: %w", err)
	}
	if current, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok && !current.Matches(fact) {
		return ctx, fmt.Errorf("event publication bundle source fact conflicts with selected run")
	}
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, fact.BundleHash())
	if err != nil {
		return ctx, fmt.Errorf("resolve event publication author activity scope: %w", err)
	}
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, scope.RuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return runtimeauthoractivity.WithScope(ctx, scope), nil
}

func eventPublicationRuntimeSourceContext(ctx context.Context, publisher BundleSourceAdmitter) (context.Context, runtimecorrelation.BundleSourceFact, bool, error) {
	if publisher != nil {
		var err error
		ctx, err = publisher.AdmitBundleSourceFact(ctx)
		if err != nil {
			return ctx, runtimecorrelation.BundleSourceFact{}, false, err
		}
	}
	fact, found := runtimecorrelation.BundleSourceFactFromContext(ctx)
	return ctx, fact, found, nil
}

func eventPublicationRunBundleContext(
	ctx context.Context,
	opts EventPublicationOptions,
	params eventPublicationParams,
	cfg eventPublicationConfig,
) (runbundle.Availability, bool, error) {
	if !params.RunIDProvided {
		return runbundle.Availability{}, false, nil
	}
	reader := opts.RunBundleContext
	if reader == nil {
		return runbundle.Availability{}, false, errors.New("run bundle context store is required")
	}
	availability, err := reader.LoadRunBundleAvailability(ctx, params.RunID)
	if errors.Is(err, runbundle.ErrRunNotFound) {
		if cfg.requireExistingExplicitRun {
			return runbundle.Availability{}, false, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": params.RunID})
		}
		return runbundle.Availability{}, false, nil
	}
	if err != nil {
		return runbundle.Availability{}, false, err
	}
	return availability, true, nil
}

func eventPublicationRunBundleAvailable(availability runbundle.Availability) error {
	if availability.ErrorCode == BundleDataIntegrityErrorCode {
		return NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
	}
	switch {
	case availability.BundleSource.IsPersisted():
		if availability.BundleHash == "" || !availability.BundleRowPresent {
			if availability.Cause == "" {
				availability.Cause = "persisted_missing_bundle_context"
			}
			return NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
		}
		return nil
	case availability.BundleSource.IsEphemeral():
		if availability.BundleHash != "" {
			return nil
		}
		if availability.Cause == "" {
			availability.Cause = "ephemeral_missing_hash"
		}
	case availability.BundleSource.IsDeleted():
		if availability.Cause == "" {
			availability.Cause = availability.BundleSource.String()
		}
	}
	return NewApplicationError(BundleUnavailableCode, false, bundleAvailabilityDetails(availability))
}

func bundleMismatchDetails(runID, requestedHash string, availability runbundle.Availability) map[string]any {
	return map[string]any{
		"run_id":          strings.TrimSpace(runID),
		"requested_hash":  strings.TrimSpace(requestedHash),
		"run_bundle_hash": strings.TrimSpace(availability.BundleHash),
		"bundle_source":   availability.BundleSource.String(),
	}
}

func bundleAvailabilityDetails(availability runbundle.Availability) map[string]any {
	details := map[string]any{}
	if availability.RunID != "" {
		details["run_id"] = strings.TrimSpace(availability.RunID)
	}
	if availability.Status != "" {
		details["status"] = strings.TrimSpace(availability.Status)
	}
	if availability.BundleHash != "" {
		details["bundle_hash"] = strings.TrimSpace(availability.BundleHash)
	}
	if source := availability.BundleSource.String(); source != "" {
		details["bundle_source"] = source
	}
	if availability.Cause != "" {
		details["cause"] = strings.TrimSpace(availability.Cause)
	}
	return details
}
