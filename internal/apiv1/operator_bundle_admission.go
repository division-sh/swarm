package apiv1

import (
	"context"
	"errors"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

type RunBundleContextStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
}

type runtimeBundleSourceFactProvider interface {
	WithBundleFingerprint(context.Context) context.Context
}

func resolveEventPublicationBundleScope(
	ctx context.Context,
	opts OperatorReadOptions,
	params eventPublicationParams,
	identity bundleIdentityParam,
	cfg eventPublicationConfig,
) (context.Context, OperatorReadOptions, eventPublicationParams, error) {
	requestedHash := strings.TrimSpace(identity.BundleHash)
	runAvailability, hasRunContext, err := eventPublicationRunBundleContext(ctx, opts, params, cfg)
	if err != nil {
		return ctx, opts, params, err
	}

	resolvedHash := requestedHash
	if hasRunContext {
		if requestedHash != "" && runAvailability.BundleHash != "" && requestedHash != runAvailability.BundleHash {
			return ctx, opts, params, NewApplicationError(BundleMismatchCode, false, bundleMismatchDetails(params.RunID, requestedHash, runAvailability))
		}
		if runtimeContextManager(opts) != nil {
			var selectedOpts OperatorReadOptions
			ctx, selectedOpts, runAvailability, err = runtimeBundleContextByRun(ctx, opts, params.RunID)
			if err != nil {
				return ctx, opts, params, err
			}
			currentFact, _ := runtimecorrelation.BundleSourceFactFromContext(ctx)
			currentFact = currentFact.Normalized()
			params.BundleHash = runAvailability.BundleHash
			params.BundleSource = currentFact.BundleSource
			params.BundleFingerprint = currentFact.BundleFingerprint
			return ctx, selectedOpts, params, nil
		}
		if err := eventPublicationRunBundleAvailable(runAvailability); err != nil {
			return ctx, opts, params, err
		}
		resolvedHash = runAvailability.BundleHash
		params.BundleSource = runAvailability.BundleSource
	}
	if resolvedHash == "" && identity.LegacyFingerprint == "" && !hasRunContext && runtimeContextManager(opts) == nil {
		currentFact, hasCurrentFact := eventPublicationRuntimeSourceFact(ctx, opts.Events)
		currentFact = currentFact.Normalized()
		if hasCurrentFact &&
			currentFact.BundleHash != "" &&
			currentFact.BundleSource == storerunlifecycle.BundleSourceEphemeral &&
			currentFact.BundleFingerprint != "" {
			params.BundleHash = currentFact.BundleHash
			params.BundleSource = currentFact.BundleSource
			params.BundleFingerprint = currentFact.BundleFingerprint
			return runtimecorrelation.WithBundleSourceFact(ctx, currentFact), opts, params, nil
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
	if runtimeContextManager(opts) != nil {
		var selectedOpts OperatorReadOptions
		var contextDef any
		ctx, selectedOpts, contextDef, err = runtimeBundleContextByHash(ctx, opts, resolvedHash, params.RunID)
		if err != nil {
			return ctx, opts, params, err
		}
		if contextDef == nil {
			return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
				"bundle_hash": resolvedHash,
				"run_id":      strings.TrimSpace(params.RunID),
				"cause":       "runtime_context_not_loaded",
			})
		}
		currentFact, _ := runtimecorrelation.BundleSourceFactFromContext(ctx)
		currentFact = currentFact.Normalized()
		params.BundleHash = resolvedHash
		params.BundleSource = currentFact.BundleSource
		params.BundleFingerprint = currentFact.BundleFingerprint
		return ctx, selectedOpts, params, nil
	}
	currentFact, hasCurrentFact := eventPublicationRuntimeSourceFact(ctx, opts.Events)
	if !hasCurrentFact || strings.TrimSpace(currentFact.BundleHash) == "" {
		return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash": resolvedHash,
			"run_id":      strings.TrimSpace(params.RunID),
			"cause":       "runtime_source_fact_missing",
		})
	}
	currentFact = currentFact.Normalized()
	if currentFact.BundleHash != resolvedHash {
		return ctx, opts, params, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash":        resolvedHash,
			"run_id":             strings.TrimSpace(params.RunID),
			"active_bundle_hash": currentFact.BundleHash,
			"cause":              "single_active_runtime_unavailable",
		})
	}

	params.BundleHash = resolvedHash
	params.BundleSource = currentFact.BundleSource
	params.BundleFingerprint = currentFact.BundleFingerprint
	return runtimecorrelation.WithBundleSourceFact(ctx, currentFact), opts, params, nil
}

func eventPublicationRuntimeSourceFact(ctx context.Context, publisher EventPublisher) (runtimecorrelation.BundleSourceFact, bool) {
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		return fact, true
	}
	provider, ok := publisher.(runtimeBundleSourceFactProvider)
	if !ok || provider == nil {
		return runtimecorrelation.BundleSourceFact{}, false
	}
	enriched := provider.WithBundleFingerprint(ctx)
	return runtimecorrelation.BundleSourceFactFromContext(enriched)
}

func eventPublicationRunBundleContext(
	ctx context.Context,
	opts OperatorReadOptions,
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
	if errors.Is(err, store.ErrRunNotFound) {
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
	source, err := storerunlifecycle.CanonicalBundleSource(availability.BundleSource)
	if err != nil {
		return err
	}
	availability.BundleSource = source
	if availability.ErrorCode == BundleDataIntegrityErrorCode {
		return NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
	}
	switch source {
	case storerunlifecycle.BundleSourcePersisted:
		if availability.BundleHash == "" || !availability.BundleRowPresent {
			if availability.Cause == "" {
				availability.Cause = "persisted_missing_bundle_context"
			}
			return NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
		}
		return nil
	case storerunlifecycle.BundleSourceEphemeral:
		if availability.BundleHash != "" {
			return nil
		}
		if availability.Cause == "" {
			availability.Cause = "ephemeral_missing_hash"
		}
	case storerunlifecycle.BundleSourceDeleted, storerunlifecycle.BundleSourceLegacy:
		if availability.Cause == "" {
			availability.Cause = source
		}
	}
	return NewApplicationError(BundleUnavailableCode, false, bundleAvailabilityDetails(availability))
}

func bundleMismatchDetails(runID, requestedHash string, availability runbundle.Availability) map[string]any {
	return map[string]any{
		"run_id":          strings.TrimSpace(runID),
		"requested_hash":  strings.TrimSpace(requestedHash),
		"run_bundle_hash": strings.TrimSpace(availability.BundleHash),
		"bundle_source":   strings.TrimSpace(availability.BundleSource),
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
	if availability.BundleSource != "" {
		details["bundle_source"] = strings.TrimSpace(availability.BundleSource)
	}
	if availability.BundleFingerprint != "" {
		details["legacy_bundle_fingerprint"] = strings.TrimSpace(availability.BundleFingerprint)
	}
	if availability.Cause != "" {
		details["cause"] = strings.TrimSpace(availability.Cause)
	}
	return details
}
