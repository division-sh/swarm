package apiv1

import (
	"context"
	"errors"
	"strings"

	swruntime "swarm/internal/runtime"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/store"
	"swarm/internal/store/runbundle"
)

func runtimeContextManager(opts OperatorReadOptions) *swruntime.RuntimeContextManager {
	if opts.RuntimeContexts == nil || opts.RuntimeContexts.Len() == 0 {
		return nil
	}
	return opts.RuntimeContexts
}

func multiRuntimeContextMode(opts OperatorReadOptions) bool {
	manager := runtimeContextManager(opts)
	return manager != nil && manager.MultiContext()
}

func operatorOptionsForBundleContext(opts OperatorReadOptions, contextDef *swruntime.BundleContext) OperatorReadOptions {
	if contextDef == nil || contextDef.Runtime == nil {
		return opts
	}
	selected := opts
	selected.Source = contextDef.Source
	selected.Bundle = contextDef.BundleIdentity
	selected.Events = contextDef.Runtime.Bus
	selected.RuntimeIngress = contextDef.Runtime.RuntimeIngress
	selected.RunControl = contextDef.Runtime.RunControl
	if contextDef.Runtime.Manager != nil {
		selected.AgentControl = contextDef.Runtime.Manager
	}
	return selected
}

func runtimeBundleContextByHash(ctx context.Context, opts OperatorReadOptions, bundleHash, runID string) (context.Context, OperatorReadOptions, *swruntime.BundleContext, error) {
	manager := runtimeContextManager(opts)
	if manager == nil {
		return ctx, opts, nil, nil
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return ctx, opts, nil, NewApplicationError(BundleScopeRequiredCode, false, map[string]any{
			"field":  "bundle_hash",
			"reason": "bundle_hash is required to select a runtime context",
		})
	}
	contextDef, ok := manager.LookupBundleHash(bundleHash)
	if !ok || contextDef == nil {
		return ctx, opts, nil, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash": bundleHash,
			"run_id":      strings.TrimSpace(runID),
			"cause":       "runtime_context_not_loaded",
		})
	}
	selected := operatorOptionsForBundleContext(opts, contextDef)
	fact := contextDef.BundleSourceFact.Normalized()
	if fact.BundleHash == "" {
		fact.BundleHash = bundleHash
	}
	return runtimecorrelation.WithBundleSourceFact(ctx, fact), selected, contextDef, nil
}

func runtimeBundleContextByRun(ctx context.Context, opts OperatorReadOptions, runID string) (context.Context, OperatorReadOptions, runbundle.Availability, error) {
	manager := runtimeContextManager(opts)
	if manager == nil {
		return ctx, opts, runbundle.Availability{}, nil
	}
	contextDef, availability, loaded, err := manager.LookupRun(ctx, strings.TrimSpace(runID))
	if errors.Is(err, store.ErrRunNotFound) {
		return ctx, opts, runbundle.Availability{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": strings.TrimSpace(runID)})
	}
	if err != nil {
		return ctx, opts, runbundle.Availability{}, err
	}
	if availability.ErrorCode == BundleDataIntegrityErrorCode {
		return ctx, opts, availability, NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
	}
	if !availability.Available() {
		return ctx, opts, availability, NewApplicationError(BundleUnavailableCode, false, bundleAvailabilityDetails(availability))
	}
	if !loaded || contextDef == nil {
		details := bundleAvailabilityDetails(availability)
		details["cause"] = "runtime_context_not_loaded"
		return ctx, opts, availability, NewApplicationError(BundleUnavailableCode, false, details)
	}
	selected := operatorOptionsForBundleContext(opts, contextDef)
	fact := contextDef.BundleSourceFact.Normalized()
	if fact.BundleHash == "" {
		fact.BundleHash = availability.BundleHash
	}
	return runtimecorrelation.WithBundleSourceFact(ctx, fact), selected, availability, nil
}

func runtimeContextRequiredError(method, reason string) error {
	return NewApplicationError(BundleScopeRequiredCode, false, map[string]any{
		"method": strings.TrimSpace(method),
		"reason": strings.TrimSpace(reason),
	})
}
