package apiv1

import (
	"context"
	"errors"
	"strings"
	"sync"

	swruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

func runtimeContextManager(manager *swruntime.RuntimeContextManager) *swruntime.RuntimeContextManager {
	if manager == nil || manager.Len() == 0 {
		return nil
	}
	return manager
}

func multiRuntimeContextMode(contexts *swruntime.RuntimeContextManager) bool {
	manager := runtimeContextManager(contexts)
	return manager != nil && manager.MultiContext()
}

type selectedRuntimeContext struct {
	BundleContext *swruntime.BundleContext
	Runtime       *swruntime.Runtime
}

type runtimeUseCollectionKey struct{}

type runtimeUseCollection struct {
	mu   sync.Mutex
	uses []*swruntime.RuntimeContextUse
}

func beginRuntimeUseCollection(ctx context.Context) (context.Context, *runtimeUseCollection) {
	if ctx == nil {
		ctx = context.Background()
	}
	uses := &runtimeUseCollection{}
	return context.WithValue(ctx, runtimeUseCollectionKey{}, uses), uses
}

func (c *runtimeUseCollection) add(use *swruntime.RuntimeContextUse) error {
	if c == nil || use == nil {
		return errors.New("runtime use collection is required")
	}
	c.mu.Lock()
	c.uses = append(c.uses, use)
	c.mu.Unlock()
	return nil
}

func (c *runtimeUseCollection) done() {
	if c == nil {
		return
	}
	c.mu.Lock()
	uses := append([]*swruntime.RuntimeContextUse(nil), c.uses...)
	c.uses = nil
	c.mu.Unlock()
	for i := len(uses) - 1; i >= 0; i-- {
		_ = uses[i].Done()
	}
}

func registerRuntimeUse(ctx context.Context, use *swruntime.RuntimeContextUse) error {
	collection, ok := ctx.Value(runtimeUseCollectionKey{}).(*runtimeUseCollection)
	if !ok || collection == nil {
		return errors.New("runtime execution requires request-scoped use ownership")
	}
	return collection.add(use)
}

func runtimeBundleContextByHash(ctx context.Context, contexts *swruntime.RuntimeContextManager, bundleHash, runID string) (context.Context, selectedRuntimeContext, error) {
	manager := runtimeContextManager(contexts)
	if manager == nil {
		return ctx, selectedRuntimeContext{}, nil
	}
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return ctx, selectedRuntimeContext{}, NewApplicationError(BundleScopeRequiredCode, false, map[string]any{
			"field":  "bundle_hash",
			"reason": "bundle_hash is required to select a runtime context",
		})
	}
	use, lookup, acquireErr := manager.AcquireBundleHash(ctx, bundleHash)
	transferred := false
	defer func() {
		if use != nil && !transferred {
			_ = use.Done()
		}
	}()
	if !lookup.Loaded() {
		return ctx, selectedRuntimeContext{}, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash": bundleHash,
			"run_id":      strings.TrimSpace(runID),
			"cause":       runtimeContextLookupCause(lookup),
		})
	}
	if acquireErr != nil {
		return ctx, selectedRuntimeContext{}, acquireErr
	}
	if err := registerRuntimeUse(ctx, use); err != nil {
		return ctx, selectedRuntimeContext{}, err
	}
	transferred = true
	ctx = use.WorkContext()
	contextDef := &use.Context
	selectedRuntime := use.Runtime()
	fact := contextDef.BundleSourceFact
	if err := fact.Validate(); err != nil || fact.BundleHash() != bundleHash {
		return ctx, selectedRuntimeContext{}, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"bundle_hash": bundleHash, "cause": "runtime_source_fact_mismatch",
		})
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	if selectedRuntime != nil {
		runtimeInstanceID := selectedRuntime.Options.RuntimeInstanceID
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash()))
	}
	return ctx, selectedRuntimeContext{BundleContext: contextDef, Runtime: selectedRuntime}, nil
}

func runtimeBundleContextByRun(ctx context.Context, contexts *swruntime.RuntimeContextManager, runID string) (context.Context, selectedRuntimeContext, runbundle.Availability, error) {
	manager := runtimeContextManager(contexts)
	if manager == nil {
		return ctx, selectedRuntimeContext{}, runbundle.Availability{}, nil
	}
	use, lookup, availability, err := manager.AcquireRun(ctx, strings.TrimSpace(runID))
	transferred := false
	defer func() {
		if use != nil && !transferred {
			_ = use.Done()
		}
	}()
	if errors.Is(err, runbundle.ErrRunNotFound) {
		return ctx, selectedRuntimeContext{}, runbundle.Availability{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": strings.TrimSpace(runID)})
	}
	if err != nil {
		return ctx, selectedRuntimeContext{}, runbundle.Availability{}, err
	}
	if availability.ErrorCode == BundleDataIntegrityErrorCode {
		return ctx, selectedRuntimeContext{}, availability, NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
	}
	loadedEphemeral := lookup.Loaded() && availability.BundleSource.IsEphemeral() && strings.TrimSpace(availability.BundleHash) != ""
	if !availability.Available() && !loadedEphemeral {
		return ctx, selectedRuntimeContext{}, availability, NewApplicationError(BundleUnavailableCode, false, bundleAvailabilityDetails(availability))
	}
	if !lookup.Loaded() {
		details := bundleAvailabilityDetails(availability)
		details["cause"] = runtimeContextLookupCause(lookup)
		return ctx, selectedRuntimeContext{}, availability, NewApplicationError(BundleUnavailableCode, false, details)
	}
	if err := registerRuntimeUse(ctx, use); err != nil {
		return ctx, selectedRuntimeContext{}, availability, err
	}
	transferred = true
	ctx = use.WorkContext()
	contextDef := &use.Context
	selectedRuntime := use.Runtime()
	fact := contextDef.BundleSourceFact
	runFact, decodeErr := runtimecorrelation.DecodeBundleSourceFact(availability.BundleHash, availability.BundleSource.String())
	if decodeErr != nil || !fact.Matches(runFact) {
		return ctx, selectedRuntimeContext{}, availability, NewApplicationError(BundleDataIntegrityErrorCode, false, map[string]any{
			"run_id": strings.TrimSpace(runID), "bundle_hash": availability.BundleHash, "cause": "runtime_source_fact_mismatch",
		})
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	if selectedRuntime != nil {
		runtimeInstanceID := selectedRuntime.Options.RuntimeInstanceID
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash()))
	}
	return ctx, selectedRuntimeContext{BundleContext: contextDef, Runtime: selectedRuntime}, availability, nil
}

func runtimeContextRequiredError(method, reason string) error {
	return NewApplicationError(BundleScopeRequiredCode, false, map[string]any{
		"method": strings.TrimSpace(method),
		"reason": strings.TrimSpace(reason),
	})
}

func runtimeContextLookupCause(lookup swruntime.RuntimeContextLookup) string {
	if cause := strings.TrimSpace(lookup.Cause); cause != "" {
		return cause
	}
	return swruntime.RuntimeContextCauseNotLoaded
}
