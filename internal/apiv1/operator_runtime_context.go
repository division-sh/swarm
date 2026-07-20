package apiv1

import (
	"context"
	"errors"
	"strings"
	"sync"

	swruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunforkadmission "github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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

func operatorOptionsForBundleContext(opts OperatorReadOptions, contextDef *swruntime.BundleContext, selectedRuntime *swruntime.Runtime) OperatorReadOptions {
	if contextDef == nil || selectedRuntime == nil {
		return opts
	}
	selected := opts
	selected.Source = contextDef.Source
	selected.Bundle = contextDef.BundleIdentity
	selected.Events = selectedRuntime.Bus
	selected.RuntimeIngress = selectedRuntime.RuntimeIngress
	selected.RunControl = selectedRuntime.RunControl
	if selectedRuntime.Manager != nil {
		selected.AgentControl = selectedRuntime.Manager
	}
	selected.RunFork = runForkExecutorForBundleContext(selected.RunFork, contextDef, selectedRuntime)
	return selected
}

func runForkExecutorForBundleContext(executor RunForkExecutor, contextDef *swruntime.BundleContext, selectedRuntime *swruntime.Runtime) RunForkExecutor {
	if contextDef == nil || selectedRuntime == nil || executor == nil {
		return executor
	}
	apply := func(selected SelectedContractRunForkExecutor) SelectedContractRunForkExecutor {
		selected.AgentRuntime.Config = selectedRuntime.Config
		selected.AgentRuntime.EntityStore = selectedRuntime.Stores.ToolEntityStore
		selected.AgentRuntime.HumanTaskStore = selectedRuntime.Stores.HumanTaskStore
		selected.AgentRuntime.SessionRegistry = selectedRuntime.Stores.SessionRegistry
		selected.AgentRuntime.ConversationStore = selectedRuntime.Stores.ConversationStore
		selected.AgentRuntime.ScheduleStore = selectedRuntime.Stores.ScheduleStore
		selected.AgentRuntime.MailboxStore = selectedRuntime.Stores.MailboxStore
		selected.AgentRuntime.Workspace = selectedRuntime.Workspace
		selected.AgentRuntime.Credentials = selectedRuntime.Credentials
		selected.AgentRuntime.LLMRuntime = selectedRuntime.LLM
		selected.ContractSelection = runtimerunforkadmission.SelectedContractSelection(contextDef.Source, contextDef.ContractsRoot)
		return selected
	}
	switch typed := executor.(type) {
	case SelectedContractRunForkExecutor:
		return apply(typed)
	case *SelectedContractRunForkExecutor:
		if typed == nil {
			return executor
		}
		copied := *typed
		selected := apply(copied)
		return selected
	default:
		return executor
	}
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
	use, lookup, acquireErr := manager.AcquireBundleHash(ctx, bundleHash)
	transferred := false
	defer func() {
		if use != nil && !transferred {
			_ = use.Done()
		}
	}()
	if !lookup.Loaded() {
		return ctx, opts, nil, NewApplicationError(BundleUnavailableCode, false, map[string]any{
			"bundle_hash": bundleHash,
			"run_id":      strings.TrimSpace(runID),
			"cause":       runtimeContextLookupCause(lookup),
		})
	}
	if acquireErr != nil {
		return ctx, opts, nil, acquireErr
	}
	if err := registerRuntimeUse(ctx, use); err != nil {
		return ctx, opts, nil, err
	}
	transferred = true
	ctx = use.WorkContext()
	contextDef := &use.Context
	selectedRuntime := use.Runtime()
	selected := operatorOptionsForBundleContext(opts, contextDef, selectedRuntime)
	fact := contextDef.BundleSourceFact.Normalized()
	if fact.BundleHash == "" {
		fact.BundleHash = bundleHash
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	if selectedRuntime != nil {
		runtimeInstanceID := selectedRuntime.Options.RuntimeInstanceID
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash))
	}
	return ctx, selected, contextDef, nil
}

func runtimeBundleContextByRun(ctx context.Context, opts OperatorReadOptions, runID string) (context.Context, OperatorReadOptions, runbundle.Availability, error) {
	manager := runtimeContextManager(opts)
	if manager == nil {
		return ctx, opts, runbundle.Availability{}, nil
	}
	use, lookup, availability, err := manager.AcquireRun(ctx, strings.TrimSpace(runID))
	transferred := false
	defer func() {
		if use != nil && !transferred {
			_ = use.Done()
		}
	}()
	if errors.Is(err, store.ErrRunNotFound) {
		return ctx, opts, runbundle.Availability{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": strings.TrimSpace(runID)})
	}
	if err != nil {
		return ctx, opts, runbundle.Availability{}, err
	}
	if availability.ErrorCode == BundleDataIntegrityErrorCode {
		return ctx, opts, availability, NewApplicationError(BundleDataIntegrityErrorCode, false, bundleAvailabilityDetails(availability))
	}
	loadedEphemeral := lookup.Loaded() && availability.BundleSource == storerunlifecycle.BundleSourceEphemeral && strings.TrimSpace(availability.BundleHash) != ""
	if !availability.Available() && !loadedEphemeral {
		return ctx, opts, availability, NewApplicationError(BundleUnavailableCode, false, bundleAvailabilityDetails(availability))
	}
	if !lookup.Loaded() {
		details := bundleAvailabilityDetails(availability)
		details["cause"] = runtimeContextLookupCause(lookup)
		return ctx, opts, availability, NewApplicationError(BundleUnavailableCode, false, details)
	}
	if err := registerRuntimeUse(ctx, use); err != nil {
		return ctx, opts, availability, err
	}
	transferred = true
	ctx = use.WorkContext()
	contextDef := &use.Context
	selectedRuntime := use.Runtime()
	selected := operatorOptionsForBundleContext(opts, contextDef, selectedRuntime)
	fact := contextDef.BundleSourceFact.Normalized()
	if fact.BundleHash == "" {
		fact.BundleHash = availability.BundleHash
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	if selectedRuntime != nil {
		runtimeInstanceID := selectedRuntime.Options.RuntimeInstanceID
		ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash))
	}
	return ctx, selected, availability, nil
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
