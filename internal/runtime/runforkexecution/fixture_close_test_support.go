package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type SelectedFixtureCloseInputForTest struct {
	AccountID string `json:"account_id"`
}

type SelectedFixtureCloseResultForTest struct {
	RunID, BindingID, ExecutionID, EventID string
	Generation                             uint64
}

// This finite fixture input consumes the existing selected execution. It does
// not expose a publisher or enable public event.publish for selected children.
func (o SelectedContractExecutionOwner) PublishSelectedFixtureCloseForTest(caller context.Context, child string, input SelectedFixtureCloseInputForTest) (result SelectedFixtureCloseResultForTest, finalErr error) {
	if caller == nil {
		return result, errors.New("fixture close requires its caller")
	}
	if err := caller.Err(); err != nil {
		return result, err
	}
	id, err := uuid.Parse(child)
	if err != nil || id == uuid.Nil || id.String() != child || strings.TrimSpace(input.AccountID) == "" {
		return result, errors.New("fixture close requires an exact child and account")
	}
	ports, err := o.require()
	if err != nil {
		return result, err
	}
	binding, found, err := ports.fork.LoadRunForkSelectedContractBinding(caller, child)
	if err != nil {
		return result, err
	}
	if !found {
		return result, errors.New("fixture close requires a selected child binding")
	}
	if err := validateSelectedContractExecutionBinding(child, binding); err != nil {
		return result, err
	}
	use, err := o.acquireSelectedFixtureCloseForTest(caller, binding)
	if err != nil {
		return result, err
	}
	released := false
	defer func() {
		if !released {
			use.cancel()
			finalErr = errors.Join(finalErr, use.lease.Done())
		}
	}()
	if err := use.grant.ProveCurrent(use.ctx); err != nil {
		return result, err
	}
	source, err := events.NewRootRoutingSource(child)
	if err != nil {
		return result, err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return result, err
	}
	event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{
		Facts: events.EventFacts{ID: uuid.NewString(), Type: "fixture.close.requested",
			Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "selected-fixture-close"},
			Payload:  payload, RoutingSource: source, CreatedAt: time.Now().UTC(), ExecutionMode: use.mode},
		RunID: child,
	})
	if err != nil {
		return result, err
	}
	publishErr := use.bus.PublishAndWait(use.ctx, event)
	cleanupErr := use.lease.Done()
	released = true
	use.cancel()
	if err := errors.Join(publishErr, cleanupErr); err != nil {
		return result, err
	}
	// Terminal completion joins the retained occurrence, so release this input
	// before waiting for its existing owner to finish the authored close.
	select {
	case <-caller.Done():
		return result, context.Cause(caller)
	case <-use.entry.done:
	}
	ports.contexts.mu.Lock()
	closeErr := use.entry.err
	ports.contexts.mu.Unlock()
	if closeErr != nil {
		return result, closeErr
	}
	result = use.result
	result.EventID = event.ID()
	return result, nil
}

type selectedFixtureCloseUseForTest struct {
	ctx    context.Context
	cancel func()
	lease  *worklifetime.Lease
	bus    *runtimebus.EventBus
	grant  startupownership.GenerationGrant
	mode   executionmode.Mode
	entry  *selectedForkContext
	result SelectedFixtureCloseResultForTest
}

func (o SelectedContractExecutionOwner) acquireSelectedFixtureCloseForTest(caller context.Context, binding runfork.RunForkSelectedContractBinding) (selectedFixtureCloseUseForTest, error) {
	contexts := o.ports.contexts
	if contexts == nil {
		return selectedFixtureCloseUseForTest{}, errors.New("fixture close lacks its selected owner")
	}
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || !contexts.recovered {
		return selectedFixtureCloseUseForTest{}, worklifetime.ErrRetired
	}
	var selected *selectedForkContext
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID != binding.ForkRunID {
			continue
		}
		if selected != nil {
			return selectedFixtureCloseUseForTest{}, errors.New("fixture close has ambiguous retained entries")
		}
		selected = entry
	}
	if selected == nil || selected.retiring || selected.closed || selected.done == nil || selected.retained == nil ||
		selected.binding != binding || selected.retained.retainedRuntime == nil {
		return selectedFixtureCloseUseForTest{}, errors.New("fixture close requires its current retained child")
	}
	runtime := selected.retained.retainedRuntime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	owned, eventBus, grant := runtime.controlContext, runtime.bus, runtime.generationGrant
	if owned == nil || eventBus == nil || grant == nil || runtime.timerLifecycle == nil {
		return selectedFixtureCloseUseForTest{}, errors.New("fixture close child is not ready")
	}
	admission, authority, err := selectedContractManagedExecutionAuthority(owned)
	if err != nil {
		return selectedFixtureCloseUseForTest{}, err
	}
	evidence, err := grant.Evidence()
	if err != nil {
		return selectedFixtureCloseUseForTest{}, err
	}
	fork := evidence.SelectedFork
	if evidence.Validate() != nil || evidence.State != startupownership.GrantAdmitted || fork == nil ||
		fork.BindingID != binding.BindingID || fork.ForkRunID != binding.ForkRunID ||
		fork.ExecutionID != authority.SelectedFork.ExecutionID || fork.ExecutionGeneration != authority.SelectedFork.Generation ||
		fork.FenceGeneration != authority.FenceGeneration || fork.ExecutionOwner != authority.ExecutionOwner ||
		fork.AdmissionFingerprint != authority.SelectedFork.AdmissionFingerprint ||
		fork.ContainerPlanFingerprint != authority.SelectedFork.ContainerPlanFingerprint ||
		fork.ActorCensusFingerprint != admission.ActorCensusFingerprint ||
		fork.EffectiveConfigFingerprint != authority.SelectedFork.EffectiveConfigFingerprint || admission.RunID != binding.ForkRunID {
		return selectedFixtureCloseUseForTest{}, errors.New("fixture close retained authority differs from its exact binding")
	}
	occurrence, found := worklifetime.OccurrenceFromContext(owned)
	selectedOccurrence, exact := occurrence.(*worklifetime.SelectedForkOccurrence)
	if !found || !exact || selectedOccurrence.Identity() != (worklifetime.SelectedForkIdentity{
		ExecutionID: authority.SelectedFork.ExecutionID, RunID: binding.ForkRunID, Generation: authority.SelectedFork.Generation,
	}) {
		return selectedFixtureCloseUseForTest{}, errors.New("fixture close lacks its exact selected occurrence")
	}
	ctx, cancel := bindSelectedDecisionCaller(caller, owned)
	lease, err := occurrence.Begin(ctx)
	if err != nil {
		cancel()
		return selectedFixtureCloseUseForTest{}, fmt.Errorf("join fixture close: %w", err)
	}
	return selectedFixtureCloseUseForTest{ctx: lease.Context(), cancel: cancel, lease: lease, bus: eventBus, grant: grant, entry: selected,
		mode: authority.ExecutionMode, result: SelectedFixtureCloseResultForTest{RunID: binding.ForkRunID,
			BindingID: binding.BindingID, ExecutionID: authority.SelectedFork.ExecutionID, Generation: authority.SelectedFork.Generation}}, nil
}
