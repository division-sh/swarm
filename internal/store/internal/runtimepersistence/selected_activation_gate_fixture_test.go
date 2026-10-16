package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type selectedStoreActivationBoundary struct {
	runforkexecution.SelectedContractForkLifecycle
	original runfork.RunForkSelectedContractExecutionActivateRequest
	binding  runfork.RunForkSelectedContractBinding
	point    runfork.RunForkPoint
	call     func(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
}

func (p *selectedStoreActivationBoundary) PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	if req.SourceRunID != p.binding.SourceRunID || req.At != p.point.Input || req.AtStart != (p.point.Kind == runfork.RunForkPointRunStart) {
		return runfork.RunForkPlan{}, fmt.Errorf("fixture preparation changed the original selected source or selector")
	}
	point := p.point
	req.ResolvedPoint = &point
	return p.SelectedContractForkLifecycle.PlanRunFork(ctx, req)
}

func (p *selectedStoreActivationBoundary) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	return p.SelectedContractForkLifecycle.RequireMaterializedRunForkForSelectedContractExecution(ctx, p.original.ForkRunID, req)
}

func (p *selectedStoreActivationBoundary) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	return p.call(ctx, req)
}

// The decorator observes only the real native call after staged preparation and
// attachment. It cannot supply an execution authority or a replacement child.
func selectedStoreActivationGateForTest(t *testing.T, ctx context.Context, selected any) func(runfork.RunForkSelectedContractExecutionActivateRequest, runfork.RunForkPoint, func(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)) (runfork.RunForkActivation, error) {
	t.Helper()
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	ctx = worklifetime.WithProcess(ctx, work.process)
	capability := selectedMaterializationProcessForTest(t, work, selected)
	probe := &selectedStoreActivationBoundary{SelectedContractForkLifecycle: selected.(runforkexecution.SelectedContractForkLifecycle)}
	owner := selectedStorePreparationOwnerWithForkForTest(t, selected, probe)
	if err := owner.BindSelectedProcess(ctx, work.process, capability); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot: repo, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
		Store: selected.(runforkexecution.SourceArtifactSelectedContractSourceStore),
	}
	return func(original runfork.RunForkSelectedContractExecutionActivateRequest, point runfork.RunForkPoint, call func(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)) (runfork.RunForkActivation, error) {
		binding, err := probe.RequireRunForkSelectedContractBinding(ctx, original.ForkRunID)
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		if err := point.Validate(); err != nil || point.Kind != binding.ForkPoint.Kind || point.Revision != binding.ForkPoint.Revision || point.EventID != binding.ForkPoint.EventID {
			return runfork.RunForkActivation{}, fmt.Errorf("fixture lost the original full fixed point: %v", err)
		}
		probe.binding, probe.original, probe.point = binding, original, point
		reached := false
		probe.call = func(actualCtx context.Context, actual runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
			if reached || actual.ForkRunID != original.ForkRunID {
				return runfork.RunForkActivation{}, fmt.Errorf("staged activation changed the original child or called twice")
			}
			reached = true
			actual.ForkOperation, actual.DataPins = original.ForkOperation, original.DataPins
			return call(actualCtx, actual)
		}
		request := runforkexecution.SelectedContractExecutionRequest{
			SourceRunID: binding.SourceRunID, At: point.Input, AtStart: point.Kind == runfork.RunForkPointRunStart,
			AllowSourceFreeze: original.AllowSourceFreeze, Owner: owner, SourceLoader: loader, ContractSelection: binding.ContractSelection,
			AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
				ProcessCapability: capability, ExecutionPosture: executionposture.Live,
				Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
			},
		}
		if original.ForkOperation != nil {
			request.DataPinOverrides = original.ForkOperation.DataPinOverrides
		}
		result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, request)
		err = errors.Join(err, owner.RetireSelectedContexts(context.Background()))
		if !reached {
			return result.Activation, fmt.Errorf("original-child execution did not reach the prepared native activation: %w", err)
		}
		return result.Activation, err
	}
}
