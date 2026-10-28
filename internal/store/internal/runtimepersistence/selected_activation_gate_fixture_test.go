package runtimepersistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type selectedStoreFixedPointComponent struct {
	runforkexecution.SelectedContractForkLifecycle
	point runfork.RunForkPoint
}

func (f *selectedStoreFixedPointComponent) PlanRunFork(ctx context.Context, request runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	point := f.point
	request.At, request.AtStart, request.ResolvedPoint = point.Input, point.Kind == runfork.RunForkPointRunStart, &point
	return f.SelectedContractForkLifecycle.PlanRunFork(ctx, request)
}

// These native controls stop at validation, before the ordinary handoff. Their
// selected preparation, execution claim and generation grant are real owners.
func selectedStoreActivationGateForTest(t *testing.T, ctx context.Context, selected any, observed ...runforkexecution.SelectedContractRuntimeExecutionLifecycle) func(runfork.RunForkSelectedContractExecutionActivateRequest, runfork.RunForkPoint, func(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)) (runfork.RunForkActivation, error) {
	t.Helper()
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	ctx = worklifetime.WithProcess(ctx, work.process)
	capability := selectedMaterializationProcessForTest(t, work, selected)
	fixed := &selectedStoreFixedPointComponent{SelectedContractForkLifecycle: selected.(runforkexecution.SelectedContractForkLifecycle)}
	owner := selectedStorePreparationOwnerWithForkForTest(t, selected, fixed, observed...)
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
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: canonicalrouting.RepoRoot(t), Store: selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)}
	return func(original runfork.RunForkSelectedContractExecutionActivateRequest, point runfork.RunForkPoint, call func(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)) (runfork.RunForkActivation, error) {
		binding, err := selected.(runforkexecution.SelectedContractForkLifecycle).RequireRunForkSelectedContractBinding(ctx, original.ForkRunID)
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		if !point.SameIdentity(binding.ForkPoint) {
			return runfork.RunForkActivation{}, fmt.Errorf("native component lost its original cut")
		}
		fixed.point = point
		var overrides []durabledata.ExplicitPin
		if original.ForkOperation != nil {
			overrides = original.ForkOperation.DataPinOverrides
		}
		prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
			SourceRunID: binding.SourceRunID, At: point.Input, AtStart: point.Kind == runfork.RunForkPointRunStart,
			ContractSelection: binding.ContractSelection, SourceLoader: loader, ExpectedBundleHash: binding.TargetRootReference.BundleHash,
			DataPinOverrides: overrides,
			AgentRuntime:     runforkexecution.SelectedContractAgentRuntimeOptions{ProcessCapability: capability, ExecutionPosture: executionposture.Live, Config: &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}}},
		})
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		t.Cleanup(func() {
			if err := prepared.Close(); err != nil {
				t.Error(err)
			}
		})
		request, err := prepared.MaterializationRequest()
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		scope, err := authoractivity.BundleScopeForTarget(ctx, request.SourceArtifactFact.BundleHash())
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		nativeCtx := authoractivity.WithScope(correlation.WithSourceArtifactFact(ctx, request.SourceArtifactFact), scope)
		child, err := fixed.RequireMaterializedRunForkForSelectedContractExecution(nativeCtx, original.ForkRunID, request)
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		original.InheritedSchedules = request.Readiness
		_, original.AllowedSourceEventIDs, _, err = runfork.RunForkContractFrontierEvidenceBinding(request.FrontierAdmission)
		if err != nil {
			return runfork.RunForkActivation{}, err
		}
		phaseCtx, _, _ := admitSelectedForkNativePhaseForTest(t, nativeCtx, authorActivityReceiptFixture{store: selected.(authorActivityReceiptStore)}, original.ExecutionSource, child, request)
		// This fixture intentionally has no ordinary target: passing all native
		// validation must still refuse an ACK at the durable-operation boundary.
		original.ForkOperation = nil
		return call(phaseCtx, original)
	}
}

func requireSelectedStoreFinalSettlement(t *testing.T, ctx context.Context, selected *PostgresStore, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	t.Helper()
	authority, ok := effects.AuthorityFromContext(ctx)
	if !ok || authority.SelectedFork.ForkRunID != req.ForkRunID {
		t.Fatal("settlement fixture lost its exact selected authority")
	}
	return runfork.RunForkActivation{}, selected.RequireRunForkSelectedContractExecutionSettlement(ctx, authority, req.AllowedSourceEventIDs, req.ExecutionSource)
}

func requireSelectedNativeValidationBeforeHandoff(t *testing.T, result runfork.RunForkActivation, err error) {
	t.Helper()
	if err == nil || err.Error() != "live fork activation requires its durable operation" || result.Activated || result.SourceFrozen || result.ForkRunStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("native validation did not stop before ordinary live handoff: %+v err=%v", result, err)
	}
}
