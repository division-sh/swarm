package manager

// Readiness entry points bind the exact source and observed row before handing execution to the owned attachment pass.

import (
	"context"
	"errors"
	"fmt"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
)

type dynamicFlowRuntimeReadinessSource struct {
	fact   runtimecorrelation.SourceArtifactFact
	source semanticview.Source
}

var errDynamicFlowRuntimeReadinessPlanStale = errors.New(
	"dynamic flow runtime readiness declared plan is stale",
)

var errDynamicFlowRuntimeReadinessSourceStale = errors.New(
	"dynamic flow runtime readiness callback source is stale",
)

func (am *AgentManager) signalDynamicFlowRuntimeReadiness() {
	if am == nil || am.dynamicFlowReadinessSignal == nil {
		return
	}
	select {
	case am.dynamicFlowReadinessSignal <- struct{}{}:
	default:
	}
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	instancePath string,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires manager and workflow store")
	}
	key, err := newDynamicFlowRuntimeReadinessKey(runID, instancePath)
	if err != nil {
		return err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, key.runID, runtimeflowidentity.RouteForInstancePath(key.instancePath))
	if err != nil {
		return err
	}
	var plan runtimepipeline.DynamicFlowRuntimeReadinessPlan
	var attemptOrdinal uint64
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	if found {
		plan, err = readiness.Plan.Normalized()
		if err != nil {
			return err
		}
		if err := validateDynamicFlowRuntimeReadinessCallbackSource(plan, admittedSource); err != nil {
			return err
		}
		attemptOrdinal = readiness.AttemptOrdinal
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: plan, attemptOrdinal: attemptOrdinal, observed: &readiness, source: admittedSource,
	})
}

func (am *AgentManager) reconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	attemptOrdinal uint64,
	source semanticview.Source,
) error {
	normalized, err := plan.Normalized()
	if err != nil {
		return err
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, source)
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(normalized, admittedSource); err != nil {
		return err
	}
	key, err := newDynamicFlowRuntimeReadinessKey(normalized.RunID, normalized.Identity.InstancePath)
	if err != nil {
		return err
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: normalized, attemptOrdinal: attemptOrdinal, source: admittedSource,
	})
}

func (am *AgentManager) reconcileCommittedDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	attemptOrdinal uint64,
	source semanticview.Source,
) error {
	normalized, err := plan.Normalized()
	if err != nil {
		return err
	}
	admittedSource, err := am.dynamicFlowRuntimeReadinessSource(ctx, source)
	if err != nil {
		return err
	}
	if err := validateDynamicFlowRuntimeReadinessCallbackSource(normalized, admittedSource); err != nil {
		return err
	}
	key, err := newDynamicFlowRuntimeReadinessKey(normalized.RunID, normalized.Identity.InstancePath)
	if err != nil {
		return err
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: normalized, attemptOrdinal: attemptOrdinal, source: admittedSource,
		topologyDurable: true,
	})
}

// PreparePersistedDynamicFlowRuntimeProcessTopology consumes the exact durable
// plan for one live flow owner without claiming durable readiness completion.
// It is used by bounded runtimes that execute before their run is activated.
func (am *AgentManager) PreparePersistedDynamicFlowRuntimeProcessTopology(
	ctx context.Context,
	owner runtimeflowidentity.RunScopedFlowInstance,
) error {
	if am == nil || am.workflowInstances == nil {
		return fmt.Errorf("dynamic flow runtime readiness finalizer requires manager and workflow store")
	}
	owner = owner.Normalize()
	if err := owner.Validate(); err != nil {
		return err
	}
	readiness, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(ctx, owner.RunID, owner.Route)
	if err != nil {
		return fmt.Errorf("load committed dynamic flow runtime readiness %s: %w", owner.Route.InstancePath, err)
	}
	if !found {
		return fmt.Errorf("committed dynamic flow runtime readiness not found for %s", owner.Route.InstancePath)
	}
	if readiness.Plan.RunID != owner.RunID || readiness.Plan.Identity.Route() != owner.Route {
		return fmt.Errorf("committed dynamic flow runtime readiness identity does not match %s", owner.Route.InstancePath)
	}
	source, err := am.dynamicFlowRuntimeReadinessSource(ctx)
	if err != nil {
		return err
	}
	plan, err := readiness.Plan.Normalized()
	if err != nil {
		return err
	}
	key, err := newDynamicFlowRuntimeReadinessKey(plan.RunID, readiness.InstancePath)
	if err != nil {
		return err
	}
	return am.reconcileDeclaredDynamicFlowRuntimeReadiness(dynamicFlowRuntimeReadinessAdmission{
		ctx: ctx, key: key, plan: plan, attemptOrdinal: readiness.AttemptOrdinal, observed: &readiness, source: source,
		processOnly: true, preAdmission: true, previouslyCompleted: (readiness.Phase == runtimepipeline.FlowAttachmentReady),
	})
}

func (am *AgentManager) dynamicFlowRuntimeReadinessSource(
	ctx context.Context,
	candidates ...semanticview.Source,
) (dynamicFlowRuntimeReadinessSource, error) {
	if am == nil {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires manager",
		)
	}
	owned := am.semanticReadinessSource
	if owned.source == nil || !sameLoadedDynamicFlowSemanticSource(am.semanticSource, owned.source) {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires the manager-owned semantic source",
		)
	}
	if len(candidates) > 0 && !sameLoadedDynamicFlowSemanticSource(owned.source, candidates[0]) {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"%w: callback semantic source is not the manager-owned loaded source",
			errDynamicFlowRuntimeReadinessSourceStale,
		)
	}
	if err := owned.fact.Validate(); err != nil {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness manager source fact: %w",
			err,
		)
	}
	sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness requires exact bundle source fact",
		)
	}
	if !sourceFact.Matches(owned.fact) {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"%w: declared=%s active=%s",
			errDynamicFlowRuntimeReadinessSourceStale,
			owned.fact.BundleHash(),
			sourceFact.BundleHash(),
		)
	}
	if strings.TrimSpace(owned.source.WorkflowVersion()) == "" {
		return dynamicFlowRuntimeReadinessSource{}, fmt.Errorf(
			"dynamic flow runtime readiness manager source requires workflow version",
		)
	}
	return owned, nil
}

func sameLoadedDynamicFlowSemanticSource(left, right semanticview.Source) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftBundle, leftOK := semanticview.Bundle(left)
	rightBundle, rightOK := semanticview.Bundle(right)
	return leftOK && rightOK && leftBundle == rightBundle
}

func dynamicFlowRuntimeReadinessSourceCoordinate(ctx context.Context) (string, error) {
	sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("dynamic flow runtime readiness requires exact source artifact fact")
	}
	if err := sourceFact.Validate(); err != nil {
		return "", fmt.Errorf("dynamic flow runtime readiness source artifact fact: %w", err)
	}
	return sourceFact.BundleHash(), nil
}

func validateDynamicFlowRuntimeReadinessCallbackSource(
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	source dynamicFlowRuntimeReadinessSource,
) error {
	bundleHash := source.fact.BundleHash()
	if bundleHash != plan.BundleHash {
		return fmt.Errorf(
			"%w: declared=%s active=%s",
			errDynamicFlowRuntimeReadinessSourceStale,
			plan.BundleHash,
			bundleHash,
		)
	}
	if source.source == nil {
		return fmt.Errorf("dynamic flow runtime readiness reconciler requires semantic source")
	}
	if strings.TrimSpace(source.source.WorkflowVersion()) != plan.WorkflowVersion {
		return fmt.Errorf(
			"dynamic flow runtime readiness %s workflow version changed: persisted=%s active=%s",
			plan.Identity.InstancePath,
			plan.WorkflowVersion,
			strings.TrimSpace(source.source.WorkflowVersion()),
		)
	}
	return nil
}
