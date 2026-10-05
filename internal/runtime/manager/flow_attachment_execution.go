package manager

// Attachment execution coalesces callers and retains cleanup ownership through settlement. These process-local leases are not durable phase authority.

import (
	"context"
	"errors"
	"fmt"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"strings"
	"time"
)

type dynamicFlowRuntimeReadinessKey struct {
	runID        string
	instancePath string
}

type dynamicFlowRuntimeReadinessAttempt struct {
	done              chan struct{}
	retiring          chan struct{}
	err               error
	attemptOrdinal    uint64
	planHash          string
	successorRequired bool
	successor         *dynamicFlowRuntimeReadinessAdmission
}

type dynamicFlowActiveAttempt struct {
	pending                  *runtimepipeline.DynamicFlowRuntimeActivationRequest
	admissionDone            chan struct{}
	receipt                  runtimepipeline.DynamicFlowRuntimeActivationAttempt
	planHash                 string
	identity                 runtimeflowidentity.RunScopedFlowInstance
	publication              runtimebus.FlowRoutePublicationHandle
	retiring                 bool
	retirementSet            *terminalFlowRetirement
	retirementKind           flowActivationRetirementDisposition
	locallyRetired           bool
	timersRetired            bool
	complete                 bool
	timersProjected          bool
	admittedPreRun           bool
	preRunPredecessorOrdinal uint64
}

var errDynamicFlowRuntimeReadinessRetiring = errors.New("dynamic flow runtime readiness retains predecessor retirement")

func (a *dynamicFlowRuntimeReadinessAttempt) wait(ctx context.Context) error {
	select {
	case <-a.done:
		return a.err
	default:
	}
	select {
	case <-a.done:
		return a.err
	case <-a.retiring:
		return errDynamicFlowRuntimeReadinessRetiring
	case <-ctx.Done():
		return ctx.Err()
	}
}

type readinessRetirementWaitersKey struct{}

// Only the owned readiness attempt installs this dependency. The lifecycle
// owner releases its callers after capturing, not predicting, a joined predecessor.
func releaseReadinessRetirementWaiters(ctx context.Context, retirement *agentRetirement) {
	if retirement == nil || (retirement.done == nil && retirement.settled == nil && retirement.leases == nil && !retirement.token.Valid()) {
		return
	}
	attempt, ok := ctx.Value(readinessRetirementWaitersKey{}).(*dynamicFlowRuntimeReadinessAttempt)
	if !ok {
		return
	}
	select {
	case <-attempt.retiring:
	default:
		close(attempt.retiring)
	}
}

const (
	dynamicFlowRuntimeReadinessCleanupTimeout = 10 * time.Second
	defaultDynamicFlowReadinessRetryInterval  = 5 * time.Second
)

func newDynamicFlowRuntimeReadinessKey(runID, instancePath string) (dynamicFlowRuntimeReadinessKey, error) {
	key := dynamicFlowRuntimeReadinessKey{
		runID:        strings.TrimSpace(runID),
		instancePath: strings.Trim(strings.TrimSpace(instancePath), "/"),
	}
	if key.runID == "" || key.instancePath == "" {
		return dynamicFlowRuntimeReadinessKey{}, fmt.Errorf("dynamic flow runtime readiness requires exact run_id and instance_id")
	}
	return key, nil
}

func (am *AgentManager) reconcileDeclaredDynamicFlowRuntimeReadiness(
	admission dynamicFlowRuntimeReadinessAdmission,
) (result error) {
	var err error
	admission.planHash, err = admission.plan.Hash()
	if err != nil {
		return err
	}
	if err := am.requireRunExecutionOwnership(admission.ctx, admission.key.runID); err != nil {
		return err
	}
	if admission.observed == nil {
		current, found, err := am.workflowInstances.LoadDynamicFlowRuntimeReadiness(admission.ctx, admission.key.runID, admission.plan.Identity.Route())
		if err != nil {
			return err
		}
		if !found {
			return errDynamicFlowRuntimeReadinessPlanStale
		}
		admission.observed = &current
	}
	if admission.observed.AttemptOrdinal != admission.attemptOrdinal || admission.observed.PlanHash != admission.planHash {
		return errDynamicFlowRuntimeReadinessPlanStale
	}
	// The caller may stop waiting, but the accepted attempt must settle its
	// retirement. Begin retains the exact Manager/standing/fork occurrence;
	// cancellation of those owners, rather than the waiter, controls execution.
	lease, err := am.beginWork(context.WithoutCancel(admission.ctx), "declared readiness attempt")
	if err != nil {
		return err
	}
	transferred := false
	defer func() {
		if !transferred {
			result = errors.Join(result, lease.Done())
		}
	}()
	am.dynamicFlowReadinessMu.Lock()
	// Coalescing schedules work only. The execution read and selected-store
	// admission reject stale callbacks before any process resource is installed.
	if am.dynamicFlowReadinessAttempts == nil {
		am.dynamicFlowReadinessAttempts = make(map[dynamicFlowRuntimeReadinessKey]*dynamicFlowRuntimeReadinessAttempt)
	}
	if attempt := am.dynamicFlowReadinessAttempts[admission.key]; attempt != nil {
		if admission.attemptOrdinal > attempt.attemptOrdinal || (admission.attemptOrdinal == attempt.attemptOrdinal && admission.planHash != attempt.planHash) {
			attempt.successorRequired = true
			successor := admission
			attempt.successor = &successor
		} else if admission.attemptOrdinal < attempt.attemptOrdinal {
			am.dynamicFlowReadinessMu.Unlock()
			return errDynamicFlowRuntimeReadinessPlanStale
		}
		am.dynamicFlowReadinessMu.Unlock()
		return attempt.wait(admission.ctx)
	}
	attempt := &dynamicFlowRuntimeReadinessAttempt{
		done:           make(chan struct{}),
		retiring:       make(chan struct{}),
		attemptOrdinal: admission.attemptOrdinal,
		planHash:       admission.planHash,
	}
	am.dynamicFlowReadinessAttempts[admission.key] = attempt
	am.dynamicFlowReadinessMu.Unlock()
	transferred = true
	owned := admission
	owned.ctx = lease.Context()
	go am.completeDeclaredDynamicFlowReadiness(owned, attempt, lease)
	return attempt.wait(admission.ctx)
}

func (am *AgentManager) completeDeclaredDynamicFlowReadiness(admission dynamicFlowRuntimeReadinessAdmission, attempt *dynamicFlowRuntimeReadinessAttempt, lease *worklifetime.Lease) {
	var result error
	completed := false
	finish := func() {
		select {
		case <-attempt.retiring:
			am.lifecycle.recordTerminalCompletion(result)
		default:
		}
		attempt.err = result
		delete(am.dynamicFlowReadinessAttempts, admission.key)
		close(attempt.done)
		completed = true
		am.lifecycle.recordTerminalCompletion(lease.Done())
	}
	defer func() {
		if completed {
			return
		}
		if recovered := recover(); recovered != nil {
			result = errors.Join(result, fmt.Errorf("dynamic flow readiness attempt panic: %v", recovered))
		}
		am.dynamicFlowReadinessMu.Lock()
		finish()
		am.dynamicFlowReadinessMu.Unlock()
	}()
	if am.testAfterDynamicFlowReadinessAdmission != nil {
		am.testAfterDynamicFlowReadinessAdmission()
	}
	current := admission
	for {
		// A coalesced successor contributes admitted source facts, not its
		// caller's lifetime. All executions remain owned by this attempt.
		current.ctx = runtimecorrelation.WithSourceArtifactFact(lease.Context(), current.source.fact)
		scope, err := runtimeauthoractivity.BundleScopeForTarget(current.ctx, current.source.fact.BundleHash())
		if err != nil {
			result = err
			return
		}
		current.ctx = runtimeauthoractivity.WithScope(current.ctx, scope)
		current.ctx = context.WithValue(current.ctx, readinessRetirementWaitersKey{}, attempt)
		attemptErr := am.reconcileDynamicFlowRuntimeReadinessOnce(current)
		am.dynamicFlowReadinessMu.Lock()
		if attempt.successorRequired {
			current = *attempt.successor
			attempt.attemptOrdinal = current.attemptOrdinal
			attempt.planHash = current.planHash
			attempt.successorRequired = false
			attempt.successor = nil
			am.dynamicFlowReadinessMu.Unlock()
			continue
		}
		result = attemptErr
		finish()
		am.dynamicFlowReadinessMu.Unlock()
		return
	}
}
