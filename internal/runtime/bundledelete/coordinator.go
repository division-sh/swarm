package bundledelete

import (
	"context"
	"fmt"
	"strings"
	"time"

	"swarm/internal/runtime/destructivereset"
	"swarm/internal/runtime/preservationcleanup"
)

type Coordinator struct {
	Planner            Planner
	Cleaner            PreservationCleaner
	Finalizer          Finalizer
	Locks              LockManager
	ContainerInventory ManagedContainerInventoryReader
	Containers         ManagedContainerStopper
	Now                func() time.Time
	Operation          string
	LockKey            string
}

func (c *Coordinator) Execute(ctx context.Context, req Request) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return Result{}, fmt.Errorf("bundle delete coordinator is required")
	}
	now := c.now()
	req, err := NormalizeRequest(req, now)
	if err != nil {
		return Result{}, err
	}
	if c.Planner == nil {
		return Result{}, fmt.Errorf("bundle delete planner is required")
	}
	if c.Finalizer == nil {
		return Result{}, fmt.Errorf("bundle delete finalizer is required")
	}
	if c.Locks == nil {
		return Result{}, fmt.Errorf("bundle delete lock manager is required")
	}

	lease, acquired, err := c.Locks.TryAcquire(ctx, c.lockKey())
	if err != nil {
		return Result{}, err
	}
	if !acquired {
		return Result{}, ErrOperationInProgress
	}
	if lease == nil {
		return Result{}, fmt.Errorf("bundle delete lock lease is missing")
	}
	defer func() {
		_ = lease.Release(context.Background())
	}()

	plan, err := c.Planner.PlanBundleDelete(ctx, req)
	if err != nil {
		return Result{}, err
	}
	plan.PlannedAt = req.RequestedAt
	result := Result{
		OK:            true,
		Status:        "completed",
		OperationName: c.operationName(),
		BundleHash:    req.BundleHash,
		Force:         req.Force,
		DryRun:        req.DryRun,
		Plan:          plan,
	}
	if !req.Force {
		return c.executeNonForce(ctx, req, result)
	}
	if c.Cleaner == nil {
		return Result{}, fmt.Errorf("bundle delete preservation cleaner is required")
	}
	if c.ContainerInventory == nil {
		return Result{}, fmt.Errorf("bundle delete managed container inventory is required")
	}
	if c.Containers == nil {
		return Result{}, fmt.Errorf("bundle delete managed container stopper is required")
	}
	containers, err := c.managedContainersForPlan(ctx, plan)
	if err != nil {
		return result.withPartialFailure("managed_containers", err), nil
	}
	result.Plan.EntityContainers = containers

	if req.DryRun {
		containerResult, err := c.applyContainers(ctx, req, result.Plan, preservationcleanup.Result{})
		if err != nil {
			return result.withPartialFailure("managed_containers", err).asDryRun(), nil
		}
		result.Status = "dry_run"
		result.Containers = containerResult
		result.ActiveRunsStopped = len(result.Plan.ActiveRuns)
		result.DeliveriesCancelled = len(result.Plan.ActiveDeliveries)
		result.ContainersStopped = len(containerResult.Selected)
		return result, nil
	}

	cleanup, err := c.Cleaner.ApplyBundleForceDeletePreservationCleanup(ctx, preservationcleanup.Request{
		OperationName: preservationcleanup.BundleForceDeleteOperationName,
		RequestedAt:   req.RequestedAt,
		ControlledBy:  preservationcleanup.BundleForceDeleteControlledBy,
		Targets:       ActiveRunTargets(result.Plan),
	})
	if err != nil {
		return result.withPartialFailure("preservation_cleanup", err), nil
	}
	result.Cleanup = cleanup
	result.ActiveRunsStopped = len(cleanup.Runs)
	result.DeliveriesCancelled = len(cleanup.Deliveries)

	containerResult, err := c.applyContainers(ctx, req, result.Plan, cleanup)
	if err != nil {
		return result.withPartialFailure("managed_containers", err), nil
	}
	result.Containers = containerResult
	result.ContainersStopped = len(containerResult.Stopped)
	if len(containerResult.Failed) > 0 {
		partial := result
		for _, failure := range containerResult.Failed {
			partial = partial.withPartialFailure("managed_containers", fmt.Errorf("%s: %s", failure.Container.Name, failure.Error))
		}
		return partial, nil
	}

	final, err := c.Finalizer.ApplyBundleDeleteFinalMutation(ctx, FinalMutationRequest{
		OperationName: c.operationName(),
		BundleHash:    req.BundleHash,
		RequestedAt:   req.RequestedAt,
	})
	if err != nil {
		return result.withPartialFailure("phase_5_bundle_delete", err), nil
	}
	result.FinalMutation = final
	result.Deleted = final.Deleted
	return result, nil
}

func (c *Coordinator) executeNonForce(ctx context.Context, req Request, result Result) (Result, error) {
	if len(result.Plan.ActiveRuns) > 0 {
		return Result{}, &ActiveRunsRemainError{
			BundleHash: req.BundleHash,
			ActiveRuns: result.Plan.ActiveRuns,
		}
	}
	if req.DryRun {
		result.Status = "dry_run"
		return result, nil
	}
	final, err := c.Finalizer.ApplyBundleDeleteFinalMutation(ctx, FinalMutationRequest{
		OperationName: c.operationName(),
		BundleHash:    req.BundleHash,
		RequestedAt:   req.RequestedAt,
	})
	if err != nil {
		return Result{}, err
	}
	result.FinalMutation = final
	result.Deleted = final.Deleted
	return result, nil
}

func (c *Coordinator) managedContainersForPlan(ctx context.Context, plan Plan) ([]destructivereset.ContainerRef, error) {
	refs, err := c.ContainerInventory.ManagedResetContainerInventory(ctx)
	if err != nil {
		return nil, err
	}
	affected := AffectedRunIDs(plan)
	out := make([]destructivereset.ContainerRef, 0, len(refs))
	for _, ref := range refs {
		if _, ok := affected[strings.TrimSpace(ref.RunID)]; !ok {
			continue
		}
		out = append(out, ref)
	}
	return out, nil
}

func (c *Coordinator) applyContainers(ctx context.Context, req Request, plan Plan, cleanup preservationcleanup.Result) (destructivereset.ContainerResetResult, error) {
	cleanupResult := destructivereset.CleanupResult{
		OperationName: c.operationName(),
		DryRun:        req.DryRun,
		AppliedAt:     cleanup.AppliedAt,
		RunIDs:        AffectedRunIDList(plan),
	}
	return c.Containers.Apply(ctx, destructivereset.ContainerResetRequest{
		Result: destructivereset.Result{
			OperationName: c.operationName(),
			DryRun:        req.DryRun,
			PlannedAt:     plan.PlannedAt,
			Plan: destructivereset.Plan{
				EntityContainers: plan.EntityContainers,
			},
		},
		Cleanup:      cleanupResult,
		ActorTokenID: req.ActorTokenID,
		RequestedAt:  req.RequestedAt,
	})
}

func (c *Coordinator) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *Coordinator) operationName() string {
	if c == nil || strings.TrimSpace(c.Operation) == "" {
		return DefaultOperationName
	}
	return strings.TrimSpace(c.Operation)
}

func (c *Coordinator) lockKey() string {
	if c == nil || strings.TrimSpace(c.LockKey) == "" {
		return destructivereset.DefaultLockKey
	}
	return strings.TrimSpace(c.LockKey)
}

func (r Result) withPartialFailure(scope string, err error) Result {
	r.OK = false
	r.Status = "partial_failure"
	r.PartialFailure = true
	if err != nil {
		r.Errors = append(r.Errors, PartialError{
			Scope:   strings.TrimSpace(scope),
			Message: strings.TrimSpace(err.Error()),
		})
	}
	return r
}

func (r Result) asDryRun() Result {
	r.Status = "dry_run"
	r.DryRun = true
	return r
}
