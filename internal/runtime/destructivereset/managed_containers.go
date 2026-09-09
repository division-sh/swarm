package destructivereset

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/containeridentity"
)

type ManagedContainerStopper struct {
	Runtime ManagedContainerRuntime
	Now     func() time.Time
}

func (s ManagedContainerStopper) Apply(ctx context.Context, req ContainerResetRequest) (ContainerResetResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	if req.ActorTokenID == "" {
		return ContainerResetResult{}, fmt.Errorf("%w: actor token id is required", ErrInvalidRequest)
	}
	if strings.TrimSpace(req.Result.OperationName) == "" {
		req.Result.OperationName = DefaultOperationName
	}
	if req.Result.PlannedAt.IsZero() {
		return ContainerResetResult{}, fmt.Errorf("%w: destructive reset plan result is required", ErrInvalidRequest)
	}
	if !req.Result.DryRun {
		if req.Cleanup.AppliedAt.IsZero() {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset cleanup result is required", ErrInvalidRequest)
		}
		if req.Cleanup.DryRun {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset managed containers require applied cleanup", ErrInvalidRequest)
		}
		if strings.TrimSpace(req.Cleanup.OperationName) != strings.TrimSpace(req.Result.OperationName) {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset cleanup operation does not match plan result", ErrInvalidRequest)
		}
		if req.Cleanup.AppliedAt.Before(req.Result.PlannedAt) {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset cleanup predates plan result", ErrInvalidRequest)
		}
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = s.now()
	}
	req.RequestedAt = req.RequestedAt.UTC()
	if s.Runtime == nil {
		return ContainerResetResult{}, fmt.Errorf("destructive reset managed container runtime is not configured")
	}
	for _, planned := range req.Result.Plan.ManagedContainers {
		if strings.TrimSpace(planned.Name) == "" || strings.TrimSpace(planned.RuntimeID) == "" {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset container name and immutable runtime ID are required", ErrInvalidRequest)
		}
	}

	result := ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
	}
	for _, planned := range req.Result.Plan.ManagedContainers {
		inspection, err := s.Runtime.InspectManagedContainer(ctx, planned.RuntimeID)
		if err != nil {
			result.Failed = append(result.Failed, ContainerStopFailure{
				Container: withContainerAction(planned, ContainerActionFailed),
				Error:     err.Error(),
			})
			continue
		}
		if !inspection.Exists {
			result.Missing = append(result.Missing, withContainerAction(planned, ContainerActionMissing))
			continue
		}
		if inspection.RuntimeID != planned.RuntimeID {
			result.Failed = append(result.Failed, ContainerStopFailure{Container: withContainerAction(planned, ContainerActionFailed), Error: "container inspection differs from planned immutable runtime ID"})
			continue
		}
		if !inspection.HasIdentity || inspection.Identity.BundleHash == "" || inspection.Identity.Validate() != nil || !inspection.Identity.ResetEligibleManaged() || !inspection.Identity.Equal(planned.Identity()) {
			result.Preserved = append(result.Preserved, preservedContainerRef(planned, inspection.Identity))
			result.Failed = append(result.Failed, ContainerStopFailure{
				Container: withContainerAction(planned, ContainerActionFailed),
				Error:     "container ownership differs from the exact planned source/projection identity",
			})
			continue
		}
		ref := ContainerRefFromIdentity(inspection.Identity, planned.RuntimeID, ContainerActionStop)
		if !inspection.Running {
			result.AlreadyStopped = append(result.AlreadyStopped, withContainerAction(ref, ContainerActionAlreadyStopped))
			continue
		}
		result.Selected = append(result.Selected, ref)
		if req.Result.DryRun {
			continue
		}
		if err := s.Runtime.StopManagedContainer(ctx, ref); err != nil {
			result.Failed = append(result.Failed, ContainerStopFailure{
				Container: withContainerAction(ref, ContainerActionFailed),
				Error:     err.Error(),
			})
			continue
		}
		result.Stopped = append(result.Stopped, withContainerAction(ref, ContainerActionStop))
	}
	return copyContainerResetResult(result), nil
}

func (s ManagedContainerStopper) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func ContainerRefFromIdentity(identity containeridentity.Identity, runtimeID, action string) ContainerRef {
	identity = identity.Normalized()
	return ContainerRef{
		RuntimeID:        strings.TrimSpace(runtimeID),
		Owner:            identity.Owner,
		Name:             strings.TrimSpace(identity.ContainerName),
		Kind:             strings.TrimSpace(identity.Kind),
		Action:           strings.TrimSpace(action),
		ResetEligible:    identity.ResetEligible,
		CreationSource:   strings.TrimSpace(identity.CreationSource),
		WorkspaceScope:   strings.TrimSpace(identity.WorkspaceScope),
		RunID:            strings.TrimSpace(identity.RunID),
		AgentIdentity:    identity.AgentIdentity.Normalize(),
		FlowInstance:     strings.Trim(strings.TrimSpace(identity.FlowInstance), "/"),
		BundleHash:       identity.BundleHash,
		SourceProjection: identity.SourceProjection,
		DataProjection:   identity.DataProjection,
	}
}

func preservedContainerRef(planned ContainerRef, identity containeridentity.Identity) ContainerRef {
	if strings.TrimSpace(identity.ContainerName) == "" {
		return withContainerAction(planned, ContainerActionUnowned)
	}
	return ContainerRefFromIdentity(identity, planned.RuntimeID, ContainerActionUnowned)
}

func withContainerAction(ref ContainerRef, action string) ContainerRef {
	return ContainerRefFromIdentity(ref.Identity(), ref.RuntimeID, action)
}

func copyContainerResetResult(result ContainerResetResult) ContainerResetResult {
	result.Selected = append([]ContainerRef(nil), result.Selected...)
	result.Preserved = append([]ContainerRef(nil), result.Preserved...)
	result.Missing = append([]ContainerRef(nil), result.Missing...)
	result.AlreadyStopped = append([]ContainerRef(nil), result.AlreadyStopped...)
	result.Stopped = append([]ContainerRef(nil), result.Stopped...)
	result.Failed = append([]ContainerStopFailure(nil), result.Failed...)
	return result
}

// A successful receipt accounts for every immutable target once. Preserving a
// changed object is safe, but is not proof that the planned resource settled.
func validateContainerSettlement(planned []ContainerRef, result ContainerResetResult) error {
	if len(result.Failed) != 0 || len(result.Preserved) != 0 {
		return fmt.Errorf("reset container settlement retains unresolved ownership")
	}
	targets := make(map[string]ContainerRef, len(planned))
	for _, target := range planned {
		if _, duplicate := targets[target.RuntimeID]; duplicate || target.RuntimeID == "" {
			return fmt.Errorf("reset container intent is missing or duplicates immutable identity %q", target.RuntimeID)
		}
		targets[target.RuntimeID] = target
	}
	settled := make(map[string]bool, len(targets))
	for _, group := range []struct {
		targets []ContainerRef
		action  string
	}{
		{result.Stopped, ContainerActionStop},
		{result.AlreadyStopped, ContainerActionAlreadyStopped},
		{result.Missing, ContainerActionMissing},
	} {
		for _, target := range group.targets {
			original, exists := targets[target.RuntimeID]
			if !exists || settled[target.RuntimeID] || !original.Identity().Equal(target.Identity()) || target.Action != group.action {
				return fmt.Errorf("reset container receipt contradicts planned target %q", target.RuntimeID)
			}
			settled[target.RuntimeID] = true
		}
	}
	if len(settled) != len(targets) {
		return fmt.Errorf("reset container receipt settled %d of %d planned targets", len(settled), len(targets))
	}
	selected := make(map[string]bool, len(result.Selected))
	for _, target := range result.Selected {
		original, exists := targets[target.RuntimeID]
		if !exists || selected[target.RuntimeID] || !original.Identity().Equal(target.Identity()) || target.Action != ContainerActionStop {
			return fmt.Errorf("reset container selection contradicts planned target %q", target.RuntimeID)
		}
		selected[target.RuntimeID] = true
	}
	if len(selected) != len(result.Stopped) {
		return fmt.Errorf("reset container receipt did not stop its exact selected set")
	}
	for _, target := range result.Stopped {
		if !selected[target.RuntimeID] {
			return fmt.Errorf("reset container receipt stopped an unselected target %q", target.RuntimeID)
		}
	}
	return nil
}
