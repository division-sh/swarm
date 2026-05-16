package destructivereset

import (
	"context"
	"fmt"
	"strings"
	"time"
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

	result := ContainerResetResult{
		OperationName: req.Result.OperationName,
		DryRun:        req.Result.DryRun,
		AppliedAt:     req.RequestedAt,
	}
	for _, planned := range req.Result.Plan.EntityContainers {
		if strings.TrimSpace(planned.Name) == "" {
			return ContainerResetResult{}, fmt.Errorf("%w: destructive reset container name is required", ErrInvalidRequest)
		}
		inspection, err := s.Runtime.InspectManagedContainer(ctx, planned.Name)
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
		if !inspection.HasIdentity || !resetEligibleManagedIdentity(inspection.Identity) || strings.TrimSpace(inspection.Identity.ContainerName) != strings.TrimSpace(planned.Name) {
			result.Preserved = append(result.Preserved, preservedContainerRef(planned, inspection.Identity))
			continue
		}
		ref := containerRefFromIdentity(inspection.Identity, ContainerActionStop)
		if !inspection.Running {
			result.AlreadyStopped = append(result.AlreadyStopped, withContainerAction(ref, ContainerActionAlreadyStopped))
			continue
		}
		result.Selected = append(result.Selected, ref)
		if req.Result.DryRun {
			continue
		}
		if err := s.Runtime.StopManagedContainer(ctx, ref.Name); err != nil {
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

func resetEligibleManagedIdentity(identity ContainerIdentity) bool {
	return strings.TrimSpace(identity.Owner) == "runtime" &&
		identity.ResetEligible &&
		resetEligibleContainerKind(identity.Kind)
}

func resetEligibleContainerKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "entity", "agent", "flow":
		return true
	default:
		return false
	}
}

func containerRefFromIdentity(identity ContainerIdentity, action string) ContainerRef {
	return ContainerRef{
		Name:           strings.TrimSpace(identity.ContainerName),
		Kind:           strings.TrimSpace(identity.Kind),
		Action:         strings.TrimSpace(action),
		ResetEligible:  identity.ResetEligible,
		CreationSource: strings.TrimSpace(identity.CreationSource),
		WorkspaceScope: strings.TrimSpace(identity.WorkspaceScope),
		RunID:          strings.TrimSpace(identity.RunID),
		EntityID:       strings.TrimSpace(identity.EntityID),
		AgentID:        strings.TrimSpace(identity.AgentID),
		FlowInstance:   strings.Trim(strings.TrimSpace(identity.FlowInstance), "/"),
	}
}

func preservedContainerRef(planned ContainerRef, identity ContainerIdentity) ContainerRef {
	if strings.TrimSpace(identity.ContainerName) == "" {
		return withContainerAction(planned, ContainerActionUnowned)
	}
	return withContainerAction(containerRefFromIdentity(identity, ContainerActionUnowned), ContainerActionUnowned)
}

func withContainerAction(ref ContainerRef, action string) ContainerRef {
	ref.Name = strings.TrimSpace(ref.Name)
	ref.Kind = strings.TrimSpace(ref.Kind)
	ref.Action = strings.TrimSpace(action)
	ref.CreationSource = strings.TrimSpace(ref.CreationSource)
	ref.WorkspaceScope = strings.TrimSpace(ref.WorkspaceScope)
	ref.RunID = strings.TrimSpace(ref.RunID)
	ref.EntityID = strings.TrimSpace(ref.EntityID)
	ref.AgentID = strings.TrimSpace(ref.AgentID)
	ref.FlowInstance = strings.Trim(strings.TrimSpace(ref.FlowInstance), "/")
	return ref
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
