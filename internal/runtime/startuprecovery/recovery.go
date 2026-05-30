package startuprecovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"swarm/internal/runtime/preservationcleanup"
	"swarm/internal/store/runbundle"
)

type AvailabilityReader interface {
	ActiveRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
}

type PreservationCleanupStore interface {
	ApplyUnavailableBundleStartupPreservationCleanup(context.Context, preservationcleanup.Request) (preservationcleanup.Result, error)
}

type ManagedContainer struct {
	Name  string
	RunID string
	Kind  string
}

type ManagedContainerOwner interface {
	ManagedContainers(context.Context) ([]ManagedContainer, error)
	StopManagedContainer(context.Context, string) error
}

type Request struct {
	AvailabilityReader AvailabilityReader
	CleanupStore       PreservationCleanupStore
	Containers         ManagedContainerOwner
	RequestedAt        time.Time
}

type Result struct {
	CheckedAvailabilities []runbundle.Availability
	DataIntegrityErrors   []runbundle.Availability
	OrphanTargets         []preservationcleanup.RunTarget
	StoppedContainers     []ManagedContainer
	Cleanup               preservationcleanup.Result
}

type DataIntegrityError struct {
	Conflicts []runbundle.Availability
}

func (e DataIntegrityError) Error() string {
	details := make([]string, 0, len(e.Conflicts))
	for _, conflict := range e.Conflicts {
		details = append(details, conflict.DetailString())
	}
	return fmt.Sprintf("%s: persisted bundle data integrity failure for %d active run(s): %s", runbundle.CodeBundleDataIntegrityError, len(e.Conflicts), strings.Join(details, "; "))
}

func IsDataIntegrityError(err error) bool {
	var target DataIntegrityError
	return errors.As(err, &target)
}

func Recover(ctx context.Context, req Request) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req.AvailabilityReader == nil {
		return Result{}, fmt.Errorf("startup recovery availability reader is required")
	}
	at := req.RequestedAt.UTC()
	if at.IsZero() {
		at = time.Now().UTC()
	}

	availabilities, err := req.AvailabilityReader.ActiveRunBundleAvailabilities(ctx)
	if err != nil {
		return Result{}, err
	}
	result := Result{CheckedAvailabilities: append([]runbundle.Availability(nil), availabilities...)}
	for _, availability := range availabilities {
		switch {
		case availability.Available():
			continue
		case availability.DataIntegrityError():
			result.DataIntegrityErrors = append(result.DataIntegrityErrors, availability)
		case availability.Unavailable():
			cause, ok := preservationcleanup.CauseForBundleSource(availability.BundleSource)
			if !ok {
				return result, fmt.Errorf("startup recovery unsupported unavailable bundle source %q for run %s", availability.BundleSource, availability.RunID)
			}
			result.OrphanTargets = append(result.OrphanTargets, preservationcleanup.RunTarget{
				RunID:             availability.RunID,
				BundleSource:      availability.BundleSource,
				BundleHash:        availability.BundleHash,
				BundleFingerprint: availability.BundleFingerprint,
				ReasonCode:        cause,
			})
		default:
			return result, fmt.Errorf("startup recovery unsupported bundle availability for run %s: %s", availability.RunID, availability.DetailString())
		}
	}
	if len(result.DataIntegrityErrors) > 0 {
		return result, DataIntegrityError{Conflicts: append([]runbundle.Availability(nil), result.DataIntegrityErrors...)}
	}
	if len(result.OrphanTargets) == 0 {
		return result, nil
	}
	if req.Containers == nil {
		return result, fmt.Errorf("startup recovery managed container owner is required for orphaned active runs")
	}
	stopped, err := stopRunScopedManagedContainers(ctx, req.Containers, result.OrphanTargets)
	if err != nil {
		return result, err
	}
	result.StoppedContainers = stopped

	if req.CleanupStore == nil {
		return result, fmt.Errorf("startup recovery preservation cleanup store is required")
	}
	cleanup, err := req.CleanupStore.ApplyUnavailableBundleStartupPreservationCleanup(ctx, preservationcleanup.Request{
		OperationName: preservationcleanup.UnavailableBundleStartupOperationName,
		RequestedAt:   at,
		ControlledBy:  preservationcleanup.UnavailableBundleStartupControlledBy,
		Targets:       result.OrphanTargets,
	})
	if err != nil {
		return result, err
	}
	result.Cleanup = cleanup
	return result, nil
}

func stopRunScopedManagedContainers(ctx context.Context, owner ManagedContainerOwner, targets []preservationcleanup.RunTarget) ([]ManagedContainer, error) {
	runIDs := map[string]struct{}{}
	for _, target := range targets {
		if target.RunID != "" {
			runIDs[target.RunID] = struct{}{}
		}
	}
	containers, err := owner.ManagedContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("startup recovery inspect managed containers: %w", err)
	}
	stopped := []ManagedContainer{}
	for _, container := range containers {
		container.Name = strings.TrimSpace(container.Name)
		container.RunID = strings.TrimSpace(container.RunID)
		container.Kind = strings.TrimSpace(container.Kind)
		if container.Name == "" || container.RunID == "" {
			continue
		}
		if _, ok := runIDs[container.RunID]; !ok {
			continue
		}
		if err := owner.StopManagedContainer(ctx, container.Name); err != nil {
			return stopped, fmt.Errorf("startup recovery stop managed container %s for run %s: %w", container.Name, container.RunID, err)
		}
		stopped = append(stopped, container)
	}
	return stopped, nil
}
