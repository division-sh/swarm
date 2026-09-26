package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// resumePrepared binds the committed child instead of repeating materialization.
// The durable binding, bundle, pins and topology are readback evidence, not a
// reconstruction of what a fresh fork would create.
func (o SelectedContractExecutionOwner) resumePrepared(ctx context.Context, prepared *PreparedSelectedFork, recovered runfork.SelectedForkRecoveryResult) (runfork.RunForkMaterialization, error) {
	if prepared == nil || recovered.Resume == nil || prepared.recoveryFromExecutionID != recovered.ExecutionID ||
		recovered.Disposition != runfork.SelectedForkRecoveryResumeFiniteFeed {
		return runfork.RunForkMaterialization{}, errors.New("selected finite-feed resume lacks admitted predecessor")
	}
	resume := recovered.Resume
	if resume.ForkRunStatus != runfork.RunForkMaterializedStatus {
		return runfork.RunForkMaterialization{}, errors.New("selected finite-feed resume child is not paused")
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || !contexts.recovered || contexts.entries[prepared.operation] == nil {
		return runfork.RunForkMaterialization{}, errors.New("selected finite-feed resume has no current process preparation")
	}
	binding, err := o.ports.fork.RequireRunForkSelectedContractBinding(ctx, recovered.RunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if err := validateSelectedContractExecutionBinding(recovered.RunID, binding); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	availability, err := o.ports.fork.LoadRunBundleAvailability(ctx, recovered.RunID)
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if !availability.Available() || availability.Status != runfork.RunForkMaterializedStatus ||
		availability.BundleHash != resume.Operation.TargetBundleHash {
		return runfork.RunForkMaterialization{}, fmt.Errorf("selected finite-feed resume child bundle differs from durable operation: %s", availability.DetailString())
	}
	action, err := selectedRecoveryActionFor(recovered, runfork.SelectedForkRecoveryEntry{Binding: binding, BundleHash: availability.BundleHash})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if action != selectedRecoveryResumeFiniteFeed || !sameSelectedForkPointIdentity(prepared.plan.ForkPoint, binding.ForkPoint) ||
		prepared.plan.SourceRunID != binding.SourceRunID || prepared.loadedSource.SourceArtifactFact.BundleHash() != availability.BundleHash {
		return runfork.RunForkMaterialization{}, errors.New("selected finite-feed resume plan differs from committed child")
	}
	if err := contexts.bindStagedPreparationLocked(prepared.operation, binding); err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	return runfork.RunForkMaterialization{
		SourceRunID: binding.SourceRunID, ForkRunID: recovered.RunID,
		ForkRunStatus: resume.ForkRunStatus, ForkPoint: binding.ForkPoint,
		SelectedContractBinding: &binding, DeliveryResumeBlocked: true, SourceRunStatusUnchanged: true,
		DataPins:        append(resume.Pins[:0:0], resume.Pins...),
		AgentTopologies: append(resume.AgentTopologies[:0:0], resume.AgentTopologies...),
	}, nil
}
