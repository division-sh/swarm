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
	if prepared == nil || recovered.Continuation == nil || recovered.Operation == nil || prepared.recoveryFromExecutionID != recovered.ExecutionID ||
		(recovered.Disposition != runfork.SelectedForkRecoveryResume && recovered.Disposition != runfork.SelectedForkRecoveryActivate) {
		return runfork.RunForkMaterialization{}, errors.New("selected resume lacks admitted predecessor")
	}
	resume := recovered.Continuation
	if resume.ForkRunStatus != runfork.RunForkMaterializedStatus && resume.ForkRunStatus != runfork.RunForkActivatedStatus {
		return runfork.RunForkMaterialization{}, errors.New("selected resume child is not executable")
	}
	contexts := o.ports.contexts
	contexts.mu.Lock()
	defer contexts.mu.Unlock()
	if contexts.retired || !contexts.recovered || contexts.entries[prepared.operation] == nil {
		return runfork.RunForkMaterialization{}, errors.New("selected resume has no current process preparation")
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
	if !availability.Available() || availability.Status != resume.ForkRunStatus ||
		availability.BundleHash != recovered.Operation.Request.TargetBundleHash {
		return runfork.RunForkMaterialization{}, fmt.Errorf("selected resume child bundle differs from durable operation: %s", availability.DetailString())
	}
	action, err := selectedRecoveryActionFor(recovered, runfork.SelectedForkRecoveryEntry{Binding: binding, BundleHash: availability.BundleHash})
	if err != nil {
		return runfork.RunForkMaterialization{}, err
	}
	if (action != selectedRecoveryResume && action != selectedRecoveryActivate) || !sameSelectedForkPointIdentity(prepared.plan.ForkPoint, binding.ForkPoint) ||
		prepared.plan.SourceRunID != binding.SourceRunID || prepared.loadedSource.SourceArtifactFact.BundleHash() != availability.BundleHash {
		return runfork.RunForkMaterialization{}, errors.New("selected resume plan differs from committed child")
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
