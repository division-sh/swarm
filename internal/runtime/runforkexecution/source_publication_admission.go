package runforkexecution

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func selectedSourceInput(prepared *PreparedSelectedFork, source runfork.RunForkSelectedContractSourceEvent) (bus.SelectedInputValidation, error) {
	input := prepared.inputs[source.SourceEventID]
	_, original := source.InputPublication.Event()
	if original != input.Present() {
		return bus.SelectedInputValidation{}, fmt.Errorf("selected input publication changed after preparation")
	}
	if !original {
		return input, nil
	}
	fingerprint, err := selectedPreparationFingerprint(source.InputPublication.Coordinates())
	if err != nil || fingerprint != prepared.inputCoordinates[source.SourceEventID] {
		return bus.SelectedInputValidation{}, fmt.Errorf("selected input evidence changed after preparation: %v", err)
	}
	return input.WithStoreProjectedPayload(source.Payload)
}

// Validate immutable source/schema facts before activation. This does not plan
// routes, claim a publication, persist an event or execute a receiver.
func admitPreparedSourcePublications(ctx context.Context, eventBus *bus.EventBus, prepared *PreparedSelectedFork, sources []runfork.RunForkSelectedContractSourceEvent, sourceRun, childRun, authority string) error {
	for _, source := range sources {
		input, err := selectedSourceInput(prepared, source)
		if err != nil {
			return err
		}
		event, err := selectedContractForkEvent(sourceRun, childRun, activityidentity.ForkLineageEventID(childRun, source.SourceEventID), source, authority)
		if err != nil {
			return err
		}
		if _, _, err := eventBus.AdmitSelectedForkPublishInput(ctx, event, input); err != nil {
			return fmt.Errorf("admit prepared selected source %s: %w", source.SourceEventID, err)
		}
	}
	return nil
}
