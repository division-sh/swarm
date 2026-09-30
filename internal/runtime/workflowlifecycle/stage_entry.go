package workflowlifecycle

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

const stageEntryBookkeepingKey = "stage_entry"

func (e Effect) WithExecutionOccurrence(kind, id string) (Effect, error) {
	if e.kind != KindAcceptedEvent || id == "" || (kind != "delivery" && kind != "timer" && kind != "gate") {
		return Effect{}, fmt.Errorf("accepted lifecycle effect requires its exact admitted execution occurrence")
	}
	e.occurrenceKind, e.occurrenceID = kind, id
	return e, nil
}

func (e Effect) StageEntry(owner flowidentity.RunScopedFlowInstance) (timeridentity.StageEntryRef, bool, error) {
	if err := owner.Validate(); err != nil {
		return timeridentity.StageEntryRef{}, false, err
	}
	if owner.Route != e.route {
		return timeridentity.StageEntryRef{}, false, fmt.Errorf("stage entry effect disagrees with lifecycle instance")
	}
	ref := timeridentity.StageEntryRef{
		RunID: owner.RunID, FlowScope: owner.Route.ScopeKey, InstanceID: owner.Route.InstanceID,
		InstancePath: owner.Route.InstancePath, EntityID: e.entityID.String(),
	}
	switch e.kind {
	case KindInitialEntry:
		ref.Stage, ref.Cause = e.stage, "construction"
	case KindAcceptedEvent:
		if e.transition == nil {
			return timeridentity.StageEntryRef{}, false, nil
		}
		ref.Stage, ref.Cause = e.transition.To(), e.occurrenceKind
		ref.EventID, ref.OccurrenceID, ref.TransitionID = e.eventID, e.occurrenceID, e.transition.ID()
	default:
		return timeridentity.StageEntryRef{}, false, fmt.Errorf("unsupported lifecycle entry effect")
	}
	return ref, true, ref.Validate()
}

func StoreStageEntry(bookkeeping map[string]any, ref timeridentity.StageEntryRef) error {
	if bookkeeping == nil {
		return fmt.Errorf("stage entry requires lifecycle bookkeeping")
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	bookkeeping[stageEntryBookkeepingKey] = ref
	return nil
}

func LoadStageEntry(bookkeeping map[string]any) (timeridentity.StageEntryRef, bool, error) {
	raw, found := bookkeeping[stageEntryBookkeepingKey]
	if !found {
		return timeridentity.StageEntryRef{}, false, nil
	}
	ref, err := timeridentity.StageEntryRefFromValue(raw)
	return ref, true, err
}
