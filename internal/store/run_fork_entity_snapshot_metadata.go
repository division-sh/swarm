package store

import (
	"fmt"
	"strings"
)

const (
	RunForkMaterializedEntitySnapshotMetadataOwner = "runtime.run_fork.materialized_entity_snapshot_metadata"

	RunForkMaterializedEntitySnapshotMetadataSourceEvent       = "source_event"
	RunForkMaterializedEntitySnapshotMetadataSourceEntityState = "source_entity_state"
)

type RunForkMaterializedEntitySnapshotMetadata struct {
	Owner        string `json:"owner"`
	FlowInstance string `json:"flow_instance"`
	EntityType   string `json:"entity_type"`
	Source       string `json:"source"`
}

type runForkMaterializedEntitySnapshotMetadataAdmission struct {
	Dispositions []RunForkReplayResumeDisposition
	Blockers     []RunForkUnsupportedBlocker
}

type runForkSourceEntityStateMetadata struct {
	FlowInstance string
	EntityType   string
	Exists       bool
}

func attachRunForkMaterializedEntitySnapshotMetadata(snapshot *runForkRevisionSnapshot, entities []RunForkEntityState) ([]RunForkEntityState, runForkMaterializedEntitySnapshotMetadataAdmission, error) {
	out := make([]RunForkEntityState, len(entities))
	copy(out, entities)
	admission := runForkMaterializedEntitySnapshotMetadataAdmission{}
	for i := range out {
		entityID := strings.TrimSpace(out[i].EntityID)
		if entityID == "" {
			blocker := runForkReplayResumeBlocker(RunForkBlockerEntitySnapshotMetadataUnproven)
			blocker.Message = "fork materialization requires a reconstructed entity_id before snapshot metadata can be classified"
			admission.Blockers = appendRunForkBlocker(admission.Blockers, blocker)
			admission.Dispositions = append(admission.Dispositions, runForkMaterializedEntitySnapshotMetadataBlockerDisposition("", blocker.Message))
			continue
		}
		metadata, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, out[i])
		if !ok {
			blocker := runForkReplayResumeBlocker(RunForkBlockerEntitySnapshotMetadataUnproven)
			blocker.Message = message
			admission.Blockers = appendRunForkBlocker(admission.Blockers, blocker)
			admission.Dispositions = append(admission.Dispositions, runForkMaterializedEntitySnapshotMetadataBlockerDisposition(entityID, message))
			continue
		}
		out[i].MaterializationMetadata = &metadata
	}
	return out, admission, nil
}

func loadRunForkMaterializedEntitySnapshotMetadata(snapshot *runForkRevisionSnapshot, entity RunForkEntityState) (RunForkMaterializedEntitySnapshotMetadata, string, bool) {
	entityID := strings.TrimSpace(entity.EntityID)
	eventFlow := loadRunForkEntityEventFlowInstance(snapshot, entityID)
	sourceState := loadRunForkSourceEntityStateMetadata(snapshot, entityID)

	flowInstance := strings.TrimSpace(eventFlow)
	source := RunForkMaterializedEntitySnapshotMetadataSourceEvent
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(sourceState.FlowInstance)
		source = RunForkMaterializedEntitySnapshotMetadataSourceEntityState
	}
	if !sourceState.Exists {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision entity metadata for entity %s", entityID), false
	}
	entityType := strings.TrimSpace(sourceState.EntityType)
	if reconstructedEntityType := stringFieldValue(entity.Fields, "entity_type"); reconstructedEntityType != "" && reconstructedEntityType != entityType {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot reconcile reconstructed entity_type %q with source-at-revision entity metadata %q for entity %s", reconstructedEntityType, entityType, entityID), false
	}
	if flowInstance == "" || entityType == "" {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision flow_instance/entity_type metadata for entity %s", entityID), false
	}
	return RunForkMaterializedEntitySnapshotMetadata{
		Owner:        RunForkMaterializedEntitySnapshotMetadataOwner,
		FlowInstance: flowInstance,
		EntityType:   entityType,
		Source:       source,
	}, "", true
}

func loadRunForkEntityEventFlowInstance(snapshot *runForkRevisionSnapshot, entityID string) string {
	if snapshot == nil {
		return ""
	}
	for index := len(snapshot.Events) - 1; index >= 0; index-- {
		event := snapshot.Events[index]
		if strings.TrimSpace(event.EntityID) == strings.TrimSpace(entityID) && strings.TrimSpace(event.FlowInstance) != "" {
			return strings.TrimSpace(event.FlowInstance)
		}
	}
	return ""
}

func loadRunForkSourceEntityStateMetadata(snapshot *runForkRevisionSnapshot, entityID string) runForkSourceEntityStateMetadata {
	if snapshot == nil {
		return runForkSourceEntityStateMetadata{}
	}
	for _, fact := range snapshot.EntityMetadata {
		if strings.TrimSpace(fact.EntityID) != strings.TrimSpace(entityID) {
			continue
		}
		entityType := strings.TrimSpace(fact.EntityType)
		if entityType == "" {
			entityType = "default"
		}
		return runForkSourceEntityStateMetadata{
			FlowInstance: strings.TrimSpace(fact.FlowInstance),
			EntityType:   entityType,
			Exists:       true,
		}
	}
	return runForkSourceEntityStateMetadata{}
}

func runForkMaterializedEntitySnapshotMetadataBlockerDisposition(entityID, message string) RunForkReplayResumeDisposition {
	return RunForkReplayResumeDisposition{
		Fact:        RunForkReplayResumeFactEntityStateSnapshot,
		EntityID:    strings.TrimSpace(entityID),
		Disposition: RunForkReplayResumeDispositionFailClosedBlocker,
		Owner:       RunForkMaterializedEntitySnapshotMetadataOwner,
		BlockerCode: RunForkBlockerEntitySnapshotMetadataUnproven,
		Message:     strings.TrimSpace(message),
	}
}

func runForkReplayResumeAdmissionWithMaterializedEntitySnapshotMetadata(admission RunForkReplayResumeAdmission, metadataAdmission runForkMaterializedEntitySnapshotMetadataAdmission) RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = RunForkReplayResumeAdmissionOwner
	}
	updatedSnapshotDisposition := false
	for i := range admission.Dispositions {
		disposition := &admission.Dispositions[i]
		if strings.TrimSpace(disposition.Fact) != RunForkReplayResumeFactEntityStateSnapshot {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != RunForkReplayResumeDispositionReconstruct {
			continue
		}
		disposition.Owner = RunForkMaterializedEntitySnapshotMetadataOwner
		disposition.Message = RunForkMaterializedEntitySnapshotMetadataOwner + " authorizes reconstructed fork current-state snapshots by carrying source-at-T materialization metadata for every planned entity"
		updatedSnapshotDisposition = true
		break
	}
	if !updatedSnapshotDisposition {
		admission.Dispositions = append(admission.Dispositions, RunForkReplayResumeDisposition{
			Fact:        RunForkReplayResumeFactEntityStateSnapshot,
			Disposition: RunForkReplayResumeDispositionReconstruct,
			Owner:       RunForkMaterializedEntitySnapshotMetadataOwner,
			Message:     RunForkMaterializedEntitySnapshotMetadataOwner + " authorizes reconstructed fork current-state snapshots by carrying source-at-T materialization metadata for every planned entity",
		})
	}
	admission.Dispositions = append(admission.Dispositions, metadataAdmission.Dispositions...)
	for _, blocker := range metadataAdmission.Blockers {
		admission.UnsupportedBlockers = appendRunForkBlocker(admission.UnsupportedBlockers, blocker)
	}
	return runForkReplayResumeAdmissionRecalculateReadiness(admission)
}
