package runforkpersistence

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type runForkMaterializedEntitySnapshotMetadataAdmission struct {
	Dispositions []runfork.RunForkReplayResumeDisposition
	Blockers     []runfork.RunForkUnsupportedBlocker
}

type runForkSourceEntityStateMetadata struct {
	FlowInstance string
	EntityType   string
	Slug         string
	Name         string
	Exists       bool
}

func attachRunForkMaterializedEntitySnapshotMetadata(snapshot *runForkRevisionSnapshot, entities []runfork.RunForkEntityState) ([]runfork.RunForkEntityState, runForkMaterializedEntitySnapshotMetadataAdmission, error) {
	out := make([]runfork.RunForkEntityState, len(entities))
	copy(out, entities)
	admission := runForkMaterializedEntitySnapshotMetadataAdmission{}
	for i := range out {
		entityID := strings.TrimSpace(out[i].EntityID)
		if entityID == "" {
			blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerEntitySnapshotMetadataUnproven)
			blocker.Message = "fork materialization requires a reconstructed entity_id before snapshot metadata can be classified"
			admission.Blockers = appendRunForkBlocker(admission.Blockers, blocker)
			admission.Dispositions = append(admission.Dispositions, runForkMaterializedEntitySnapshotMetadataBlockerDisposition("", blocker.Message))
			continue
		}
		metadata, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(snapshot, out[i])
		if !ok {
			blocker := runForkReplayResumeBlocker(runfork.RunForkBlockerEntitySnapshotMetadataUnproven)
			blocker.Message = message
			admission.Blockers = appendRunForkBlocker(admission.Blockers, blocker)
			admission.Dispositions = append(admission.Dispositions, runForkMaterializedEntitySnapshotMetadataBlockerDisposition(entityID, message))
			continue
		}
		out[i].MaterializationMetadata = &metadata
	}
	return out, admission, nil
}

func loadRunForkMaterializedEntitySnapshotMetadata(snapshot *runForkRevisionSnapshot, entity runfork.RunForkEntityState) (runfork.RunForkMaterializedEntitySnapshotMetadata, string, bool) {
	entityID := strings.TrimSpace(entity.EntityID)
	eventFlow := loadRunForkEntityEventFlowInstance(snapshot, entityID)
	sourceState := loadRunForkSourceEntityStateMetadata(snapshot, entityID)

	flowInstance := strings.TrimSpace(eventFlow)
	source := runfork.RunForkMaterializedEntitySnapshotMetadataSourceEvent
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(sourceState.FlowInstance)
		source = runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState
	}
	if !sourceState.Exists {
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision entity metadata for entity %s", entityID), false
	}
	entityType := strings.TrimSpace(sourceState.EntityType)
	if flowInstance == "" || entityType == "" {
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision flow_instance/entity_type metadata for entity %s", entityID), false
	}
	return runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner:        runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
		FlowInstance: flowInstance,
		EntityType:   entityType,
		Slug:         strings.TrimSpace(sourceState.Slug),
		Name:         strings.TrimSpace(sourceState.Name),
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
			Slug:         strings.TrimSpace(fact.Slug),
			Name:         strings.TrimSpace(fact.Name),
			Exists:       true,
		}
	}
	return runForkSourceEntityStateMetadata{}
}

func runForkMaterializedEntitySnapshotMetadataBlockerDisposition(entityID, message string) runfork.RunForkReplayResumeDisposition {
	return runfork.RunForkReplayResumeDisposition{
		Fact:        runfork.RunForkReplayResumeFactEntityStateSnapshot,
		EntityID:    strings.TrimSpace(entityID),
		Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
		Owner:       runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
		BlockerCode: runfork.RunForkBlockerEntitySnapshotMetadataUnproven,
		Message:     strings.TrimSpace(message),
	}
}

func runForkReplayResumeAdmissionWithMaterializedEntitySnapshotMetadata(admission runfork.RunForkReplayResumeAdmission, metadataAdmission runForkMaterializedEntitySnapshotMetadataAdmission) runfork.RunForkReplayResumeAdmission {
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = runfork.RunForkReplayResumeAdmissionOwner
	}
	updatedSnapshotDisposition := false
	for i := range admission.Dispositions {
		disposition := &admission.Dispositions[i]
		if strings.TrimSpace(disposition.Fact) != runfork.RunForkReplayResumeFactEntityStateSnapshot {
			continue
		}
		if strings.TrimSpace(disposition.Disposition) != runfork.RunForkReplayResumeDispositionReconstruct {
			continue
		}
		disposition.Owner = runfork.RunForkMaterializedEntitySnapshotMetadataOwner
		disposition.Message = runfork.RunForkMaterializedEntitySnapshotMetadataOwner + " authorizes reconstructed fork current-state snapshots by carrying source-at-T materialization metadata for every planned entity"
		updatedSnapshotDisposition = true
		break
	}
	if !updatedSnapshotDisposition {
		admission.Dispositions = append(admission.Dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:        runfork.RunForkReplayResumeFactEntityStateSnapshot,
			Disposition: runfork.RunForkReplayResumeDispositionReconstruct,
			Owner:       runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
			Message:     runfork.RunForkMaterializedEntitySnapshotMetadataOwner + " authorizes reconstructed fork current-state snapshots by carrying source-at-T materialization metadata for every planned entity",
		})
	}
	admission.Dispositions = append(admission.Dispositions, metadataAdmission.Dispositions...)
	for _, blocker := range metadataAdmission.Blockers {
		admission.UnsupportedBlockers = appendRunForkBlocker(admission.UnsupportedBlockers, blocker)
	}
	return runfork.RecalculateReplayResumeAdmission(admission)
}
