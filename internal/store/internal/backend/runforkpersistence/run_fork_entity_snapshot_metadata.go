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

func attachRunForkMaterializedEntitySnapshotMetadata(snapshot *runForkRevisionSnapshot, entities []runfork.RunForkEntityState) ([]runfork.RunForkEntityState, runForkMaterializedEntitySnapshotMetadataAdmission, error) {
	if err := validateRunForkEntityMetadataOwners(snapshot); err != nil {
		return nil, runForkMaterializedEntitySnapshotMetadataAdmission{}, err
	}
	seen := make(map[string]struct{}, len(entities))
	for _, entity := range entities {
		entityID := strings.TrimSpace(entity.EntityID)
		if _, duplicate := seen[entityID]; duplicate {
			return nil, runForkMaterializedEntitySnapshotMetadataAdmission{}, runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("duplicate reconstructed entity owner %q", entityID))
		}
		seen[entityID] = struct{}{}
	}
	out := make([]runfork.RunForkEntityState, len(entities))
	copy(out, entities)
	admission := runForkMaterializedEntitySnapshotMetadataAdmission{}
	for i := range out {
		out[i].MaterializationMetadata = nil
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
	if err := validateRunForkEntityMetadataOwners(snapshot); err != nil {
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, err.Error(), false
	}
	var sourceState *runForkRevisionEntityMetadata
	if snapshot != nil {
		for i := range snapshot.EntityMetadata {
			if strings.TrimSpace(snapshot.EntityMetadata[i].EntityID) == entityID {
				sourceState = &snapshot.EntityMetadata[i]
				break
			}
		}
	}
	if sourceState == nil {
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision entity metadata for entity %s", entityID), false
	}
	flowInstance := strings.TrimSpace(sourceState.FlowInstance)
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
		Source:       runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState,
	}, "", true
}

func validateRunForkEntityMetadataOwners(snapshot *runForkRevisionSnapshot) error {
	if snapshot == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(snapshot.EntityMetadata))
	for _, fact := range snapshot.EntityMetadata {
		entityID := strings.TrimSpace(fact.EntityID)
		if entityID == "" {
			return runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, "snapshot metadata requires an exact entity owner")
		}
		if _, duplicate := seen[entityID]; duplicate {
			return runForkReplayResumeError(runfork.RunForkBlockerEntitySnapshotMetadataUnproven, runfork.RunForkReplayResumeFactEntityStateSnapshot, fmt.Sprintf("duplicate snapshot metadata entity owner %q", entityID))
		}
		seen[entityID] = struct{}{}
	}
	return nil
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
