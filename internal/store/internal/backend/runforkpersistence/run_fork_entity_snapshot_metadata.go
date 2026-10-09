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

// Runnable historical consumers need both sides of the same fixed snapshot.
// A header-only lookup cannot validate state; imported state cannot supply it.
func loadRunForkConstructedEntityState(snapshot *runForkRevisionSnapshot, entityID string) (runfork.RunForkEntityState, error) {
	if snapshot == nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("constructed entity requires a fixed-revision snapshot")
	}
	states, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	states, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, states)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	for _, state := range states {
		if state.EntityID != entityID {
			continue
		}
		metadata := state.MaterializationMetadata
		if metadata == nil {
			return runfork.RunForkEntityState{}, fmt.Errorf("entity %s has no admitted fixed-revision header metadata", entityID)
		}
		if metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			return runfork.RunForkEntityState{}, fmt.Errorf("entity %s requires constructed-header evidence, not imported state", entityID)
		}
		return state, nil
	}
	return runfork.RunForkEntityState{}, fmt.Errorf("entity %s has no reconstructed state in the fixed revision", entityID)
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
		if metadata.Source == runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			entered := metadata.EnteredStateAt
			out[i].EnteredStateAt = &entered
		}
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
	source := runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState
	switch sourceState.ConstructionKind {
	case "constructed":
		source = runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance
		if len(sourceState.FlowConfig) == 0 || sourceState.CreatedAt.IsZero() || sourceState.UpdatedAt.IsZero() || sourceState.EnteredStateAt.IsZero() || sourceState.Status == "" ||
			sourceState.CurrentState == "" || sourceState.CurrentState != entity.CurrentState || sourceState.FlowTemplate == "" || (sourceState.Mode != "static" && sourceState.Mode != "template") ||
			(sourceState.Status == "terminated") != !sourceState.TerminatedAt.IsZero() {
			return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove constructed header metadata for entity %s", entityID), false
		}
	case "imported_state":
		if entityType == "" {
			return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization imported state has no entity type for %s", entityID), false
		}
	default:
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization has unknown construction evidence for entity %s", entityID), false
	}
	if flowInstance == "" {
		return runfork.RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-revision flow_instance/entity_type metadata for entity %s", entityID), false
	}
	return runfork.RunForkMaterializedEntitySnapshotMetadata{
		FlowConfig:     append([]byte(nil), sourceState.FlowConfig...),
		Owner:          runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
		FlowInstance:   flowInstance,
		ParentInstance: sourceState.ParentInstance,
		InstanceKey:    sourceState.InstanceKey,
		EntityType:     entityType,
		Slug:           strings.TrimSpace(sourceState.Slug),
		Name:           strings.TrimSpace(sourceState.Name),
		Source:         source,
		StageDefined:   sourceState.StageDefined,
		FlowTemplate:   sourceState.FlowTemplate,
		Mode:           sourceState.Mode,
		Status:         sourceState.Status,
		CreatedAt:      sourceState.CreatedAt,
		UpdatedAt:      sourceState.UpdatedAt,
		TerminatedAt:   sourceState.TerminatedAt,
		EnteredStateAt: sourceState.EnteredStateAt,
	}, "", true
}

// The relational parent foreign key requires the admitted parent header first.
// Order by recorded construction facts, never by path shape or entity hashes.
func runForkEntitiesInConstructionOrder(entities []runfork.RunForkEntityState) ([]runfork.RunForkEntityState, error) {
	byPath := make(map[string]int, len(entities))
	for index, entity := range entities {
		metadata := entity.MaterializationMetadata
		if metadata == nil {
			return nil, fmt.Errorf("historical entity %s requires admitted metadata", entity.EntityID)
		}
		if metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
			continue
		}
		if _, duplicate := byPath[metadata.FlowInstance]; duplicate {
			return nil, fmt.Errorf("historical construction has duplicate path %s", metadata.FlowInstance)
		}
		byPath[metadata.FlowInstance] = index
	}
	visiting, done := make([]bool, len(entities)), make([]bool, len(entities))
	ordered := make([]runfork.RunForkEntityState, 0, len(entities))
	var visit func(int) error
	visit = func(index int) error {
		if done[index] {
			return nil
		}
		if visiting[index] {
			return fmt.Errorf("historical construction has cyclic parent ancestry")
		}
		visiting[index] = true
		metadata := entities[index].MaterializationMetadata
		if metadata.ParentInstance != "" {
			parent, found := byPath[metadata.ParentInstance]
			if !found {
				return fmt.Errorf("historical construction %s has no admitted parent %s", metadata.FlowInstance, metadata.ParentInstance)
			}
			if err := visit(parent); err != nil {
				return err
			}
		}
		done[index] = true
		ordered = append(ordered, entities[index])
		return nil
	}
	for index := range entities {
		if err := visit(index); err != nil {
			return nil, err
		}
	}
	return ordered, nil
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
