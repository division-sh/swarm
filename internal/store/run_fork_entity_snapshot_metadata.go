package store

import (
	"context"
	"database/sql"
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

func (s *PostgresStore) attachRunForkMaterializedEntitySnapshotMetadata(ctx context.Context, sourceRunID string, cursor runForkEventCursor, entities []RunForkEntityState) ([]RunForkEntityState, runForkMaterializedEntitySnapshotMetadataAdmission, error) {
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
		metadata, message, ok, err := s.loadRunForkMaterializedEntitySnapshotMetadata(ctx, sourceRunID, cursor, out[i])
		if err != nil {
			return nil, runForkMaterializedEntitySnapshotMetadataAdmission{}, err
		}
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

func (s *PostgresStore) loadRunForkMaterializedEntitySnapshotMetadata(ctx context.Context, sourceRunID string, cursor runForkEventCursor, entity RunForkEntityState) (RunForkMaterializedEntitySnapshotMetadata, string, bool, error) {
	entityID := strings.TrimSpace(entity.EntityID)
	eventFlow, err := s.loadRunForkEntityEventFlowInstance(ctx, sourceRunID, cursor, entityID)
	if err != nil {
		return RunForkMaterializedEntitySnapshotMetadata{}, "", false, err
	}
	sourceState, err := s.loadRunForkSourceEntityStateMetadata(ctx, sourceRunID, cursor, entityID)
	if err != nil {
		return RunForkMaterializedEntitySnapshotMetadata{}, "", false, err
	}

	flowInstance := strings.TrimSpace(eventFlow)
	source := RunForkMaterializedEntitySnapshotMetadataSourceEvent
	if flowInstance == "" {
		flowInstance = strings.TrimSpace(sourceState.FlowInstance)
		source = RunForkMaterializedEntitySnapshotMetadataSourceEntityState
	}
	if !sourceState.Exists {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-T entity_state metadata for entity %s", entityID), false, nil
	}
	entityType := strings.TrimSpace(sourceState.EntityType)
	if reconstructedEntityType := stringFieldValue(entity.Fields, "entity_type"); reconstructedEntityType != "" && reconstructedEntityType != entityType {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot reconcile reconstructed entity_type %q with source-at-T entity_state entity_type %q for entity %s", reconstructedEntityType, entityType, entityID), false, nil
	}
	if flowInstance == "" || entityType == "" {
		return RunForkMaterializedEntitySnapshotMetadata{}, fmt.Sprintf("fork materialization cannot prove source-at-T flow_instance/entity_type metadata for entity %s", entityID), false, nil
	}
	return RunForkMaterializedEntitySnapshotMetadata{
		Owner:        RunForkMaterializedEntitySnapshotMetadataOwner,
		FlowInstance: flowInstance,
		EntityType:   entityType,
		Source:       source,
	}, "", true, nil
}

func (s *PostgresStore) loadRunForkEntityEventFlowInstance(ctx context.Context, sourceRunID string, cursor runForkEventCursor, entityID string) (string, error) {
	var flowInstance string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM events
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		  AND COALESCE(flow_instance, '') <> ''
		  AND (created_at, event_id) <= ($3::timestamptz, $4::uuid)
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, sourceRunID, entityID, cursor.CreatedAt, cursor.EventID).Scan(&flowInstance)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load fork-point event flow_instance metadata for %s: %w", entityID, err)
	}
	return strings.TrimSpace(flowInstance), nil
}

func (s *PostgresStore) loadRunForkSourceEntityStateMetadata(ctx context.Context, sourceRunID string, cursor runForkEventCursor, entityID string) (runForkSourceEntityStateMetadata, error) {
	var metadata runForkSourceEntityStateMetadata
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, ''), COALESCE(NULLIF(entity_type, ''), 'default')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		  AND created_at <= $3::timestamptz
	`, sourceRunID, entityID, cursor.CreatedAt).Scan(&metadata.FlowInstance, &metadata.EntityType)
	if err == sql.ErrNoRows {
		return runForkSourceEntityStateMetadata{}, nil
	}
	if err != nil {
		return runForkSourceEntityStateMetadata{}, fmt.Errorf("load source entity_state metadata for %s: %w", entityID, err)
	}
	metadata.FlowInstance = strings.TrimSpace(metadata.FlowInstance)
	metadata.EntityType = strings.TrimSpace(metadata.EntityType)
	if metadata.EntityType == "" {
		metadata.EntityType = "default"
	}
	metadata.Exists = true
	return metadata, nil
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
