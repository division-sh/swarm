package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type EntityMutationEvidence struct {
	operatorread.RunDebugMutation
	EventName       string
	RegisteredAgent bool
}

func (s *PipelinePostgresOwner) ObserveMutationHistoryForTest(ctx context.Context, runID string) ([]EntityMutationEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []EntityMutationEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeMutationHistory(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveMutationHistoryForTest(ctx context.Context, runID string) ([]EntityMutationEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []EntityMutationEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeMutationHistory(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeMutationHistory(ctx context.Context, tx *sql.Tx, runID string) ([]EntityMutationEvidence, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT m.mutation_id,m.entity_id,m.domain,m.path,
		COALESCE(CAST(m.new_value AS TEXT),'null'),COALESCE(CAST(m.old_value AS TEXT),'null'),
		COALESCE(m.writer_type,''),COALESCE(m.writer_id,''),COALESCE(m.handler_step,''),
		COALESCE(CAST(m.caused_by_event AS TEXT),''),COALESCE(e.event_name,''),
		EXISTS(SELECT 1 FROM agents a WHERE a.run_id=m.run_id AND a.agent_id=m.writer_id),m.created_at
		FROM entity_mutations m LEFT JOIN events e ON e.event_id=m.caused_by_event
		WHERE m.run_id=$1 ORDER BY m.created_at DESC,m.mutation_id DESC`, runID)
	if err != nil {
		return nil, err
	}
	var out []EntityMutationEvidence
	for rows.Next() {
		var row EntityMutationEvidence
		var value, old []byte
		var created any
		if err := rows.Scan(&row.MutationID, &row.EntityID, &row.Domain, &row.Path, &value, &old, &row.WriterType, &row.WriterID, &row.HandlerStep, &row.CausedByEvent, &row.EventName, &row.RegisteredAgent, &created); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.CreatedAt, _, err = sqliteTimeValue(created)
		if err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.NewValue, row.OldValue = json.RawMessage(value), json.RawMessage(old)
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type CardContentionEvidence struct{ Requests, DecidedChanges int }

func (s *PipelinePostgresOwner) ObserveCardContentionForTest(ctx context.Context, key, cardID string) (CardContentionEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return CardContentionEvidence{}, err
	}
	var out CardContentionEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeCardContention(ctx, tx, key, cardID, &out) })
	if err != nil {
		return CardContentionEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveCardContentionForTest(ctx context.Context, key, cardID string) (CardContentionEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return CardContentionEvidence{}, err
	}
	var out CardContentionEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeCardContention(ctx, tx, key, cardID, &out) })
	if err != nil {
		return CardContentionEvidence{}, err
	}
	return out, nil
}

func observeCardContention(ctx context.Context, tx *sql.Tx, key, cardID string, out *CardContentionEvidence) error {
	if _, err := uuid.Parse(cardID); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM api_idempotency WHERE idempotency_key=$1),(SELECT COUNT(*) FROM decision_card_changes WHERE card_id=$2 AND change_type='decided')`, key, cardID).Scan(&out.Requests, &out.DecidedChanges)
}

func (s *PipelinePostgresOwner) AdvanceGateHeaderRevisionForTest(ctx context.Context, state pipeline.WorkflowEngineStateRecord) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return advanceGateHeaderRevision(ctx, tx, state) })
}

func (s *PipelineSQLiteOwner) AdvanceGateHeaderRevisionForTest(ctx context.Context, state pipeline.WorkflowEngineStateRecord) error {
	return s.backend.RunTransaction(ctx, "gate header contention fixture", func(ctx context.Context, tx *sql.Tx) error { return advanceGateHeaderRevision(ctx, tx, state) })
}

func advanceGateHeaderRevision(ctx context.Context, tx *sql.Tx, state pipeline.WorkflowEngineStateRecord) error {
	result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND entity_id=$2 AND revision=$3`, state.Identity.RunID, state.EntityID, state.ExpectedRevision)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("CAS fault did not change exact header: rows=%d", n)
	}
	return nil
}

type WriterStageEvidence struct{ EntityID, State string }

func (s *PipelinePostgresOwner) ObserveWriterStageForTest(ctx context.Context, runID, entityID, stage string) (WriterStageEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterStageEvidence{}, err
	}
	var out WriterStageEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeWriterStage(ctx, tx, runID, entityID, stage, &out)
	})
	if err != nil {
		return WriterStageEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveWriterStageForTest(ctx context.Context, runID, entityID, stage string) (WriterStageEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterStageEvidence{}, err
	}
	var out WriterStageEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeWriterStage(ctx, tx, runID, entityID, stage, &out)
	})
	if err != nil {
		return WriterStageEvidence{}, err
	}
	return out, nil
}

func observeWriterStage(ctx context.Context, tx *sql.Tx, runID, entityID, stage string, out *WriterStageEvidence) error {
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	var err error
	if entityID != "" {
		if _, err := uuid.Parse(entityID); err != nil {
			return err
		}
		out.EntityID = entityID
		err = tx.QueryRowContext(ctx, `SELECT current_state FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&out.State)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT entity_id,current_state FROM flow_instances WHERE run_id=$1 AND current_state=$2 ORDER BY created_at,entity_id LIMIT 1`, runID, stage).Scan(&out.EntityID, &out.State)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

func (s *PipelinePostgresOwner) ObservePendingFixtureCardForTest(ctx context.Context, runID string) (string, error) {
	var id string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, runID).Scan(&id)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *PipelineSQLiteOwner) ObservePendingFixtureCardForTest(ctx context.Context, runID string) (string, error) {
	var id string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, runID).Scan(&id)
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

type WriterFlowEvidence struct {
	InstancePath string
	Config       json.RawMessage
}

func (s *PipelinePostgresOwner) ObserveWriterFlowForTest(ctx context.Context, runID, entityID string) (WriterFlowEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterFlowEvidence{}, err
	}
	var out WriterFlowEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeWriterFlow(ctx, tx, runID, entityID, &out) })
	if err != nil {
		return WriterFlowEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveWriterFlowForTest(ctx context.Context, runID, entityID string) (WriterFlowEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return WriterFlowEvidence{}, err
	}
	var out WriterFlowEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeWriterFlow(ctx, tx, runID, entityID, &out) })
	if err != nil {
		return WriterFlowEvidence{}, err
	}
	return out, nil
}

func observeWriterFlow(ctx context.Context, tx *sql.Tx, runID, entityID string, out *WriterFlowEvidence) error {
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	if entityID == "" {
		return tx.QueryRowContext(ctx, `SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template='hub'`, runID).Scan(&out.InstancePath)
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT instance_path,CAST(config AS TEXT) FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&out.InstancePath, &raw); err != nil {
		return err
	}
	out.Config = json.RawMessage(raw)
	return nil
}
