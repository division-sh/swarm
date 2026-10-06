package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/division-sh/swarm/internal/operatorread"
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
