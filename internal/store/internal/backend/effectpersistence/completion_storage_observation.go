package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type CompletionAttemptStorage struct {
	OperationID, AttemptID, Adapter, Kind, State string
	Ordinal                                      int
	TargetKind, TargetID                         string
	TargetOrdinal                                int
	Targets                                      []CompletionTargetStorage
	Spend                                        []CompletionSpendStorage
}

type CompletionTargetStorage struct {
	RowID, TargetID, AttemptID string
	Ordinal                    int
}

type CompletionSpendStorage struct {
	LedgerID, AttemptID string
	Evidence            runtimeeffects.CompletionEvidence
}

func (s *EffectPostgresOwner) ReadCompletionAttemptStorageForTest(ctx context.Context, adapter, attemptID string) ([]CompletionAttemptStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("completion observation requires the original postgres effect owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readCompletionAttemptStorageForTest(ctx, s.backend.RunReadTransaction, adapter, attemptID)
}

func (s *EffectSQLiteOwner) ReadCompletionAttemptStorageForTest(ctx context.Context, adapter, attemptID string) ([]CompletionAttemptStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("completion observation requires the original sqlite effect owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readCompletionAttemptStorageForTest(ctx, s.backend.RunReadTransaction, adapter, attemptID)
}

func readCompletionAttemptStorageForTest(ctx context.Context, read func(context.Context, func(context.Context, *sql.Tx) error) error, adapter, attemptID string) ([]CompletionAttemptStorage, error) {
	if adapter == "" || strings.TrimSpace(adapter) != adapter {
		return nil, fmt.Errorf("completion observation requires an exact adapter")
	}
	if attemptID != "" {
		id, err := uuid.Parse(attemptID)
		if err != nil || id == uuid.Nil || id.String() != attemptID {
			return nil, fmt.Errorf("completion observation requires an exact attempt identity")
		}
	}
	var evidence []CompletionAttemptStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(o.operation_id AS TEXT),CAST(a.attempt_id AS TEXT),
			a.attempt_ordinal,a.adapter,o.effect_kind,a.state,COALESCE(a.usage_target_kind,''),
			COALESCE(CAST(a.usage_target_id AS TEXT),''),COALESCE(a.target_ordinal,0)
			FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
			WHERE a.adapter=$1 AND ($2='' OR CAST(a.attempt_id AS TEXT)=$2)
			ORDER BY a.attempt_ordinal,a.attempt_id`, adapter, attemptID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row CompletionAttemptStorage
			if err := rows.Scan(&row.OperationID, &row.AttemptID, &row.Ordinal, &row.Adapter, &row.Kind, &row.State, &row.TargetKind, &row.TargetID, &row.TargetOrdinal); err != nil {
				return err
			}
			evidence = append(evidence, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if attemptID != "" && len(evidence) != 1 {
			return fmt.Errorf("completion observation attempt %s is absent for adapter %s", attemptID, adapter)
		}
		for i := range evidence {
			if err := readCompletionTargetsForTest(ctx, tx, &evidence[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return evidence, nil
}

func readCompletionTargetsForTest(ctx context.Context, tx *sql.Tx, out *CompletionAttemptStorage) error {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(turn_id AS TEXT),CAST(turn_id AS TEXT),CAST(completion_attempt_id AS TEXT),0
		FROM agent_turns WHERE completion_attempt_id=$1
		UNION ALL SELECT CAST(completion_id AS TEXT),CAST(fork_turn_id AS TEXT),CAST(attempt_id AS TEXT),ordinal
		FROM conversation_fork_turn_completions WHERE attempt_id=$1 ORDER BY 1`, out.AttemptID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row CompletionTargetStorage
		if err := rows.Scan(&row.RowID, &row.TargetID, &row.AttemptID, &row.Ordinal); err != nil {
			return fmt.Errorf("completion target read: %w", err)
		}
		out.Targets = append(out.Targets, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT CAST(ledger_id AS TEXT),CAST(external_effect_attempt_id AS TEXT),CAST(completion_evidence AS TEXT)
		FROM spend_ledger WHERE external_effect_attempt_id=$1 ORDER BY ledger_id`, out.AttemptID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var row CompletionSpendStorage
		var raw []byte
		if err := rows.Scan(&row.LedgerID, &row.AttemptID, &raw); err != nil {
			return err
		}
		row.Evidence, err = runtimeeffects.DecodeCompletionEvidence(raw)
		if err != nil {
			return err
		}
		out.Spend = append(out.Spend, row)
	}
	return rows.Err()
}
