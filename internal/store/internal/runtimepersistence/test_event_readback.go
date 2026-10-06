package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// ReadSemanticEventFixtureEvidenceForTest keeps record, revision and delivery
// observations in one read snapshot owned by the original selected coordinator.

func ReadSemanticEventFixtureEvidenceForTest(ctx context.Context, selected any, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	var empty SemanticEventFixtureEvidence
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(eventID) == "" {
		return empty, fmt.Errorf("semantic event fixture evidence requires run and event identities")
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return empty, err
	}
	evidence := SemanticEventFixtureEvidence{
		DeliveryProjections: make(map[string][18]string),
		DeliveryStatuses:    make(map[string]string),
	}
	read := func(ctx context.Context, tx *sql.Tx) error {
		if err := readSemanticEventFacts(ctx, tx, string(dialect) == "postgres", runID, eventID, &evidence); err != nil {
			return err
		}
		count, err := runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		if err != nil {
			return err
		}
		evidence.RevisionCount = int(count)
		if err := readSemanticEventPipelineReceipt(ctx, tx, eventID, &evidence); err != nil {
			return err
		}
		evidence.DeliveryProjections, evidence.DeliveryStatuses, err = delivery.ReadSemanticEventDeliveryStorage(ctx, tx, eventID)
		if err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts
			WHERE event_id = $1 AND subscriber_type <> 'platform'`, eventID).Scan(&evidence.NonPlatformReceiptCount)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return empty, err
	}
	return evidence, nil
}

func readSemanticEventFacts(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventID string, evidence *SemanticEventFixtureEvidence) error {
	var err error
	evidence.Record, evidence.RecordFound, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)
	if err != nil {
		return err
	}
	if evidence.RecordFound && evidence.Record.RunID != runID {
		return fmt.Errorf("semantic event fixture %s does not belong to run %s", eventID, runID)
	}
	if err := tx.QueryRowContext(ctx, `SELECT status, COALESCE(event_count, 0) FROM runs WHERE run_id = $1`, runID).
		Scan(&evidence.RunStatus, &evidence.RunEventCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT scope FROM committed_replay_scopes WHERE event_id = $1`, eventID).
		Scan(&evidence.CommittedScope); err == nil {
		evidence.CommittedScopeFound = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	evidence.SettledDeliveryAttemptCount, err = delivery.ReadSemanticEventSettledAttemptCount(ctx, tx, eventID)
	return err
}

func readSemanticEventPipelineReceipt(ctx context.Context, tx *sql.Tx, eventID string, evidence *SemanticEventFixtureEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts
		WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).
		Scan(&evidence.PipelineReceiptCount); err != nil {
		return err
	}
	if evidence.PipelineReceiptCount == 0 {
		return nil
	}
	var failure sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT outcome, COALESCE(reason_code, ''), CAST(failure AS TEXT) FROM event_receipts
		WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).
		Scan(&evidence.PipelineReceiptOutcome, &evidence.PipelineReceiptReason, &failure); err != nil {
		return err
	}
	if failure.Valid {
		if err := json.Unmarshal([]byte(failure.String), &evidence.PipelineReceiptFailure); err != nil {
			return fmt.Errorf("decode pipeline receipt failure: %w", err)
		}
	}
	return nil
}
