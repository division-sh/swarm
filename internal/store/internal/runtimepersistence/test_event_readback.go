package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"strings"
)

func LoadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, error) {
	var empty events.Event
	var record eventrecord.Record
	var found bool
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return empty, fmt.Errorf("canonical event readback requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
				record, found, err = eventrecordpostgres.Load(ctx, tx, eventID)
				return err
			})
		}
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return empty, fmt.Errorf("canonical event readback requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
				record, found, err = eventrecordsqlite.Load(ctx, tx, eventID)
				return err
			})
		}
	default:
		return empty, fmt.Errorf("canonical event readback store %T is unsupported", selected)
	}
	if err != nil {
		return empty, err
	}
	if !found {
		return empty, fmt.Errorf("canonical event record %s is missing", eventID)
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, err
	}
	return admitted.Event(), nil
}

type SemanticEventFixtureEvidence struct {
	Record                      eventrecord.Record
	RecordFound                 bool
	RevisionCount               int
	PipelineReceiptCount        int
	PipelineReceiptOutcome      string
	PipelineReceiptReason       string
	PipelineReceiptFailure      *runtimefailures.Envelope
	CommittedScope              string
	CommittedScopeFound         bool
	RunStatus                   string
	RunEventCount               int
	SettledDeliveryAttemptCount int
	DeliveryProjections         map[string][18]string
	DeliveryStatuses            map[string]string
	NonPlatformReceiptCount     int
}

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
		if err := readSemanticEventPipelineReceipt(ctx, tx, eventID, &evidence); err != nil {
			return err
		}
		var err error
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
	if postgres {
		evidence.Record, evidence.RecordFound, err = eventrecordpostgres.Load(ctx, tx, eventID)
	} else {
		evidence.Record, evidence.RecordFound, err = eventrecordsqlite.Load(ctx, tx, eventID)
	}
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
	if err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = $1`, runID).Scan(&evidence.RevisionCount)
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
