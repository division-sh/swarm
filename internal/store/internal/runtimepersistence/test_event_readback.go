package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
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
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
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
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts a
			JOIN event_deliveries d ON d.delivery_id = a.delivery_id
			WHERE d.event_id = $1 AND a.closure_kind = 'settled'`, eventID).
			Scan(&evidence.SettledDeliveryAttemptCount); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = $1`, runID).Scan(&evidence.RevisionCount); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts
			WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).
			Scan(&evidence.PipelineReceiptCount); err != nil {
			return err
		}
		if evidence.PipelineReceiptCount != 0 {
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
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT delivery_id, status, route_identity, subscriber_type, subscriber_id,
				agent_name_owner, agent_name_source, agent_route_presence,
				agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
				CAST(delivery_target_route AS TEXT), CAST(delivery_context AS TEXT),
				CAST(delivery_payload_projection AS TEXT), CAST(connect_execution_claim AS TEXT),
				CAST(receiver_materialization_plan AS TEXT), execution_authority_kind,
				authority_bundle_hash, execution_authority_id, CAST(execution_authority_generation AS TEXT)
			FROM event_deliveries WHERE event_id = $1`, eventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var deliveryID, status string
			var projection [18]string
			args := []any{&deliveryID, &status}
			for index := range projection {
				args = append(args, &projection[index])
			}
			if err := rows.Scan(args...); err != nil {
				return err
			}
			evidence.DeliveryProjections[deliveryID] = projection
			evidence.DeliveryStatuses[deliveryID] = status
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
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

// LoadCanonicalEventRecordForTest exposes only decoded evidence. Record and
// inherited-owner validation share the selected backend's read snapshot.

type ReplyReturnStorageEvidence struct {
	Contexts, EventLinkedDeadLetters int
}

func ReadReplyReturnStorageForTest(ctx context.Context, selected any, runID string) (ReplyReturnStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return ReplyReturnStorageEvidence{}, fmt.Errorf("reply return evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return ReplyReturnStorageEvidence{}, err
	}
	var evidence ReplyReturnStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM reply_contexts WHERE run_id=$1),
			(SELECT COUNT(*) FROM dead_letters d JOIN events e ON e.event_id=d.original_event_id WHERE e.run_id=$1)`, runID).
			Scan(&evidence.Contexts, &evidence.EventLinkedDeadLetters)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return ReplyReturnStorageEvidence{}, err
	}
	return evidence, nil
}

// SemanticEventFixtureEvidence is exact storage evidence, not a query capability.
