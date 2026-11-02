package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

func requireEventOwnedReferences(ctx context.Context, tx *sql.Tx, postgres bool, record eventrecord.Record) error {
	if sourceID := strings.TrimSpace(record.SourceEventID); sourceID != "" {
		if err := requireSameRunEventReference(ctx, tx, postgres, "causal source", sourceID, record.RunID); err != nil {
			return err
		}
	}
	if referenceID := strings.TrimSpace(record.OperatorReferencedEventID); referenceID != "" {
		if err := requireSameRunEventReference(ctx, tx, postgres, "operator reference", referenceID, record.RunID); err != nil {
			return err
		}
	}
	return nil
}

func requireSameRunEventReference(ctx context.Context, tx *sql.Tx, postgres bool, relation, eventID, runID string) error {
	eventID = strings.TrimSpace(eventID)
	runID = strings.TrimSpace(runID)
	if tx == nil || eventID == "" || runID == "" {
		return fmt.Errorf("%s requires event_id and run_id", relation)
	}
	query := `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	if postgres {
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid FOR KEY SHARE`
	}
	var actualRunID string
	err := tx.QueryRowContext(ctx, query, eventID).Scan(&actualRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s event %s does not exist", relation, eventID)
	}
	if err != nil {
		return fmt.Errorf("load %s event %s: %w", relation, eventID, err)
	}
	if strings.TrimSpace(actualRunID) != runID {
		if relation == "causal source" && strings.TrimSpace(actualRunID) == "" {
			owned, err := runlessPlatformDeliveryOwnsRun(ctx, tx, postgres, eventID, runID)
			if err != nil {
				return err
			}
			if owned {
				return nil
			}
		}
		return fmt.Errorf("%s event %s belongs to run %s, not run %s", relation, eventID, strings.TrimSpace(actualRunID), runID)
	}
	return nil
}

func runlessPlatformDeliveryOwnsRun(ctx context.Context, tx *sql.Tx, postgres bool, eventID, runID string) (bool, error) {
	var (
		admitted events.AdmittedEvent
		found    bool
		err      error
	)
	if postgres {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	}
	if err != nil || !found || !admitted.Event().IsRunlessPlatformPublication() {
		return false, err
	}
	query := `SELECT 1 FROM event_deliveries WHERE event_id = ? AND run_id = ? LIMIT 1`
	if postgres {
		query = `SELECT 1 FROM event_deliveries WHERE event_id = $1::uuid AND run_id = $2::uuid LIMIT 1`
	}
	var present int
	if err := tx.QueryRowContext(ctx, query, eventID, runID).Scan(&present); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("load platform source delivery %s in run %s: %w", eventID, runID, err)
	}
	return present == 1, nil
}
