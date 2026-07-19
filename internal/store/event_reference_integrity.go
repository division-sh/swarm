package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func requireEventOwnedReferences(ctx context.Context, db storerunlifecycle.DBTX, dialect storerunlifecycle.Dialect, record eventrecord.Record) error {
	if sourceID := strings.TrimSpace(record.SourceEventID); sourceID != "" {
		if err := requireSameRunEventReference(ctx, db, dialect, "causal source", sourceID, record.RunID); err != nil {
			return err
		}
	}
	if referenceID := strings.TrimSpace(record.OperatorReferencedEventID); referenceID != "" {
		if err := requireSameRunEventReference(ctx, db, dialect, "operator reference", referenceID, record.RunID); err != nil {
			return err
		}
	}
	return nil
}

func requireSameRunEventReference(ctx context.Context, db storerunlifecycle.DBTX, dialect storerunlifecycle.Dialect, relation, eventID, runID string) error {
	eventID = strings.TrimSpace(eventID)
	runID = strings.TrimSpace(runID)
	if db == nil || eventID == "" || runID == "" {
		return fmt.Errorf("%s requires event_id and run_id", relation)
	}
	var query string
	switch dialect {
	case storerunlifecycle.DialectPostgres:
		query = `SELECT COALESCE(run_id::text, '') FROM events WHERE event_id = $1::uuid FOR KEY SHARE`
	case storerunlifecycle.DialectSQLite:
		query = `SELECT COALESCE(CAST(run_id AS TEXT), '') FROM events WHERE event_id = ?`
	default:
		return fmt.Errorf("%s: unsupported store dialect %q", relation, dialect)
	}
	var actualRunID string
	err := db.QueryRowContext(ctx, query, eventID).Scan(&actualRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s event %s does not exist", relation, eventID)
	}
	if err != nil {
		return fmt.Errorf("load %s event %s: %w", relation, eventID, err)
	}
	if strings.TrimSpace(actualRunID) != runID {
		return fmt.Errorf("%s event %s belongs to run %s, not run %s", relation, eventID, strings.TrimSpace(actualRunID), runID)
	}
	return nil
}
