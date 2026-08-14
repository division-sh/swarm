package activityresult

import (
	"context"
	"database/sql"
	"fmt"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
)

type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func LoadPostgres(ctx context.Context, db Queryer, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	return load(ctx, db, request, `
		SELECT event_id::text, event_name
		FROM events
		WHERE event_id IN ($1::uuid, $2::uuid)
		ORDER BY event_id
	`)
}

func LoadSQLite(ctx context.Context, db Queryer, request runtimeactivityresult.Query) (runtimeactivityresult.Record, bool, error) {
	return load(ctx, db, request, `
		SELECT event_id, event_name
		FROM events
		WHERE event_id IN (?, ?)
		ORDER BY event_id
	`)
}

func load(ctx context.Context, db Queryer, request runtimeactivityresult.Query, query string) (runtimeactivityresult.Record, bool, error) {
	if db == nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("activity result reader is required")
	}
	rows, err := db.QueryContext(ctx, query, request.SuccessEventID, request.FailureEventID)
	if err != nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("lookup recorded activity result %s: %w", request.ActivityID, err)
	}
	defer rows.Close()
	found := make([]runtimeactivityresult.Record, 0, 2)
	for rows.Next() {
		var record runtimeactivityresult.Record
		if err := rows.Scan(&record.EventID, &record.EventType); err != nil {
			return runtimeactivityresult.Record{}, false, fmt.Errorf("scan recorded activity result %s: %w", request.ActivityID, err)
		}
		found = append(found, record)
	}
	if err := rows.Err(); err != nil {
		return runtimeactivityresult.Record{}, false, fmt.Errorf("iterate recorded activity result %s: %w", request.ActivityID, err)
	}
	switch len(found) {
	case 0:
		return runtimeactivityresult.Record{}, false, nil
	case 1:
		return found[0], true, nil
	default:
		return runtimeactivityresult.Record{}, false, fmt.Errorf("activity request %s has both success and failure results recorded", request.RequestEventID)
	}
}
