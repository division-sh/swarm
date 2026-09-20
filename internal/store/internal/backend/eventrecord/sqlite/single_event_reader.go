package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

// SingleEventReader reuses only the canonical single-event SELECT. Admission,
// lineage reads and settlement decoding still run for every observation.
type SingleEventReader struct {
	statement sqlitebackend.FixedReadStatement
}

func (r *SingleEventReader) LoadAdmitted(ctx context.Context, b *sqlitebackend.Backend, eventID string) (events.AdmittedEvent, events.RouteSettlement, bool, error) {
	stmt, err := r.statement.PrepareContext(ctx, b, selectSingleRecord)
	if err != nil {
		return events.AdmittedEvent{}, events.RouteSettlement{}, false, fmt.Errorf("load sqlite event record: %w", err)
	}
	return LoadAdmitted(ctx, singleEventQueryer{Backend: b, stmt: stmt}, eventID)
}

type singleEventQueryer struct {
	*sqlitebackend.Backend
	stmt *sql.Stmt
}

func (q singleEventQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if query == selectSingleRecord {
		return q.stmt.QueryRowContext(ctx, args...)
	}
	return q.Backend.QueryRowContext(ctx, query, args...)
}
