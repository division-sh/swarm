package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/staged"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// InsertStagedChildEventForTest owns the native fixture commit without creating
// a historical cut; the caller captures its complete precondition afterward.
func InsertStagedChildEventForTest(ctx context.Context, selected any, eventID, runID, parentEventID string,
	eventType events.EventType, producer events.ProducerIdentity, payload []byte,
	envelope events.EventEnvelope, at time.Time) (events.Event, error) {
	event, record, err := eventfixture.StagedChild(eventID, runID, parentEventID, eventType, producer, payload, envelope, at)
	if err != nil {
		return events.Event{}, err
	}
	write := func(dialect authoractivity.Dialect) func(context.Context, *mutationprotocol.Attempt) (events.Event, error) {
		return func(txctx context.Context, attempt *mutationprotocol.Attempt) (events.Event, error) {
			err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				_, err := staged.Insert(txctx, tx, dialect, record)
				return err
			})
			return event, err
		}
	}
	var result mutationprotocol.Result[events.Event]
	switch store := selected.(type) {
	case *PostgresStore:
		result = mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, write(authoractivity.DialectPostgres))
	case *SQLiteRuntimeStore:
		result = mutationprotocol.RunSQLite(ctx, store.backend, "stage child event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, write(authoractivity.DialectSQLite))
	default:
		return events.Event{}, fmt.Errorf("staged child event selected store %T is unsupported", selected)
	}
	if err := result.Err(); err != nil {
		return events.Event{}, err
	}
	committed, ok := result.Value()
	if !ok {
		return events.Event{}, fmt.Errorf("staged child event commit was not acknowledged")
	}
	return committed, nil
}
