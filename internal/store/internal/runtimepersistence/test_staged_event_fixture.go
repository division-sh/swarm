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
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func SeedPostForkSourceEventForTest(ctx context.Context, selected any, runID, eventID, entityID string, at time.Time) (events.Event, error) {
	var empty, event events.Event
	for _, id := range []string{runID, eventID, entityID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return empty, err
		}
	}
	if at.IsZero() {
		return empty, fmt.Errorf("post-fork source fixture requires its original timestamp")
	}
	producer, err := events.NewProducerIdentity(events.EventProducerExternal, "source-runtime")
	if err != nil {
		return empty, err
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return empty, err
	}
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		event, err = eventfixture.ExistingRunRoot(ctx, attempt, dialect, eventID, runID, "source.after",
			producer, []byte(`{}`),
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1"), at)
		return err
	})
	if err != nil {
		return empty, err
	}
	return event, nil
}

func SeedRunDiagnosticDeadLetterHistoryForTest(ctx context.Context, selected any, runID, eventID string, payload []byte, at time.Time) (events.Event, error) {
	var empty, event events.Event
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return empty, err
		}
	}
	if at.IsZero() {
		return empty, fmt.Errorf("diagnostic history fixture requires its original timestamp")
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return empty, err
	}
	err = runEventFixtureMutationForTest(ctx, selected, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		event, err = eventfixture.DiagnosticDirectForRun(ctx, attempt, dialect, eventID, runID, "", "runtime", payload, at)
		if err != nil {
			return err
		}
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return pipelinepersistence.InsertDiagnosticDeadLetterHistoryFixtureTx(ctx, tx, eventID, at)
		})
	})
	if err != nil {
		return empty, err
	}
	return event, nil
}

// InsertStagedChildEventForTest owns the native fixture commit without creating
// a historical cut; the caller captures its complete precondition afterward.
func InsertStagedChildEventForTest(ctx context.Context, selected any, eventID, runID, parentEventID string,
	eventType events.EventType, producer events.ProducerIdentity, payload []byte,
	envelope events.EventEnvelope, at time.Time) (events.Event, error) {
	var empty events.Event
	event, record, err := eventfixture.StagedChild(eventID, runID, parentEventID, eventType, producer, payload, envelope, at)
	if err != nil {
		return empty, err
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
		return empty, fmt.Errorf("staged child event selected store %T is unsupported", selected)
	}
	if err := result.Err(); err != nil {
		return empty, err
	}
	committed, ok := result.Value()
	if !ok {
		return empty, fmt.Errorf("staged child event commit was not acknowledged")
	}
	return committed, nil
}
