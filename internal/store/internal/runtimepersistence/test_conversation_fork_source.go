package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

// ConversationForkSourceFixture is the fixed historical source used by the
// served fork/snapshot proof. It is not a runtime history-import capability.
type ConversationForkSourceFixture struct {
	Identity                                       agentidentity.Identity
	RunID, SessionID, EntityID, Event1ID, Event2ID string
	CreatedAt, Turn1At, Turn2At                    time.Time
	kind                                           conversationForkFixtureKind
}

// SeedConversationForkSourceForTest uses the selected backend's existing write
// admission. No caller-supplied SQL or transaction callback escapes the facade.
func SeedConversationForkSourceForTest(ctx context.Context, selected any, fixture ConversationForkSourceFixture) ([2]events.Event, error) {
	var result [2]events.Event
	bundleHash, err := validateConversationForkFixture(ctx, fixture)
	if err != nil {
		return result, err
	}
	persist := func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect) error {
		var storedHash string
		if err := tx.QueryRowContext(ctx, "SELECT bundle_hash FROM runs WHERE run_id=$1", fixture.RunID).Scan(&storedHash); err != nil {
			return err
		}
		if storedHash != bundleHash {
			return fmt.Errorf("fork fixture source does not match admitted run")
		}
		statements, err := conversationForkProjectionStatements(fixture, dialect == authoractivityfixture.DialectPostgres)
		if err != nil {
			return err
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return fmt.Errorf("seed conversation fork source: %w", err)
			}
		}
		if fixture.kind != conversationForkServedSource {
			return nil
		}
		producer, err := events.NewProducerIdentity(events.EventProducerExternal, "served-fork-source")
		if err != nil {
			return err
		}
		for i, input := range []struct {
			id   string
			kind events.EventType
			at   time.Time
		}{{fixture.Event1ID, "task.ready", fixture.Turn1At}, {fixture.Event2ID, "task.done", fixture.Turn2At}} {
			event, err := eventfixture.ExistingRunRoot(ctx, attempt, dialect, input.id, fixture.RunID, input.kind, producer, []byte("{}"), events.EventEnvelope{}, input.at)
			if err != nil {
				return err
			}
			result[i] = event
		}
		return nil
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("postgres fixture store is required")
		}
		err = mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivityfixture.DialectPostgres)
			})
		}).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		err = mutationprotocol.RunSQLite(ctx, store.backend, "conversation fork source fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivityfixture.DialectSQLite)
			})
		}).Err()
	default:
		err = fmt.Errorf("fork fixture store %T is unsupported", selected)
	}
	if err != nil {
		return [2]events.Event{}, err
	}
	return result, nil
}

func seedConversationForkChatEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time) error {
	_, err := SeedConversationForkSourceForTest(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt, kind: conversationForkNativeChat})
	return err
}

func seedConversationForkDataOnlyEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time) error {
	_, err := SeedConversationForkSourceForTest(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt, kind: conversationForkDataOnlyChat})
	return err
}

func validateConversationForkFixture(ctx context.Context, fixture ConversationForkSourceFixture) (string, error) {
	if fixture.kind == conversationForkServedSource {
		identity, err := fixture.Identity.StorageFields()
		if err != nil {
			return "", err
		}
		if identity.RunID != fixture.RunID {
			return "", fmt.Errorf("fork fixture run does not match agent identity")
		}
		if fixture.SessionID == "" || fixture.EntityID == "" || fixture.Event1ID == "" || fixture.Event2ID == "" || fixture.CreatedAt.IsZero() || fixture.Turn1At.IsZero() || !fixture.Turn2At.After(fixture.Turn1At) {
			return "", fmt.Errorf("fork fixture requires exact historical identities and timestamps")
		}
	}
	for _, id := range []string{fixture.RunID, fixture.EntityID} {
		if _, err := uuid.Parse(id); err != nil {
			return "", fmt.Errorf("conversation fixture requires exact run/entity UUIDs: %w", err)
		}
	}
	if fixture.Turn1At.IsZero() {
		return "", fmt.Errorf("conversation fixture requires an exact historical cut")
	}
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" {
		return "", fmt.Errorf("fork fixture requires exact source scope")
	}
	return source.BundleHash(), nil
}
