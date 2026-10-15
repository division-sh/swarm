package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
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
	Construction                                   ConversationForkConstruction
	kind                                           conversationForkFixtureKind
}

type ConversationForkSourceResult struct {
	Events       [2]events.Event
	Construction pipeline.CommittedFlowInstanceActivation
}

// SeedConversationForkSourceForTest uses the selected backend's existing write
// admission. No caller-supplied SQL or transaction callback escapes the facade.
func SeedConversationForkSourceForTest(ctx context.Context, selected any, fixture ConversationForkSourceFixture) (ConversationForkSourceResult, error) {
	var result ConversationForkSourceResult
	bundleHash, err := validateConversationForkFixture(ctx, fixture)
	if err != nil {
		return result, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, fixture.RunID)
	evidence := mutationprotocol.RevisionOnly
	if fixture.kind != conversationForkDataOnlyChat {
		evidence = mutationprotocol.Story
		record, err := fixture.Construction.Child.PersistenceRecord()
		if err != nil {
			return result, fmt.Errorf("conversation fixture requires complete construction: %w", err)
		}
		if record.Identity.RunID != fixture.RunID || record.EntityID != fixture.EntityID ||
			!record.CreatedAt.Equal(fixture.Turn1At.Add(-30*time.Second).UTC().Truncate(time.Microsecond)) || fixture.Construction.Child.Readiness.BundleHash != bundleHash {
			return result, fmt.Errorf("conversation construction does not match fixed historical source/identity/time")
		}
		input := fixture.Construction.CreatingEvent
		if input.ID() == "" || input.ID() != fixture.Construction.Child.CreatingInput.EventID || input.RunID() != fixture.RunID || !input.CreatedAt().Equal(fixture.Construction.Child.OccurredAt) {
			return result, fmt.Errorf("conversation construction requires its original prepared creating event")
		}
	} else if fixture.Construction.Child.Identity.InstancePath != "" || fixture.Construction.Parent != nil {
		return result, fmt.Errorf("data-only conversation fixture cannot carry construction")
	}
	persist := func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect) error {
		var storedHash string
		if err := tx.QueryRowContext(ctx, "SELECT bundle_hash FROM runs WHERE run_id=$1", fixture.RunID).Scan(&storedHash); err != nil {
			return err
		}
		if storedHash != bundleHash {
			return fmt.Errorf("fork fixture source does not match admitted run")
		}
		if fixture.kind != conversationForkDataOnlyChat {
			input := fixture.Construction.CreatingEvent
			producer, err := events.NewProducerIdentity(events.EventProducerExternal, "conversation-construction-fixture")
			if err != nil {
				return err
			}
			if _, err := eventfixture.ExistingRunRoot(ctx, attempt, dialect, input.ID(), fixture.RunID, input.Type(), producer, input.Payload(), events.EventEnvelope{}, input.CreatedAt()); err != nil {
				return err
			}
			switch store := selected.(type) {
			case *PostgresStore:
				result.Construction, err = store.pipelinePostgresOwner.CommitConversationForkConstructionForTest(ctx, attempt, fixture.Construction.Parent, fixture.Construction.Child)
			case *SQLiteRuntimeStore:
				result.Construction, err = store.pipelineSQLiteOwner.CommitConversationForkConstructionForTest(ctx, attempt, fixture.Construction.Parent, fixture.Construction.Child)
			}
			if err != nil {
				return err
			}
		}
		statements, err := conversationForkProjectionStatements(fixture, dialect == authoractivityfixture.DialectPostgres)
		if err != nil {
			return err
		}
		for _, statement := range statements {
			changed, err := tx.ExecContext(ctx, statement.query, statement.args...)
			if err != nil {
				return fmt.Errorf("seed conversation fork source: %w", err)
			}
			if strings.HasPrefix(statement.query, "UPDATE ") {
				if err := requireMailboxFixtureRow(changed); err != nil {
					return err
				}
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
			result.Events[i] = event
		}
		return nil
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("postgres fixture store is required")
		}
		err = mutationprotocol.RunPostgres(ctx, store.backend, evidence, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivityfixture.DialectPostgres)
			})
		}).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		err = mutationprotocol.RunSQLite(ctx, store.backend, "conversation fork source fixture", evidence, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivityfixture.DialectSQLite)
			})
		}).Err()
	default:
		err = fmt.Errorf("fork fixture store %T is unsupported", selected)
	}
	if err != nil {
		return ConversationForkSourceResult{}, err
	}
	if fixture.kind != conversationForkDataOnlyChat {
		result.Construction = result.Construction.WithCommitAcknowledgment()
	}
	return result, nil
}

func seedConversationForkChatEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time, construction ConversationForkConstruction) error {
	_, err := SeedConversationForkSourceForTest(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt, Construction: construction, kind: conversationForkNativeChat})
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
