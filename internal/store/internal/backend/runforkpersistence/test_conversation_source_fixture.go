package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type ConversationForkFixtureKind uint8

const (
	ConversationForkServedSource ConversationForkFixtureKind = iota
	ConversationForkNativeChat
	ConversationForkDataOnlyChat
)
const (
	conversationForkServedSource = ConversationForkServedSource
	conversationForkNativeChat   = ConversationForkNativeChat
	conversationForkDataOnlyChat = ConversationForkDataOnlyChat
)

type ConversationForkConstruction struct {
	Parent        *pipeline.FlowInstanceActivationPlan
	Child         pipeline.FlowInstanceActivationPlan
	CreatingEvent events.Event
}

type ConversationForkSourceFixture struct {
	Identity                                       agentidentity.Identity
	RunID, SessionID, EntityID, Event1ID, Event2ID string
	CreatedAt, Turn1At, Turn2At                    time.Time
	Construction                                   ConversationForkConstruction
	Kind                                           ConversationForkFixtureKind
}

type ConversationForkSourceResult struct {
	Events       [2]events.Event
	Construction pipeline.CommittedFlowInstanceActivation
}

func (s *RunForkPostgresOwner) SeedConversationForkSourceForTest(ctx context.Context, fixture ConversationForkSourceFixture, bundleHash string, evidence mutationprotocol.Evidence) (ConversationForkSourceResult, error) {
	return seedConversationForkSourceForTest(ctx, s, fixture, bundleHash, evidence)
}

func (s *RunForkSQLiteOwner) SeedConversationForkSourceForTest(ctx context.Context, fixture ConversationForkSourceFixture, bundleHash string, evidence mutationprotocol.Evidence) (ConversationForkSourceResult, error) {
	return seedConversationForkSourceForTest(ctx, s, fixture, bundleHash, evidence)
}

func seedConversationForkSourceForTest(ctx context.Context, selected any, fixture ConversationForkSourceFixture, bundleHash string, evidence mutationprotocol.Evidence) (ConversationForkSourceResult, error) {
	var result ConversationForkSourceResult
	var err error
	persist := func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect) error {
		var storedHash string
		if err := tx.QueryRowContext(ctx, "SELECT bundle_hash FROM runs WHERE run_id=$1", fixture.RunID).Scan(&storedHash); err != nil {
			return err
		}
		if storedHash != bundleHash {
			return fmt.Errorf("fork fixture source does not match admitted run")
		}
		if fixture.Kind != conversationForkDataOnlyChat {
			input := fixture.Construction.CreatingEvent
			producer, err := events.NewProducerIdentity(events.EventProducerExternal, "conversation-construction-fixture")
			if err != nil {
				return err
			}
			if _, err := eventpersistence.InsertExistingRunRootEventTxForTest(ctx, attempt, dialect == authoractivityfixture.DialectPostgres, input.ID(), fixture.RunID, input.Type(), producer, input.Payload(), events.EventEnvelope{}, input.CreatedAt()); err != nil {
				return err
			}
			switch store := selected.(type) {
			case *RunForkPostgresOwner:
				prepared, found, loadErr := store.events.(*eventpersistence.EventPostgresOwner).LoadPreparedPublishEventTx(ctx, tx, input.ID())
				if loadErr != nil || !found {
					return errors.Join(loadErr, fmt.Errorf("fixed construction requires its persisted creating event"))
				}
				result.Construction, err = store.PipelinePostgresOwner.CommitFixedForkConstructionForTest(ctx, attempt, fixture.Construction.Parent, fixture.Construction.Child, prepared.Event)
			case *RunForkSQLiteOwner:
				prepared, found, loadErr := store.events.(*eventpersistence.EventSQLiteOwner).LoadPreparedPublishEventTx(ctx, tx, input.ID())
				if loadErr != nil || !found {
					return errors.Join(loadErr, fmt.Errorf("fixed construction requires its persisted creating event"))
				}
				result.Construction, err = store.PipelineSQLiteOwner.CommitFixedForkConstructionForTest(ctx, attempt, fixture.Construction.Parent, fixture.Construction.Child, prepared.Event)
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
				count, err := changed.RowsAffected()
				if err != nil {
					return err
				}
				if count != 1 {
					return fmt.Errorf("mailbox fixture requires exactly one row, changed %d", count)
				}
			}
		}
		if fixture.Kind != conversationForkServedSource {
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
			event, err := eventpersistence.InsertExistingRunRootEventTxForTest(ctx, attempt, dialect == authoractivityfixture.DialectPostgres, input.id, fixture.RunID, input.kind, producer, []byte("{}"), events.EventEnvelope{}, input.at)
			if err != nil {
				return err
			}
			result.Events[i] = event
		}
		return nil
	}
	switch store := selected.(type) {
	case *RunForkPostgresOwner:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("postgres fixture store is required")
		}
		err = mutationprotocol.RunPostgres(ctx, store.backend, evidence, mutationprotocol.Ordinary, nil, store.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivityfixture.DialectPostgres)
			})
		}).Err()
	case *RunForkSQLiteOwner:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		err = mutationprotocol.RunSQLite(ctx, store.backend, "conversation fork source fixture", evidence, mutationprotocol.Ordinary, nil, store.candidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
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
	if fixture.Kind != conversationForkDataOnlyChat {
		result.Construction = result.Construction.WithCommitAcknowledgment()
	}
	return result, nil
}
