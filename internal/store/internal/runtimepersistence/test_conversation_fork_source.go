package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// ConversationForkSourceFixture is the fixed historical source used by the
// served fork/snapshot proof. It is not a runtime history-import capability.
type ConversationForkSourceFixture struct {
	Identity                                       agentidentity.Identity
	RunID, SessionID, EntityID, Event1ID, Event2ID string
	CreatedAt, Turn1At, Turn2At                    time.Time
}

// SeedConversationForkSourceForTest uses the selected backend's existing write
// admission. No caller-supplied SQL or transaction callback escapes the facade.
func SeedConversationForkSourceForTest(ctx context.Context, selected any, fixture ConversationForkSourceFixture) ([2]events.Event, error) {
	identity, err := fixture.Identity.StorageFields()
	if err != nil {
		return [2]events.Event{}, err
	}
	if identity.RunID != fixture.RunID {
		return [2]events.Event{}, fmt.Errorf("fork fixture run does not match agent identity")
	}
	if fixture.SessionID == "" || fixture.EntityID == "" || fixture.Event1ID == "" || fixture.Event2ID == "" || fixture.CreatedAt.IsZero() || fixture.Turn1At.IsZero() || !fixture.Turn2At.After(fixture.Turn1At) {
		return [2]events.Event{}, fmt.Errorf("fork fixture requires exact historical identities and timestamps")
	}
	return seedConversationForkFixture(ctx, selected, fixture, conversationForkServedSource)
}

func seedConversationForkChatEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time) error {
	_, err := seedConversationForkFixture(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt}, conversationForkNativeChat)
	return err
}

func seedConversationForkDataOnlyEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time) error {
	_, err := seedConversationForkFixture(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt}, conversationForkDataOnlyChat)
	return err
}

func seedConversationForkFixture(ctx context.Context, selected any, fixture ConversationForkSourceFixture, kind conversationForkFixtureKind) ([2]events.Event, error) {
	var result [2]events.Event
	for _, id := range []string{fixture.RunID, fixture.EntityID} {
		if _, err := uuid.Parse(id); err != nil {
			return result, fmt.Errorf("conversation fixture requires exact run/entity UUIDs: %w", err)
		}
	}
	if fixture.Turn1At.IsZero() {
		return result, fmt.Errorf("conversation fixture requires an exact historical cut")
	}
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" {
		return result, fmt.Errorf("fork fixture requires exact source scope")
	}
	persist := func(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect authoractivity.Dialect) error {
		query := "SELECT bundle_hash FROM runs WHERE run_id=?"
		if dialect == authoractivity.DialectPostgres {
			query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
		}
		var bundleHash string
		if err := tx.QueryRowContext(ctx, query, fixture.RunID).Scan(&bundleHash); err != nil {
			return err
		}
		if bundleHash != source.BundleHash() {
			return fmt.Errorf("fork fixture source does not match admitted run")
		}
		statements, err := conversationForkProjectionStatements(fixture, kind, dialect)
		if err != nil {
			return err
		}
		if kind == conversationForkServedSource {
			identity, err := fixture.Identity.StorageFields()
			if err != nil {
				return err
			}
			query := `INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, status, created_at, updated_at
			) VALUES (?,?,?,?,?,?,?,?,?,1,'active',?,?)`
			at := fixture.CreatedAt.Add(-3 * time.Minute)
			args := []any{fixture.SessionID, fixture.RunID, identity.AgentID, identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath, at, at}
			if dialect == authoractivity.DialectPostgres {
				query = `INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, status, created_at, updated_at
			) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'active',$10,$10)`
				args = args[:10]
			}
			statements = append([]conversationForkFixtureStatement{{query, args}}, statements...)
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return fmt.Errorf("seed conversation fork source: %w", err)
			}
		}
		if kind != conversationForkServedSource {
			return nil
		}

		for i, eventID := range []string{fixture.Event1ID, fixture.Event2ID} {
			kind, at := events.EventType("task.ready"), fixture.Turn1At
			if i == 1 {
				kind, at = "task.done", fixture.Turn2At
			}
			producer, err := events.NewProducerIdentity(events.EventProducerExternal, "served-fork-source")
			if err != nil {
				return err
			}
			event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{
				RunID: fixture.RunID,
				Facts: events.EventFacts{ID: eventID, Type: kind,
					Producer: events.ProducerClaim{Type: producer.Type(), ID: producer.ID()},
					Payload:  []byte("{}"), Envelope: events.EventEnvelope{}, RoutingSource: events.NoRoutingSource(),
					CreatedAt: at, ExecutionMode: executionmode.Live},
			})
			if err != nil {
				return err
			}
			// This fixed historical witness is explicitly schema-less, as before;
			// production event admission and persistence still own its evidence.
			binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
				BundleHash: "bundle-v2:sha256:0000000000000000000000000000000000000000000000000000000000000000",
				EventKey:   string(kind), SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
				SchemaClass: events.PayloadSchemaSchemaLess,
			})
			if err != nil {
				return err
			}
			payload, err := events.NewPayloadAdmission(event.Payload(), binding)
			if err != nil {
				return err
			}
			bound, err := events.ApplyPayloadAdmission(event, payload)
			if err != nil {
				return err
			}
			admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				return err
			}
			ledger, err := events.NewConnectEvaluationLedger(nil)
			if err != nil {
				return err
			}
			settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
			if err != nil {
				return err
			}
			record, err := eventrecord.FromAdmitted(admitted, settlement)
			if err != nil {
				return err
			}
			if dialect == authoractivity.DialectPostgres {
				_, err = insertCanonicalFixtureRecordForTest(ctx, attempt, "postgres", record)
			} else {
				_, err = insertCanonicalFixtureRecordForTest(ctx, attempt, "sqlite", record)
			}
			if err != nil {
				return err
			}
			result[i] = event
		}
		return nil
	}
	var err error
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("postgres fixture store is required")
		}
		err = mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivity.DialectPostgres)
			})
		}).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		err = mutationprotocol.RunSQLite(ctx, store.backend, "conversation fork source fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return persist(txctx, tx, attempt, authoractivity.DialectSQLite)
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
