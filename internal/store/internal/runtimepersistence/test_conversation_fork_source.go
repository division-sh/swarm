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
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
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
	var result [2]events.Event
	identity, err := fixture.Identity.StorageFields()
	if err != nil {
		return result, err
	}
	if identity.RunID != fixture.RunID {
		return result, fmt.Errorf("fork fixture run does not match agent identity")
	}
	if fixture.SessionID == "" || fixture.EntityID == "" || fixture.Event1ID == "" || fixture.Event2ID == "" || fixture.CreatedAt.IsZero() || fixture.Turn1At.IsZero() || !fixture.Turn2At.After(fixture.Turn1At) {
		return result, fmt.Errorf("fork fixture requires exact historical identities and timestamps")
	}
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" {
		return result, fmt.Errorf("fork fixture requires exact source scope")
	}
	persist := func(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect) error {
		query := "SELECT bundle_hash FROM runs WHERE run_id=?"
		if dialect == authoractivityfixture.DialectPostgres {
			query = "SELECT bundle_hash FROM runs WHERE run_id=$1::uuid"
		}
		var bundleHash string
		if err := tx.QueryRowContext(ctx, query, fixture.RunID).Scan(&bundleHash); err != nil {
			return err
		}
		if bundleHash != source.BundleHash() {
			return fmt.Errorf("fork fixture source does not match admitted run")
		}
		now := fixture.CreatedAt
		var statements []struct {
			query string
			args  []any
		}
		switch dialect {
		case authoractivityfixture.DialectPostgres:
			statements = []struct {
				query string
				args  []any
			}{
				{`INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source, status, created_at, updated_at
			) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored','active',$10,$10)`,
					[]any{fixture.SessionID, fixture.RunID, identity.AgentID, identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath, now.Add(-3 * time.Minute)}},
				{`INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid,$2::uuid,'flow/forkchat','default','after','{}'::jsonb,'{"name":"After"}'::jsonb,'{}'::jsonb,2,$3,$3,$3)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second)}},
				{`INSERT INTO entity_mutations (run_id, entity_id, domain, path, old_value, new_value, writer_type, writer_id, created_at) VALUES ($1::uuid,$2::uuid,'lifecycle_state','',NULL,'"draft"'::jsonb,'platform','test',$3),($1::uuid,$2::uuid,'authored_field','name',NULL,'"Before"'::jsonb,'platform','test',$3),($1::uuid,$2::uuid,'lifecycle_state','','"draft"'::jsonb,'"after"'::jsonb,'platform','test',$4)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.Turn1At.Add(10 * time.Second)}},
			}
		case authoractivityfixture.DialectSQLite:
			statements = []struct {
				query string
				args  []any
			}{
				{`INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source, status, created_at, updated_at
			) VALUES (?,?,?,?,?,?,?,?,?,1,'authored','active',?,?)`,
					[]any{fixture.SessionID, fixture.RunID, identity.AgentID, identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath, now.Add(-3 * time.Minute), now.Add(-3 * time.Minute)}},
				{`INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?,?,'flow/forkchat','default','after','{}','{"name":"After"}','{}',2,?,?,?)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second), fixture.Turn1At.Add(10 * time.Second), fixture.Turn1At.Add(10 * time.Second)}},
				{`INSERT INTO entity_mutations (run_id, entity_id, domain, path, old_value, new_value, writer_type, writer_id, created_at) VALUES (?,?,'lifecycle_state','',NULL,'"draft"','platform','test',?),(?,?,'authored_field','name',NULL,'"Before"','platform','test',?),(?,?,'lifecycle_state','','"draft"','"after"','platform','test',?)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second)}},
			}
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return fmt.Errorf("seed conversation fork source: %w", err)
			}
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
			event, err := eventfixture.ExistingRunRoot(ctx, tx, dialect, eventID, fixture.RunID, kind, producer, []byte("{}"), events.EventEnvelope{}, at)
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
		err = store.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return persist(ctx, tx, authoractivityfixture.DialectPostgres)
		})
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		err = store.backend.RunTransaction(ctx, "conversation fork source fixture", func(ctx context.Context, tx *sql.Tx) error {
			return persist(ctx, tx, authoractivityfixture.DialectSQLite)
		})
	default:
		err = fmt.Errorf("fork fixture store %T is unsupported", selected)
	}
	if err != nil {
		return [2]events.Event{}, err
	}
	return result, nil
}
