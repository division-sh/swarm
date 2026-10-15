package runtimepersistence

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

type conversationForkFixtureKind uint8

const (
	conversationForkServedSource conversationForkFixtureKind = iota
	conversationForkNativeChat
	conversationForkDataOnlyChat
)

type conversationForkFixtureStatement struct {
	query string
	args  []any
}

// These are fixed historical witnesses, not caller-authored history or SQL.
func conversationForkProjectionStatements(fixture ConversationForkSourceFixture, postgres bool) ([]conversationForkFixtureStatement, error) {
	kind := fixture.kind
	if kind != conversationForkServedSource && kind != conversationForkNativeChat && kind != conversationForkDataOnlyChat {
		return nil, fmt.Errorf("unknown conversation fixture kind")
	}
	bornAt, advancedAt := fixture.Turn1At.Add(-30*time.Second), fixture.Turn1At.Add(10*time.Second)
	path, fields, gates, bookkeeping, accumulator := "flow/forkchat", `{"name":"After"}`, `{}`, `{}`, `{}`
	if kind != conversationForkServedSource {
		fields = `{"name":"After","current_state":"business-draft","gates":{"review":"authored-review"},"accumulator":{"total":"authored-total"},"bookkeeping":{"activation":"manual"}}`
	}
	if kind == conversationForkNativeChat {
		gates, bookkeeping, accumulator = `{"review":true}`, `{"activation":"standing"}`, `{"total":7}`
	}
	if kind == conversationForkDataOnlyChat {
		path = "flow/forkchat/import"
	}
	statements := []conversationForkFixtureStatement{}
	if kind != conversationForkDataOnlyChat {
		query := `UPDATE flow_instances SET current_state='after', gates=?, bookkeeping=?, accumulator=?, revision=2, entered_state_at=?, updated_at=? WHERE run_id=? AND instance_path='flow/forkchat' AND entity_id=?`
		args := []any{gates, bookkeeping, accumulator, advancedAt, advancedAt, fixture.RunID, fixture.EntityID}
		if postgres {
			query = `UPDATE flow_instances SET current_state='after', gates=$1::jsonb, bookkeeping=$2::jsonb, accumulator=$3::jsonb, revision=2, entered_state_at=$4, updated_at=$5 WHERE run_id=$6::uuid AND instance_path='flow/forkchat' AND entity_id=$7::uuid`
		}
		statements = append(statements, conversationForkFixtureStatement{query, args})
	}
	query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, fields, created_at, updated_at) VALUES (?,?,?,'default',?,?,?)`
	if postgres {
		query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, fields, created_at, updated_at) VALUES ($1::uuid,$2::uuid,$3,'default',$4::jsonb,$5,$6)`
	}
	args := []any{fixture.RunID, fixture.EntityID, path, fields, advancedAt, advancedAt}
	if kind != conversationForkDataOnlyChat {
		query = `UPDATE entity_state SET fields=?, created_at=?, updated_at=? WHERE run_id=? AND entity_id=? AND flow_instance=?`
		args = []any{fields, advancedAt, advancedAt, fixture.RunID, fixture.EntityID, path}
		if postgres {
			query = `UPDATE entity_state SET fields=$1::jsonb, created_at=$2, updated_at=$3 WHERE run_id=$4::uuid AND entity_id=$5::uuid AND flow_instance=$6`
		}
	}
	statements = append(statements, conversationForkFixtureStatement{query, args})
	type mutation struct {
		domain   mutationlog.Domain
		path     string
		old, new any
		at       time.Time
	}
	var mutations []mutation
	if kind == conversationForkDataOnlyChat {
		mutations = append(mutations, mutation{mutationlog.DomainAuthoredField, "name", nil, `"Before"`, bornAt})
	} else {
		entry, err := json.Marshal(fixture.Construction.Child.Instance.Bookkeeping["stage_entry"])
		if err != nil {
			return nil, err
		}
		// Keep the constructor's real entry fact in history, then record its
		// removal at this witness's independently fixed later advance.
		mutations = append(mutations, mutation{mutationlog.DomainBookkeeping, "stage_entry", string(entry), nil, advancedAt})
	}
	if kind != conversationForkServedSource {
		keptAt, removedAt := fixture.Turn1At.Add(-20*time.Second), fixture.Turn1At.Add(-25*time.Second)
		mutations = append(mutations,
			mutation{mutationlog.DomainAuthoredField, "current_state", nil, `"business-draft"`, keptAt},
			mutation{mutationlog.DomainAuthoredField, "gates.review", nil, `"authored-review"`, keptAt},
			mutation{mutationlog.DomainAuthoredField, "accumulator.total", nil, `"authored-total"`, keptAt},
			mutation{mutationlog.DomainAuthoredField, "bookkeeping.activation", nil, `"manual"`, keptAt},
			mutation{mutationlog.DomainAuthoredField, "removed", nil, `"temporary"`, removedAt},
			mutation{mutationlog.DomainAuthoredField, "removed", `"temporary"`, nil, keptAt})
		if kind == conversationForkNativeChat {
			mutations = append(mutations,
				mutation{mutationlog.DomainBookkeeping, "activation", nil, `"standing"`, keptAt},
				mutation{mutationlog.DomainGate, "review", nil, `true`, keptAt},
				mutation{mutationlog.DomainAccumulator, "total", nil, `7`, keptAt},
				mutation{mutationlog.DomainAccumulator, "removed", nil, `9`, removedAt},
				mutation{mutationlog.DomainAccumulator, "removed", `9`, nil, keptAt},
				mutation{mutationlog.DomainBookkeeping, "removed", nil, `"temporary"`, removedAt},
				mutation{mutationlog.DomainBookkeeping, "removed", `"temporary"`, nil, keptAt},
				mutation{mutationlog.DomainGate, "removed", nil, `true`, removedAt},
				mutation{mutationlog.DomainGate, "removed", `true`, nil, keptAt})
		}
	}
	if kind != conversationForkDataOnlyChat {
		mutations = append(mutations, mutation{mutationlog.DomainLifecycleState, "", `"draft"`, `"after"`, advancedAt})
	}
	mutations = append(mutations, mutation{mutationlog.DomainAuthoredField, "name", `"Before"`, `"After"`, advancedAt})
	query = `INSERT INTO entity_mutations (run_id, entity_id, domain, path, old_value, new_value, writer_type, writer_id, created_at) VALUES (?,?,?,?,?,?,'platform','test',?)`
	if postgres {
		query = `INSERT INTO entity_mutations (run_id, entity_id, domain, path, old_value, new_value, writer_type, writer_id, created_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5::jsonb,$6::jsonb,'platform','test',$7)`
	}
	for _, row := range mutations {
		if err := mutationlog.ValidateDomainPath(row.domain, row.path); err != nil {
			return nil, err
		}
		statements = append(statements, conversationForkFixtureStatement{query, []any{fixture.RunID, fixture.EntityID, string(row.domain), row.path, row.old, row.new, row.at}})
	}
	if kind == conversationForkServedSource {
		session, err := conversationForkSessionStatement(fixture, postgres)
		if err != nil {
			return nil, err
		}
		statements = append([]conversationForkFixtureStatement{session}, statements...)
	}
	return statements, nil
}

func conversationForkSessionStatement(fixture ConversationForkSourceFixture, postgres bool) (conversationForkFixtureStatement, error) {
	identity, err := fixture.Identity.StorageFields()
	if err != nil {
		return conversationForkFixtureStatement{}, err
	}
	query := `INSERT INTO agent_sessions (
		session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
		flow_scope_key, flow_instance_id, flow_instance,
		memory_enabled, status, created_at, updated_at
	) VALUES (?,?,?,?,?,?,?,?,?,1,'active',?,?)`
	at := fixture.CreatedAt.Add(-3 * time.Minute)
	args := []any{fixture.SessionID, fixture.RunID, identity.AgentID, identity.NameOwner, identity.NameSource, identity.RoutePresence, identity.FlowScopeKey, identity.FlowInstanceID, identity.FlowInstancePath, at, at}
	if postgres {
		query = `INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, status, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'active',$10,$10)`
		args = args[:10]
	}
	return conversationForkFixtureStatement{query, args}, nil
}
