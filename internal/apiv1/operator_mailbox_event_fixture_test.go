package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

// These persisted-event readers are shared by supported proposed-effect tests.
// The handler/rule mailbox_write producers and materializer-failure roots were
// retired under #2307 Gate A; these helpers confer no authored action authority.
func loadMailboxWritePersistedEvent(t *testing.T, db *sql.DB, backend, eventID string) events.Event {
	t.Helper()
	sqlText := ""
	if strings.HasPrefix(backend, "sqlite") {
		sqlText = `
			SELECT event_id, COALESCE(run_id, ''), event_name, COALESCE(produced_by, ''),
			       COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(scope, 'global'),
			       payload, created_at, COALESCE(source_event_id, ''),
			       COALESCE(routing_source_kind, 'absent'), COALESCE(routing_source_authority, ''),
			       COALESCE(source_route, '{}'), COALESCE(target_route, '{}'), COALESCE(target_set, '[]')
			FROM events
			WHERE event_id = ?
		`
	} else {
		sqlText = `
			SELECT event_id::text, COALESCE(run_id::text, ''), event_name, COALESCE(produced_by, ''),
			       COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(scope, 'global'),
			       payload, created_at, COALESCE(source_event_id::text, ''),
			       COALESCE(routing_source_kind, 'absent'), COALESCE(routing_source_authority, ''),
			       COALESCE(source_route, '{}'::jsonb), COALESCE(target_route, '{}'::jsonb), COALESCE(target_set, '[]'::jsonb)
			FROM events
			WHERE event_id = $1::uuid
		`
	}
	var id, runID, eventName, producedBy, entityID, flowInstance, scope, sourceEventID string
	var routingSourceKind, routingSourceAuthority string
	var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
	if err := db.QueryRowContext(context.Background(), sqlText, eventID).Scan(
		&id,
		&runID,
		&eventName,
		&producedBy,
		&entityID,
		&flowInstance,
		&scope,
		&payloadRaw,
		&createdAtRaw,
		&sourceEventID,
		&routingSourceKind,
		&routingSourceAuthority,
		&sourceRouteRaw,
		&targetRouteRaw,
		&targetSetRaw,
	); err != nil {
		t.Fatalf("%s load event %s: %v", backend, eventID, err)
	}
	envelope := mailboxWriteDBEnvelope(t, entityID, flowInstance, scope, sourceRouteRaw, targetRouteRaw, targetSetRaw)
	routingSource, err := events.RestoreRoutingSource(routingSourceKind, envelope.Source, routingSourceAuthority)
	if err != nil {
		t.Fatalf("%s restore event %s routing source: %v", backend, eventID, err)
	}
	return eventtest.PersistedProjectionWithRoutingSource(
		id,
		events.EventType(eventName),
		producedBy,
		"",
		mailboxWriteDBJSON(payloadRaw, "{}"),
		0,
		runID,
		sourceEventID,
		envelope,
		routingSource,
		mailboxWriteDBTime(createdAtRaw))

}

func mailboxWriteDBEnvelope(t *testing.T, entityID, flowInstance, scope string, sourceRouteRaw, targetRouteRaw, targetSetRaw any) events.EventEnvelope {
	t.Helper()
	envelope := events.EventEnvelope{
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		Scope:        events.EventScope(strings.TrimSpace(scope)),
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(sourceRouteRaw, "{}"), &envelope.Source); err != nil {
		t.Fatalf("decode source_route: %v", err)
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(targetRouteRaw, "{}"), &envelope.Target); err != nil {
		t.Fatalf("decode target_route: %v", err)
	}
	if err := json.Unmarshal(mailboxWriteDBJSON(targetSetRaw, "[]"), &envelope.TargetSet); err != nil {
		t.Fatalf("decode target_set: %v", err)
	}
	return envelope.Normalized()
}

func mailboxWriteDBJSON(raw any, fallback string) json.RawMessage {
	switch v := raw.(type) {
	case nil:
		return json.RawMessage(fallback)
	case json.RawMessage:
		if len(v) == 0 {
			return json.RawMessage(fallback)
		}
		return v
	case []byte:
		if len(v) == 0 {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(v)
	case string:
		if strings.TrimSpace(v) == "" {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil || len(encoded) == 0 {
			return json.RawMessage(fallback)
		}
		return json.RawMessage(encoded)
	}
}

func mailboxWriteDBTime(raw any) time.Time {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC()
	case []byte:
		return mailboxWriteParseDBTime(string(v))
	case string:
		return mailboxWriteParseDBTime(v)
	default:
		return time.Now().UTC()
	}
}

func mailboxWriteParseDBTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Now().UTC()
}

func sourceArtifactFactForTestBundle(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("test bundle has no admitted source artifact")
	}
	return mustAPITestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
}
