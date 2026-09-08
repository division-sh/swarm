package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

type eventReadQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadPostgresEventIdentity(ctx context.Context, q eventReadQueryer, eventID string) (eventrecord.Record, bool, error) {
	return eventrecordpostgres.Load(ctx, q, eventID)
}

func loadSQLiteEventIdentity(ctx context.Context, q eventReadQueryer, eventID string) (eventrecord.Record, bool, error) {
	return eventrecordsqlite.Load(ctx, q, eventID)
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return raw
}

func decodeEventRecord(record eventrecord.Record) (events.AdmittedEvent, error) {
	return record.Decode()
}

func decodeStoredFailure(raw any) (*runtimefailures.Envelope, error) {
	var encoded []byte
	switch value := raw.(type) {
	case nil:
		return nil, nil
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		var err error
		encoded, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode stored canonical failure: %w", err)
		}
	}
	if text := strings.TrimSpace(string(encoded)); text == "" || text == "null" {
		return nil, nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode stored canonical failure: %w", err)
	}
	return &failure, nil
}

func sqliteToolJSONPath(path string) string {
	segments := strings.Split(strings.TrimSpace(path), ".")
	out := "$"
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment != "" {
			out += "." + segment
		}
	}
	return out
}

func agentIdentityFields(identity runtimeagentidentity.Identity) (runtimeagentidentity.StorageFields, error) {
	fields, err := identity.Normalize().StorageFields()
	if err != nil {
		return runtimeagentidentity.StorageFields{}, fmt.Errorf("agent identity storage fields: %w", err)
	}
	return fields, nil
}

func agentIdentityFromColumns(runID, agentID, nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID, flowInstance string) (runtimeagentidentity.Identity, error) {
	return runtimeagentidentity.FromStorageFields(runtimeagentidentity.StorageFields{
		RunID:   runID,
		AgentID: agentID, NameOwner: nameOwner, NameSource: nameSource,
		RoutePresence: routePresence, FlowScopeKey: flowScopeKey,
		FlowInstanceID: flowInstanceID, FlowInstancePath: flowInstance,
	})
}

func sqliteJSONRawMessage(raw any) json.RawMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), value...)
	case []byte:
		return json.RawMessage(append([]byte(nil), value...))
	case string:
		return json.RawMessage(value)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return encoded
	}
}

func decodeStoreJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), true, nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid sqlite time %q", raw)
}
