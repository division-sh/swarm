package store

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type fakeConversationCapabilitySource struct {
	caps StoreSchemaCapabilities
	err  error
}

func (s fakeConversationCapabilitySource) ResolveSchemaCapabilities(context.Context) (StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func TestOperatorConversationReadSurfaceListUsesCanonicalProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	runID := "11111111-1111-1111-1111-111111111111"
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	reader := NewOperatorConversationReadSurface(db, fakeConversationCapabilitySource{caps: StoreSchemaCapabilities{
		Conversations: ConversationSchemaCapabilities{
			Sessions:     SchemaFlavorCanonical,
			Turns:        SchemaFlavorCanonical,
			TurnBlocks:   true,
			SessionRunID: true,
		},
	}})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs("agent-1", runID, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "scope_key", "scope", "runtime_mode", "status", "turn_count", "message_count", "runtime_state", "turn_id", "task_id", "parse_ok", "turn_blocks", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", runID, "live_session", "global", "global", "session", "active", 2, 4, []byte(`{"summary":"brief"}`), "turn-1", "task-1", true, []byte(`[]`), now, nil, now))

	result, err := reader.ListOperatorConversations(context.Background(), OperatorConversationListOptions{
		AgentID: "agent-1",
		RunID:   runID,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("ListOperatorConversations: %v", err)
	}
	if len(result.Conversations) != 1 {
		t.Fatalf("conversation count = %d", len(result.Conversations))
	}
	row := result.Conversations[0]
	if row.SessionID != "sess-1" || row.AgentID != "agent-1" || row.RunID != runID || row.MessageCount != 4 || row.Summary != "brief" {
		t.Fatalf("unexpected conversation row: %+v", row)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
