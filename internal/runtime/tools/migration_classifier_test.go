package tools_test

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	runtimebus "empireai/internal/runtime/bus"
	runtimetools "empireai/internal/runtime/tools"
)

func TestClassifyMigration_AdditiveOnlyIsSafe(t *testing.T) {
	classification := runtimetools.ClassifyMigration(`
		CREATE TABLE foo (id uuid primary key);
		CREATE INDEX idx_foo_id ON foo(id);
		ALTER TABLE foo ADD COLUMN name text;
		INSERT INTO foo (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok');
	`)
	if !classification.Safe || classification.RequiresApproval {
		t.Fatalf("expected additive migration to be safe, got %+v", classification)
	}
	if len(classification.DestructiveOps) != 0 {
		t.Fatalf("expected no destructive ops, got %+v", classification.DestructiveOps)
	}
}

func TestClassifyMigration_DestructiveRequiresApproval(t *testing.T) {
	classification := runtimetools.ClassifyMigration(`
		ALTER TABLE users DROP COLUMN legacy_phone;
		TRUNCATE audit_log;
	`)
	if classification.Safe || !classification.RequiresApproval {
		t.Fatalf("expected destructive migration to require approval, got %+v", classification)
	}
	if len(classification.DestructiveOps) < 2 {
		t.Fatalf("expected destructive ops to be captured, got %+v", classification.DestructiveOps)
	}
}

func TestRuntimeToolExecutor_DeployMigrationGuardrailRejectsDestructiveSQL(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := runtimetools.NewExecutor(bus, nil, nil)
	mb := &mailboxStoreStub{}
	exec.SetMailboxStore(mb)
	actor := models.AgentConfig{
		ID:         "devops-agent-11111111-1111-1111-1111-111111111111",
		Role:       "devops-agent",
		Mode:       "operating",
		VerticalID: "11111111-1111-1111-1111-111111111111",
	}

	ctx := runtimeactor.WithActor(context.Background(), actor)
	ctx = runtimebus.WithInboundEvent(ctx, events.Event{
		ID:          "deploy-req-1",
		Type:        events.EventType("deploy_requested"),
		SourceAgent: "cto-agent-11111111-1111-1111-1111-111111111111",
		VerticalID:  actor.VerticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": actor.VerticalID}),
	})

	_, err := exec.Execute(ctx, "emit_devops_deploy_requested", map[string]any{
		"vertical_id":      actor.VerticalID,
		"requesting_agent": actor.ID,
		"environment":      "production",
		"version":          12,
		"manifest": map[string]any{
			"migration_sql": "DROP TABLE users;",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "migration_requires_approval") {
		t.Fatalf("expected migration guardrail error, got %v", err)
	}
	if strings.TrimSpace(mb.last.Type) != "migration_approval" {
		t.Fatalf("expected deploy migration mailbox item, got %+v", mb.last)
	}
}
