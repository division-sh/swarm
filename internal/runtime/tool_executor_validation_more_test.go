package runtime

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/models"
)

func TestToolExecutor_AgentManagement_AndMailbox_ErrorBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	ex := NewRuntimeToolExecutor(bus, scheduler, nil)
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}

	// No manager configured.
	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "x", "role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.execAgentFire(actor, map[string]any{"agent_id": "x"}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.execAgentReconfigure(actor, map[string]any{"agent_id": "x", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}

	// With manager: cover validation branches.
	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "config.id is required") {
		t.Fatalf("expected config.id required, got %v", err)
	}
	// Unauthorized manager action: vp-growth can't manage backend-agent.
	if _, err := ex.execAgentHire(models.AgentConfig{ID: "g", Role: "vp-growth", Mode: "operating", VerticalID: "v1"}, map[string]any{
		"config": map[string]any{"id": "x", "role": "backend-agent"},
	}); err == nil {
		t.Fatal("expected authorization error")
	}

	// Spawn an agent, then try to hire the same ID again to force manager error.
	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err != nil {
		t.Fatalf("expected initial hire ok: %v", err)
	}
	if _, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err == nil {
		t.Fatal("expected duplicate hire error")
	}

	if _, err := ex.execAgentFire(actor, map[string]any{}); err == nil || !strings.Contains(err.Error(), "agent_id is required") {
		t.Fatalf("expected agent_id required, got %v", err)
	}
	if _, err := ex.execAgentFire(actor, map[string]any{"agent_id": "nope"}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := ex.execAgentReconfigure(actor, map[string]any{"agent_id": "nope", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}

	// Mailbox send error branches.
	if _, err := ex.execMailboxSend(actor, map[string]any{"type": "review"}); err == nil || !strings.Contains(err.Error(), "mailbox store is not configured") {
		t.Fatalf("expected mailbox store error, got %v", err)
	}
	ex.SetMailboxStore(&mailboxStub{})
	if _, err := ex.execMailboxSend(actor, map[string]any{"type": "review", "timeout_at": "nope"}); err == nil || !strings.Contains(err.Error(), "invalid timeout_at") {
		t.Fatalf("expected timeout_at error, got %v", err)
	}
}
