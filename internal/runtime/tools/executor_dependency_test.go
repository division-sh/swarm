package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type allowMailboxAuthority struct{}

func (allowMailboxAuthority) CanonicalRole(role string) string { return strings.TrimSpace(role) }
func (allowMailboxAuthority) ProducerRoles() []string          { return nil }
func (allowMailboxAuthority) ProducerEventsForRole(string) []string {
	return nil
}
func (allowMailboxAuthority) HasMessageAuthority(actor, target models.AgentConfig) bool { return false }
func (allowMailboxAuthority) AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	return nil
}
func (allowMailboxAuthority) AuthorizeManagement(actor, target models.AgentConfig) error { return nil }
func (allowMailboxAuthority) AuthorizeMailboxSend(actor models.AgentConfig) error        { return nil }
func (allowMailboxAuthority) CanDecideHumanTasks(role string) bool                       { return true }

type mailboxStoreStub struct {
	last MailboxItem
	id   string
}

func (s *mailboxStoreStub) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	s.last = item
	if strings.TrimSpace(s.id) == "" {
		return "mailbox-1", nil
	}
	return s.id, nil
}
func (*mailboxStoreStub) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}
func (*mailboxStoreStub) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }
func (*mailboxStoreStub) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}
func (*mailboxStoreStub) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}
func (*mailboxStoreStub) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (*mailboxStoreStub) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (*mailboxStoreStub) MarkMailboxItemNotified(context.Context, string) error { return nil }

type entityPersistenceStub struct{}

func (*entityPersistenceStub) LoadEntityState(context.Context, EntityIdentity) (map[string]any, bool, error) {
	return nil, false, nil
}
func (*entityPersistenceStub) QueryEntityStates(context.Context, EntityStateQuery) ([]map[string]any, error) {
	return nil, nil
}
func (*entityPersistenceStub) SaveEntityField(context.Context, EntityFieldUpdate) (int, error) {
	return 0, nil
}
func (*entityPersistenceStub) CreateEntity(context.Context, EntityCreateRecord) error { return nil }

type humanTaskPersistenceStub struct{}

func (*humanTaskPersistenceStub) CreateHumanTask(context.Context, HumanTaskCreateRecord) (string, error) {
	return "task-1", nil
}
func (*humanTaskPersistenceStub) HumanTaskRequeueCount(context.Context, string) (int, error) {
	return 0, nil
}
func (*humanTaskPersistenceStub) CountApprovedHumanTasksSince(context.Context, time.Time) (int, error) {
	return 0, nil
}
func (*humanTaskPersistenceStub) DecideHumanTask(context.Context, HumanTaskDecisionRecord) error {
	return nil
}

func TestExecutorMailboxSendFailsWithoutMailboxStore(t *testing.T) {
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{AuthorityProvider: allowMailboxAuthority{}})

	_, err := exec.execMailboxSend(context.Background(), models.AgentConfig{ID: "agent-1", EntityID: "entity-1"}, map[string]any{
		"type": "approval",
	})
	if err == nil || !strings.Contains(err.Error(), "mailbox store is not configured") {
		t.Fatalf("execMailboxSend err = %v, want mailbox store error", err)
	}
}

func TestExecutorMailboxSendUsesConstructorOwnedMailboxStore(t *testing.T) {
	store := &mailboxStoreStub{id: "mailbox-42"}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		MailboxStore:      store,
		AuthorityProvider: allowMailboxAuthority{},
	})

	out, err := exec.execMailboxSend(context.Background(), models.AgentConfig{ID: "agent-1", EntityID: "entity-1"}, map[string]any{
		"type":    "approval",
		"summary": "Need review",
	})
	if err != nil {
		t.Fatalf("execMailboxSend: %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok || strings.TrimSpace(asString(result["mailbox_id"])) != "mailbox-42" {
		t.Fatalf("execMailboxSend output = %#v, want mailbox_id mailbox-42", out)
	}
	if got := strings.TrimSpace(store.last.Type); got != "approval" {
		t.Fatalf("stored mailbox type = %q, want approval", got)
	}
	if got := strings.TrimSpace(store.last.EntityID); got != "entity-1" {
		t.Fatalf("stored mailbox entity_id = %q, want entity-1", got)
	}
}

func TestExecutorEntityStoreDependencyFailsWithoutConstructorOwnedStore(t *testing.T) {
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})

	store, err := exec.entityStoreDependency()
	if store != nil || err == nil || !strings.Contains(err.Error(), "entity persistence store is not configured") {
		t.Fatalf("entityStoreDependency = (%v, %v), want nil store error", store, err)
	}
}

func TestExecutorEntityStoreDependencyUsesConstructorOwnedStore(t *testing.T) {
	store := &entityPersistenceStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{EntityStore: store})

	got, err := exec.entityStoreDependency()
	if err != nil {
		t.Fatalf("entityStoreDependency: %v", err)
	}
	if got != store {
		t.Fatalf("entityStoreDependency store = %p, want %p", got, store)
	}
}

func TestExecutorHumanTaskStoreDependencyFailsWithoutConstructorOwnedStore(t *testing.T) {
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})

	store, err := exec.humanTaskStoreDependency()
	if store != nil || err == nil || !strings.Contains(err.Error(), "human task persistence store is not configured") {
		t.Fatalf("humanTaskStoreDependency = (%v, %v), want nil store error", store, err)
	}
}

func TestExecutorHumanTaskStoreDependencyUsesConstructorOwnedStore(t *testing.T) {
	store := &humanTaskPersistenceStub{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{HumanTaskStore: store})

	got, err := exec.humanTaskStoreDependency()
	if err != nil {
		t.Fatalf("humanTaskStoreDependency: %v", err)
	}
	if got != store {
		t.Fatalf("humanTaskStoreDependency store = %p, want %p", got, store)
	}
}
