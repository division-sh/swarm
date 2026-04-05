package tools

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
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

func TestExecutorSQLDBDependencyFailsWithoutConstructorOwnedDB(t *testing.T) {
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})

	db, err := exec.sqlDBDependency()
	if db != nil || err == nil || !strings.Contains(err.Error(), "sql db is not configured") {
		t.Fatalf("sqlDBDependency = (%v, %v), want nil sql db error", db, err)
	}
}

func TestExecutorSQLDBDependencyUsesConstructorOwnedDB(t *testing.T) {
	db := &sql.DB{}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{SQLDB: db})

	got, err := exec.sqlDBDependency()
	if err != nil {
		t.Fatalf("sqlDBDependency: %v", err)
	}
	if got != db {
		t.Fatalf("sqlDBDependency db = %p, want %p", got, db)
	}
}
