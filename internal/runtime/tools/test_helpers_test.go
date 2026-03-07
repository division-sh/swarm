package tools_test

import (
	"context"
	"strings"
)

type testMailboxStore struct {
	last      MailboxItem
	defaultID string
}

func (m *testMailboxStore) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if strings.TrimSpace(item.ID) == "" {
		if strings.TrimSpace(m.defaultID) != "" {
			return m.defaultID, nil
		}
		return "m-1", nil
	}
	return item.ID, nil
}

func (m *testMailboxStore) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *testMailboxStore) CountMailboxItems(context.Context, string) (int, error) {
	return 0, nil
}
func (m *testMailboxStore) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}
func (m *testMailboxStore) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *testMailboxStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *testMailboxStore) MarkMailboxItemNotified(context.Context, string) error { return nil }
func (m *testMailboxStore) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

type testScheduleStore struct{ upsert int }

func (s *testScheduleStore) UpsertSchedule(context.Context, Schedule) error { s.upsert++; return nil }
func (s *testScheduleStore) CancelSchedule(context.Context, string, string) error {
	return nil
}
func (s *testScheduleStore) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (s *testScheduleStore) MarkScheduleFired(context.Context, Schedule) error { return nil }
