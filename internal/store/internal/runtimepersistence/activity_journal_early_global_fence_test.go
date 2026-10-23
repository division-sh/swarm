package runtimepersistence

import (
	"database/sql"

	"testing"
)

func activityJournalSequenceHead(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var head int64
	if err := db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&head); err != nil {
		t.Fatal(err)
	}
	return head
}
