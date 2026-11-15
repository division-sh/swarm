package channeldelivery

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestChannelDeliveryResponsibilityProjection(t *testing.T) {
	// These are projection fixtures, not evidence of provider acceptance. The
	// native effect matrix proves the journal transitions producing these rows.
	for _, test := range []struct {
		name, source, status, phase, effect string
		want                                int
	}{
		{"pending", "card", "pending", "planned", "", 1},
		{"deferred", "card", "pending", "rendered", "", 1},
		{"superseded", "card", "superseded", "rendered", "", 0},
		{"decided", "card", "decided", "rendered", "", 0},
		{"expired", "card", "expired", "rendered", "", 0},
		{"notice_completed", "notice", "decided", "rendered", "", 0},
		{"authorized", "card", "superseded", "retired", "authorized", 1},
		{"launched", "card", "superseded", "retired", "launched", 1},
		{"observed", "card", "superseded", "retired", "response_observed", 1},
		{"uncertain", "card", "superseded", "uncertain", "outcome_uncertain", 0},
		{"terminal_failure", "card", "superseded", "rendered", "terminal_failure", 0},
		{"summary", "summary", "", "planned", "", 1},
		{"response", "response", "", "rendered", "", 1},
	} {
		t.Run("projection/"+test.name, func(t *testing.T) {
			db := newChannelResponsibilityProjectionDB(t)
			statements := []struct {
				query string
				args  []any
			}{
				{`INSERT INTO channel_delivery_plans VALUES ('delivery',?, 'source',NULL,?)`, []any{test.source, test.phase}},
				{`INSERT INTO decision_cards VALUES ('source',?)`, []any{test.status}},
				{`INSERT INTO mailbox VALUES ('source',?)`, []any{test.status}},
				{`INSERT INTO runtime_external_effect_operations VALUES ('channel_delivery','{"delivery_id":"delivery"}',?)`, []any{test.effect}},
			}
			for _, statement := range statements {
				if _, err := db.Exec(statement.query, statement.args...); err != nil {
					t.Fatal(err)
				}
			}
			backend, err := sqlitebackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			count, err := CountUnsettledChannelResponsibilitiesForTest(context.Background(), backend, false)
			if err != nil || count != test.want {
				t.Fatalf("responsibility count=%d want=%d: %v", count, test.want, err)
			}
		})
	}
}

func newChannelResponsibilityProjectionDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "wait.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, ddl := range []string{
		`CREATE TABLE channel_delivery_plans (delivery_id TEXT, source_kind TEXT, source_id TEXT, current_receipt_operation_id TEXT, state TEXT NOT NULL)`,
		`CREATE TABLE channel_delivery_receipts (delivery_id TEXT, effect_operation_id TEXT, state TEXT)`,
		`CREATE TABLE runtime_external_effect_operations (authority_kind TEXT, authority_evidence TEXT, state TEXT)`,
		`CREATE TABLE mailbox (item_id TEXT, status TEXT)`,
		`CREATE TABLE decision_cards (card_id TEXT, status TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
