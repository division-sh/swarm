package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"

	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestForkReceiverNoticeStoragePreservesCompleteSourceScopedRowsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			source, sibling := seedDeliveryRecoveryClaim(t, f, ctx), seedDeliveryRecoveryClaim(t, f, ctx)
			writer := f.store.(interface {
				InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error)
			})
			for _, item := range []runtimetools.MailboxItem{
				{Type: runtimetools.NotifyHumanMailboxItemType, EventID: source.Snapshot.EventID, Summary: "second", Context: []byte(`{"message":"second","lex":1.00}`)},
				{Type: runtimetools.NotifyHumanMailboxItemType, EventID: source.Snapshot.EventID, Summary: "first", Context: []byte(`{"message":"first"}`), Notified: true},
				{Type: runtimetools.NotifyHumanMailboxItemType, EventID: sibling.Snapshot.EventID, Summary: "sibling", Context: []byte(`{"message":"sibling"}`)},
				{Type: runtimetools.NotifyHumanMailboxItemType, Summary: "runless", Context: []byte(`{"message":"runless"}`)},
			} {
				if _, err := writer.InsertMailboxItem(ctx, item); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for index, scope := range []struct {
				run  string
				want int
			}{{source.Snapshot.RunID, 2}, {sibling.Snapshot.RunID, 1}, {uuid.NewString(), 0}} {
				before := originalForkNoticeRows(t, f.db, scope.run)
				got, err := ReadForkReceiverNoticeStorageForTest(ctx, f.store, scope.run)
				if err != nil || len(got) != scope.want || got == nil || !reflect.DeepEqual(got, before) {
					t.Fatalf("full notice rows/NULL/clock/text/order/source scope changed: got=%v want=%v err=%v", got, before, err)
				}
				counts := probe.Snapshot()
				if counts.Total.Begun != uint64(index+1) || counts.Total.ReadCommits != uint64(index+1) || counts.Total.WriteCommits != 0 || counts.Active != 0 {
					t.Fatalf("notice witness escaped original selected read cut: %+v", counts)
				}
				if after := originalForkNoticeRows(t, f.db, scope.run); !reflect.DeepEqual(before, after) {
					t.Fatal("notice witness mutated storage")
				}
			}
		})
	}
}

func originalForkNoticeRows(t *testing.T, db *sql.DB, runID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = string(raw)
			}
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func TestForkReceiverNoticeStorageRefusesInvalidCancelledClosedAndPartialEvidenceBothStores(t *testing.T) {
	ctx, runID := context.Background(), uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadForkReceiverNoticeStorageForTest(ctx, selected, runID); err == nil || got != nil {
			t.Fatalf("invalid notice owner leaked physical facts: %v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, invalid := range []string{"", "bad", uuid.Nil.String()} {
				if got, err := ReadForkReceiverNoticeStorageForTest(ctx, f.store, invalid); err == nil || got != nil {
					t.Fatal("invalid notice run identity became evidence:", got, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadForkReceiverNoticeStorageForTest(cancelled, f.store, runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatal("cancelled notice read leaked evidence:", got, err)
			}
			requireForkNoticeEncodingRefusal(t, f)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadForkReceiverNoticeStorageForTest(ctx, f.store, runID); err == nil || got != nil {
				t.Fatal("closed notice owner leaked evidence:", got, err)
			}
		})
	}
}

func requireForkNoticeEncodingRefusal(t *testing.T, f authorActivityReceiptFixture) {
	t.Helper()
	ctx := testAuthorActivityContext()
	claimed := seedDeliveryRecoveryClaim(t, f, ctx)
	writer := f.store.(interface {
		InsertMailboxItem(context.Context, runtimetools.MailboxItem) (string, error)
	})
	id, err := writer.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		Type: runtimetools.NotifyHumanMailboxItemType, EventID: claimed.Snapshot.EventID,
		Summary: "encoding witness", Context: []byte(`{"message":"encoding witness"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	before := originalForkNoticeRows(t, f.db, claimed.Snapshot.RunID)
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `ALTER TABLE mailbox ADD COLUMN fork_notice_unencodable DOUBLE PRECISION`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() error {
		if restored {
			return nil
		}
		err := runUnrevisionedEventFixtureTransactionForTest(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE mailbox DROP COLUMN fork_notice_unencodable`)
			return err
		})
		if err == nil {
			restored = true
		}
		return err
	}
	defer func() {
		if err := restore(); err != nil {
			t.Error(err)
		}
	}()
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE mailbox SET fork_notice_unencodable=$1 WHERE item_id=$2`, math.Inf(1), id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadForkReceiverNoticeStorageForTest(ctx, f.store, claimed.Snapshot.RunID); err == nil || got != nil {
		t.Fatal("notice encoding failure returned partial evidence:", got, err)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadForkReceiverNoticeStorageForTest(ctx, f.store, claimed.Snapshot.RunID)
	if err != nil || !reflect.DeepEqual(got, before) {
		t.Fatal("failed notice read changed original history:", got, err)
	}
}
