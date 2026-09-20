package pipelinepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func foldStatementKey() fanoutobligation.IntentKey {
	return fanoutobligation.IntentKey{RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString(),
		ElementRef: runtimecontracts.FanOutElementRef{FlowPath: "root", Family: "handler_rule", SemanticPath: `handlers["items.ready"].rules[0]`}}
}

func foldStatementArgs(key fanoutobligation.IntentKey) []driver.Value {
	return []driver.Value{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath}
}

func foldStatementRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"cardinality", "cursor", "status", "present", "ordinal", "outcome_kind", "event_id", "source_event_id", "inherited_disposition"})
}

func TestFanOutFoldStatementLazyQueriesAndClose(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	q := &fanOutFoldStatement{Tx: tx}
	if err := q.close(); err != nil || q.joined != nil {
		t.Fatalf("unused scope prepared a statement: %v", err)
	}
	invalid := fanoutobligation.IntentKey{}
	_, want := foldFanOutIntentTerminalDispositions(ctx, tx, false, invalid)
	_, got := foldFanOutIntentTerminalDispositions(ctx, q, false, invalid)
	if fmt.Sprint(want) != fmt.Sprint(got) || reflect.TypeOf(want) != reflect.TypeOf(got) || q.joined != nil {
		t.Fatalf("invalid key must fail before preparation: raw=%v prepared=%v", want, got)
	}
	// Neither other QueryContext reads nor QueryRowContext reads are prepared.
	mock.ExpectQuery("SELECT 7").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(7)).RowsWillBeClosed()
	rows, err := q.QueryContext(ctx, "SELECT 7")
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT 8").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(8)).RowsWillBeClosed()
	var n int
	if err := q.QueryRowContext(ctx, "SELECT 8").Scan(&n); err != nil || n != 8 {
		t.Fatalf("delegated read=%d: %v", n, err)
	}
	key := foldStatementKey()
	for invocation := range 2 {
		if invocation > 0 {
			q = &fanOutFoldStatement{Tx: tx}
		}
		stmt := mock.ExpectPrepare(fanOutFoldJoinedQuery).WillBeClosed()
		for read := range 2 {
			stmt.ExpectQuery().WithArgs(foldStatementArgs(key)...).
				WillReturnRows(foldStatementRows().AddRow(read, 0, "canceled", false, nil, nil, nil, nil, nil)).RowsWillBeClosed()
			fold, err := foldFanOutIntentTerminalDispositions(ctx, q, false, key)
			if err != nil || fold.Summary.Total != read || fold.Summary.Canceled != read {
				t.Fatalf("invocation=%d read=%d fold=%+v: %v", invocation, read, fold, err)
			}
			// Even after preparation, a bad key must not execute SQL.
			if _, err := foldFanOutIntentTerminalDispositions(ctx, q, false, invalid); fmt.Sprint(err) != fmt.Sprint(want) {
				t.Fatalf("reused scope invalid key: %v", err)
			}
		}
		if err := q.close(); err != nil {
			t.Fatal(err)
		}
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFanOutFoldStatementFailureCleanup(t *testing.T) {
	for _, stage := range []string{"prepare", "query", "rows", "scan", "canceled", "close"} {
		t.Run(stage, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			q := &fanOutFoldStatement{Tx: tx}
			key := foldStatementKey()
			fault := errors.New("fold statement injected fault")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "canceled" {
				cancel()
				fault = context.Canceled
			} else {
				stmt := mock.ExpectPrepare(fanOutFoldJoinedQuery)
				if stage == "prepare" {
					stmt.WillReturnError(fault)
				} else {
					stmt.WillBeClosed()
					query := stmt.ExpectQuery().WithArgs(foldStatementArgs(key)...)
					switch stage {
					case "query":
						query.WillReturnError(fault)
					case "rows":
						query.WillReturnRows(foldStatementRows().AddRow(0, 0, "closed", false, nil, nil, nil, nil, nil).RowError(0, fault)).RowsWillBeClosed()
					case "scan":
						query.WillReturnRows(foldStatementRows().AddRow(nil, 0, "closed", false, nil, nil, nil, nil, nil)).RowsWillBeClosed()
					case "close":
						stmt.WillReturnCloseError(fault)
						query.WillReturnRows(foldStatementRows().AddRow(0, 0, "closed", false, nil, nil, nil, nil, nil)).RowsWillBeClosed()
					}
				}
			}
			_, got := foldFanOutIntentTerminalDispositions(ctx, q, false, key)
			if stage == "close" && got != nil || stage != "close" && (got == nil || stage != "scan" && !errors.Is(got, fault)) {
				t.Fatalf("error=%v, want %v", got, fault)
			}
			closeErr := q.close()
			if stage == "close" && closeErr != fault || stage != "close" && closeErr != nil {
				t.Fatalf("close error=%v, stage=%s", closeErr, stage)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func sqliteFoldStatementFixture(t testing.TB, count int) (*sql.DB, []fanoutobligation.IntentKey) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fold-statements.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	coords := "run_id TEXT, triggering_delivery_id TEXT, flow_path TEXT, declaration_family TEXT, semantic_path TEXT"
	for _, ddl := range []string{
		"CREATE TABLE fan_out_intents (" + coords + ", cardinality BIGINT, cursor BIGINT, status TEXT, PRIMARY KEY (run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path))",
		"CREATE TABLE fan_out_outcomes (" + coords + ", ordinal BIGINT, outcome_kind TEXT, event_id TEXT, source_event_id TEXT, inherited_disposition TEXT, PRIMARY KEY (run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path, ordinal))",
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	keys := make([]fanoutobligation.IntentKey, count)
	for i := range keys {
		keys[i] = foldStatementKey()
		key := keys[i]
		if _, err := db.Exec(`INSERT INTO fan_out_intents VALUES ($1,$2,$3,$4,$5,4,0,'open')`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath); err != nil {
			t.Fatal(err)
		}
	}
	return db, keys
}

func TestFanOutFoldStatementSQLiteFreshFacts(t *testing.T) {
	db, keys := sqliteFoldStatementFixture(t, 1)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	q := &fanOutFoldStatement{Tx: tx}
	defer q.close()
	before, err := foldFanOutIntentTerminalDispositions(ctx, q, false, keys[0])
	if err != nil || before.EnumerationClosed {
		t.Fatalf("before=%+v: %v", before, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, rawCanceled := foldFanOutIntentTerminalDispositions(canceled, tx, false, keys[0])
	_, preparedCanceled := foldFanOutIntentTerminalDispositions(canceled, q, false, keys[0])
	if !errors.Is(preparedCanceled, context.Canceled) || fmt.Sprint(rawCanceled) != fmt.Sprint(preparedCanceled) || reflect.TypeOf(rawCanceled) != reflect.TypeOf(preparedCanceled) {
		t.Fatalf("canceled reused statement: raw=%v prepared=%v", rawCanceled, preparedCanceled)
	}
	for _, mutation := range []string{
		`UPDATE fan_out_intents SET status='canceled', cursor=1`,
		`INSERT INTO fan_out_outcomes SELECT run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,0,'semantic_rejected',NULL,NULL,NULL FROM fan_out_intents`,
		`UPDATE fan_out_outcomes SET ordinal=2`,
		`UPDATE fan_out_outcomes SET ordinal=0`,
		`UPDATE fan_out_intents SET status='bogus', cardinality=-1`,
		`DELETE FROM fan_out_intents`,
	} {
		if _, err := tx.ExecContext(ctx, mutation); err != nil {
			t.Fatal(err)
		}
		raw, rawErr := foldFanOutIntentTerminalDispositions(ctx, tx, false, keys[0])
		prepared, preparedErr := foldFanOutIntentTerminalDispositions(ctx, q, false, keys[0])
		if !reflect.DeepEqual(raw, prepared) || fmt.Sprint(rawErr) != fmt.Sprint(preparedErr) || reflect.TypeOf(rawErr) != reflect.TypeOf(preparedErr) {
			t.Fatalf("after %s: raw=%+v %v prepared=%+v %v", mutation, raw, rawErr, prepared, preparedErr)
		}
	}
	if before.EnumerationClosed || before.Summary.Total != 4 {
		t.Fatalf("prior result changed: %+v", before)
	}
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := foldFanOutIntentTerminalDispositions(ctx, q, false, keys[0]); err == nil {
		t.Fatal("closed invocation unexpectedly reused its statement")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()
	q2 := &fanOutFoldStatement{Tx: tx2}
	defer q2.close()
	after, err := foldFanOutIntentTerminalDispositions(ctx, q2, false, keys[0])
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("new transaction retained prior facts: before=%+v after=%+v: %v", before, after, err)
	}
}

func BenchmarkFanOutFoldSQLiteStatements64(b *testing.B) {
	db, keys := sqliteFoldStatementFixture(b, 64)
	for _, prepared := range []bool{false, true} {
		b.Run(fmt.Sprintf("prepared_%t", prepared), func(b *testing.B) {
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var reader pipelineQueryer = tx
				var q *fanOutFoldStatement
				if prepared {
					q = &fanOutFoldStatement{Tx: tx}
					reader = q
				}
				for _, key := range keys {
					if _, err := foldFanOutIntentTerminalDispositions(ctx, reader, false, key); err != nil {
						if q != nil {
							_ = q.close()
						}
						b.Fatal(err)
					}
				}
				if q != nil {
					if err := q.close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
