package releasee2e

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Failure-only observation uses a new read-only connection, not another RPC
// through the possibly blocked child. Every part survives later capture errors.
func collectLifecycleRPCFailure(process *releaseServeProcess, selected goldenStoreSelection, params map[string]any, emit func(string, any, error)) {
	liveErr := process.cmd.Process.Signal(syscall.Signal(0))
	select {
	case <-process.exited:
		emit("process", "exited", process.waitError())
	default:
		emit("process", "not_joined", liveErr)
	}
	emit("child_output_before_signal", process.output.String(), nil)
	process.collectStartupEvidence()
	emit("child_output_after_signal", process.output.String(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openLifecycleFailureInspection(selected)
	if err != nil {
		emit("store_open", nil, err)
		return
	}
	defer func() { emit("store_close", nil, db.Close()) }()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		emit("store_snapshot", nil, err)
		return
	}
	defer func() { emit("store_snapshot_rollback", nil, tx.Rollback()) }()
	cardID, _ := params["card_id"].(string)
	queries := []struct{ part, query string }{
		{"decision", `SELECT CAST(c.card_id AS TEXT), CAST(c.run_id AS TEXT), c.status, COALESCE(c.verdict, ''), COALESCE(CAST(c.decision_event_id AS TEXT), ''), r.status FROM decision_cards c JOIN runs r ON r.run_id=c.run_id WHERE c.card_id=$1`},
		{"deliveries", `SELECT status, COUNT(*) FROM event_deliveries WHERE run_id=(SELECT run_id FROM decision_cards WHERE card_id=$1) GROUP BY status ORDER BY status`},
		{"pipeline", `SELECT COUNT(*), COALESCE(SUM(CASE WHEN p.outcome IS NULL THEN 1 ELSE 0 END), 0) FROM events e LEFT JOIN event_receipts p ON p.event_id=e.event_id AND p.subscriber_type='platform' AND p.subscriber_id='pipeline' WHERE e.run_id=(SELECT run_id FROM decision_cards WHERE card_id=$1)`},
	}
	for _, query := range queries {
		partCtx, partCancel := context.WithTimeout(ctx, time.Second)
		value, err := readLifecycleFailureRows(partCtx, tx, query.query, cardID)
		partCancel()
		emit(query.part, value, err)
	}
}

func openLifecycleFailureInspection(selected goldenStoreSelection) (*sql.DB, error) {
	switch selected.name {
	case "postgres":
		if selected.inspectionConnector == nil {
			return nil, fmt.Errorf("exact selected PostgreSQL inspection connector is absent")
		}
		return sql.OpenDB(selected.inspectionConnector), nil
	case "sqlite":
		if selected.inspectionSQLitePath == "" {
			return nil, fmt.Errorf("exact selected SQLite inspection path is absent")
		}
		uri := url.URL{Scheme: "file", Path: selected.inspectionSQLitePath}
		uri.RawQuery = url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(0)"}}.Encode()
		return sql.Open("sqlite", uri.String())
	default:
		return nil, fmt.Errorf("unknown selected inspection backend %q", selected.name)
	}
}

func readLifecycleFailureRows(ctx context.Context, tx *sql.Tx, query, cardID string) (value [][]string, err error) {
	rows, err := tx.QueryContext(ctx, query, cardID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		row := make([]string, len(columns))
		dest := make([]any, len(columns))
		for i := range row {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return value, err
		}
		value = append(value, row)
	}
	return value, rows.Err()
}

func TestLifecycleFailureInspectionReadOnlyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := goldenSQLiteStore(t.TempDir())
			if backend == "postgres" {
				dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
				if dsn == "" {
					t.Fatalf("%s is required for selected-store failure inspection", goldenPostgresEnv)
				}
				selected = goldenPostgresStore(t, dsn)
			} else {
				writer, err := sql.Open("sqlite", selected.inspectionSQLitePath)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := writer.Exec(`CREATE TABLE evidence_marker (id TEXT)`)
				closeErr := writer.Close()
				if writeErr != nil || closeErr != nil {
					t.Fatalf("offline evidence fixture: write=%v close=%v", writeErr, closeErr)
				}
			}
			db, err := openLifecycleFailureInspection(selected)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := readLifecycleFailureRows(ctx, tx, `SELECT CAST($1 AS TEXT)`, "exact-card")
			if err != nil || len(rows) != 1 || rows[0][0] != "exact-card" {
				t.Fatalf("exact read: %v, %v", rows, err)
			}
			if _, err := tx.ExecContext(ctx, `CREATE TABLE forbidden_evidence_write (id TEXT)`); err == nil {
				t.Fatal("failure observer acquired write permission")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLifecycleFailureInspectionDoesNotCreateMissingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	db, err := openLifecycleFailureInspection(goldenStoreSelection{name: "sqlite", inspectionSQLitePath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err == nil {
		t.Fatal("read-only inspection opened a missing store")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("inspection created state: %v", err)
	}
	for _, selected := range []goldenStoreSelection{{name: "sqlite"}, {name: "postgres"}, {name: "unknown"}} {
		if _, err := openLifecycleFailureInspection(selected); err == nil {
			t.Fatalf("unbound inspection accepted: %#v", selected)
		}
	}
}
