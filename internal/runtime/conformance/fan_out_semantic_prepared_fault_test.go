package conformance

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// These are isolated driver tables, not admitted event-store fixtures. The
// cardinality proof separately exercises canonical publication and rollback.
func TestSemanticProofPreparedFaultMatchesRawBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			connector := &semanticProofConnector{}
			if backend == "postgres" {
				connector.dsn, _, _ = testutil.StartEmptyPostgres(t)
				connector.driver = &pq.Driver{}
			} else {
				connector.dsn = "file:" + filepath.Join(t.TempDir(), "prepared-fault.sqlite")
				connector.driver = &sqlite.Driver{}
			}
			db := sql.OpenDB(connector)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = db.Close() })
			for _, table := range []string{"events", "fan_out_outcomes"} {
				if _, err := db.Exec("CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
				for _, queryRows := range []bool{false, true} {
					operation := "exec"
					if queryRows {
						operation = "returning"
					}
					for _, prepared := range []bool{false, true} {
						mode := "raw"
						if prepared {
							mode = "prepared"
						}
						t.Run(table+"/"+operation+"/"+mode, func(t *testing.T) {
							ctx := context.Background()
							tx, err := db.BeginTx(ctx, nil)
							if err != nil {
								t.Fatal(err)
							}
							defer tx.Rollback()
							query := "INSERT INTO " + table + " (id) VALUES ($1), ($1+1)"
							if queryRows {
								query += " RETURNING id"
							}
							newFault := func() *semanticProofSQLFault {
								return &semanticProofSQLFault{cause: errors.New("native insert completed"), outcomeInsert: table == "fan_out_outcomes"}
							}
							withFault := func(f *semanticProofSQLFault) context.Context {
								return context.WithValue(ctx, semanticProofSQLFaultKey{}, f)
							}
							preparationFault := newFault()
							var stmt *sql.Stmt
							if prepared {
								stmt, err = tx.PrepareContext(withFault(preparationFault), query)
								if err != nil {
									t.Fatal(err)
								}
								defer stmt.Close()
							}
							execute := func(ctx context.Context, id int) error {
								if !queryRows {
									if stmt != nil {
										_, err := stmt.ExecContext(ctx, id)
										return err
									}
									_, err := tx.ExecContext(ctx, query, id)
									return err
								}
								var rows *sql.Rows
								var err error
								if stmt != nil {
									rows, err = stmt.QueryContext(ctx, id)
								} else {
									rows, err = tx.QueryContext(ctx, query, id)
								}
								if err != nil {
									return err
								}
								returned := 0
								for rows.Next() {
									var got int
									if err := rows.Scan(&got); err != nil {
										t.Fatal(err)
									}
									returned++
								}
								err = errors.Join(rows.Err(), rows.Close())
								if err == nil && returned != 2 || err != nil && returned != 0 {
									t.Fatalf("returned=%d err=%v", returned, err)
								}
								return err
							}
							if err := execute(ctx, 1); err != nil || preparationFault.fired.Load() {
								t.Fatalf("preparation context leaked into execution: %v", err)
							}
							first := newFault()
							if err := execute(withFault(first), 10); !errors.Is(err, first.cause) || !first.fired.Load() {
								t.Fatalf("missing native-execution fault: %v", err)
							}
							if err := execute(withFault(first), 20); err != nil {
								t.Fatalf("same fault fired twice: %v", err)
							}
							second := newFault()
							if err := execute(withFault(second), 30); !errors.Is(err, second.cause) || !second.fired.Load() {
								t.Fatalf("new execution context lost its fault: %v", err)
							}
							foreign := newFault()
							foreign.outcomeInsert = !foreign.outcomeInsert
							if err := execute(withFault(foreign), 40); err != nil || foreign.fired.Load() {
								t.Fatalf("wrong INSERT family consumed fault: %v", err)
							}
							canceledFault := newFault()
							canceled, cancel := context.WithCancel(withFault(canceledFault))
							cancel()
							if err := execute(canceled, 50); !errors.Is(err, context.Canceled) || canceledFault.fired.Load() {
								t.Fatalf("canceled execution consumed fault: %v", err)
							}
							var count int
							if err := tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 10 {
								t.Fatalf("native writes before rollback=%d want10: %v", count, err)
							}
							// Native failure must win without consuming a post-success fault.
							nativeFault := newFault()
							if err := execute(withFault(nativeFault), 1); err == nil || errors.Is(err, nativeFault.cause) || nativeFault.fired.Load() {
								t.Fatalf("native duplicate error replaced by injection: %v", err)
							}
							if stmt != nil {
								if err := stmt.Close(); err != nil {
									t.Fatal(err)
								}
							}
							if err := tx.Rollback(); err != nil {
								t.Fatal(err)
							}
							if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
								t.Fatalf("native rollback leaked %d rows: %v", count, err)
							}
						})
					}
				}
			}
		})
	}
}
