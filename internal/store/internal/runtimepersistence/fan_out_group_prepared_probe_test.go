package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"

	"modernc.org/sqlite"
)

func TestFanOutGroupPreparedProbeMatchesRawSQLite(t *testing.T) {
	for _, operation := range []string{"exec", "query", "empty-query"} {
		for _, failure := range []string{"none", "before", "after", "native"} {
			if operation == "empty-query" && failure == "native" {
				continue
			}
			for _, mode := range []string{"raw", "prepared", "legacy-prepared"} {
				t.Run(operation+"/"+failure+"/"+mode, func(t *testing.T) {
					ctx := context.Background()
					probe := &groupProofConnector{driver: &sqlite.Driver{}, dsn: ":memory:"}
					db := sql.OpenDB(probe)
					t.Cleanup(func() { probe.set(nil); _ = db.Close() })
					db.SetMaxOpenConns(1)
					if _, err := db.ExecContext(ctx, `CREATE TABLE group_probe (id INTEGER PRIMARY KEY)`); err != nil {
						t.Fatal(err)
					}
					if failure == "native" {
						if _, err := db.ExecContext(ctx, `INSERT INTO group_probe VALUES (1)`); err != nil {
							t.Fatal(err)
						}
					}
					query, before, after := `INSERT INTO group_probe VALUES (?)`, "before_exec", "after_exec"
					if operation != "exec" {
						query, before, after = `INSERT INTO group_probe VALUES (?1), (?1 + 1) RETURNING id`, "before_query", "after_query_row"
					}
					if operation == "empty-query" {
						query = `SELECT id FROM group_probe WHERE id < ?`
					}
					injected := errors.New("group probe execution fault")
					var phases []string
					probe.set(func(phase, observed string) error {
						if phase == "reset_session" {
							return nil
						}
						if observed != query {
							t.Fatalf("hook query=%q, want %q", observed, query)
						}
						phases = append(phases, phase)
						if failure == "before" && phase == before || failure == "after" && phase == after {
							return injected
						}
						return nil
					})
					var stmt *sql.Stmt
					var legacy driver.Stmt
					var conn *sql.Conn
					var err error
					if mode == "prepared" {
						stmt, err = db.PrepareContext(ctx, query)
						if err != nil {
							t.Fatal(err)
						}
						defer stmt.Close()
					} else if mode == "legacy-prepared" {
						conn, err = db.Conn(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer conn.Close()
						err = conn.Raw(func(c any) error {
							legacy, err = c.(driver.Conn).Prepare(query)
							return err
						})
						if err != nil {
							t.Fatal(err)
						}
						defer legacy.Close()
					}
					if len(phases) != 0 {
						t.Fatalf("preparation fired execution hooks: %v", phases)
					}
					for repeat := 0; repeat < 3; repeat++ {
						phases = nil
						value := int64(10 * (repeat + 1))
						if failure == "native" {
							value = 1
						}
						returned := 0
						if legacy != nil {
							err = conn.Raw(func(any) error {
								if operation == "exec" {
									_, err := legacy.Exec([]driver.Value{value})
									return err
								}
								rows, err := legacy.Query([]driver.Value{value})
								if err != nil {
									return err
								}
								defer rows.Close()
								for {
									err := rows.Next(make([]driver.Value, 1))
									if err == io.EOF {
										return nil
									}
									if err != nil {
										return err
									}
									returned++
								}
							})
						} else if operation == "exec" {
							if stmt != nil {
								_, err = stmt.ExecContext(ctx, value)
							} else {
								_, err = db.ExecContext(ctx, query, value)
							}
						} else {
							var rows *sql.Rows
							if stmt != nil {
								rows, err = stmt.QueryContext(ctx, value)
							} else {
								rows, err = db.QueryContext(ctx, query, value)
							}
							if err == nil {
								for rows.Next() {
									returned++
								}
								err = errors.Join(rows.Err(), rows.Close())
							}
						}
						wantPhases := []string{before}
						if failure != "before" && failure != "native" && operation != "empty-query" {
							wantPhases = append(wantPhases, after)
						}
						if !reflect.DeepEqual(phases, wantPhases) {
							t.Fatalf("execution %d hooks=%v, want %v", repeat, phases, wantPhases)
						}
						wantFault := failure == "before" || failure == "after" && operation != "empty-query"
						if errors.Is(err, injected) != wantFault || (err != nil) != (wantFault || failure == "native") {
							t.Fatalf("execution %d error=%v, want injected=%v native=%v", repeat, err, wantFault, failure == "native")
						}
						wantReturned := 0
						if operation == "query" && failure == "none" {
							wantReturned = 2
						}
						if returned != wantReturned {
							t.Fatalf("returned=%d, want %d", returned, wantReturned)
						}
					}
					probe.set(nil)
					if legacy != nil {
						if err := legacy.Close(); err != nil {
							t.Fatal(err)
						}
						if err := conn.Close(); err != nil {
							t.Fatal(err)
						}
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM group_probe`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					wantCount := 0
					if failure == "native" {
						wantCount = 1
					} else if failure != "before" && operation != "empty-query" {
						wantCount = 3
						if operation == "query" {
							wantCount *= 2
						}
					}
					if count != wantCount {
						t.Fatalf("native writes=%d, want %d", count, wantCount)
					}
				})
			}
		}
	}
}
