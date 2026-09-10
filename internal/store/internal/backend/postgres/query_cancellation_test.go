package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// The server notice is the barrier: cancellation occurs during actual query
// execution, not before acquisition or after an already completed callback.
func TestPostgresNativeQueryCancellation(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, surface := range []string{"exec", "query_row", "rows", "prepared_exec", "prepared_rows"} {
			t.Run(fmt.Sprintf("read=%t/%s", read, surface), func(t *testing.T) {
				_, db, _ := testutil.StartPostgres(t)
				db.SetMaxOpenConns(1)
				if _, err := db.Exec(`CREATE FUNCTION cancellation_probe() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'query-active'; PERFORM pg_sleep(30); RETURN 1; END $$`); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				noticed := false
				if err := conn.Raw(func(raw any) error {
					pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { noticed = true; cancel() })
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				backend, err := New(db)
				if err != nil {
					t.Fatal(err)
				}
				run := backend.RunTransaction
				if read {
					run = backend.RunReadTransaction
				}
				err = run(ctx, func(ctx context.Context, tx *sql.Tx) error {
					const query = "SELECT cancellation_probe()"
					switch surface {
					case "exec":
						_, err := tx.ExecContext(ctx, query)
						return err
					case "query_row":
						var value int
						return tx.QueryRowContext(ctx, query).Scan(&value)
					case "rows":
						rows, err := tx.QueryContext(ctx, query)
						if err != nil {
							return err
						}
						for rows.Next() {
						}
						return errors.Join(rows.Err(), rows.Close())
					default:
						stmt, err := tx.PrepareContext(ctx, query)
						if err != nil {
							return err
						}
						defer stmt.Close()
						if surface == "prepared_exec" {
							_, err := stmt.ExecContext(ctx)
							return err
						}
						rows, err := stmt.QueryContext(ctx)
						if err != nil {
							return err
						}
						for rows.Next() {
						}
						return errors.Join(rows.Err(), rows.Close())
					}
				})
				if !noticed {
					t.Fatal("actual server execution barrier was not reached")
				}
				if !onlyCancellationLeaves(err, context.Canceled) {
					t.Errorf("native owning cancellation lost provenance: %T %v", err, err)
				}
				progress, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				if err := db.PingContext(progress); err != nil {
					t.Fatalf("pool1 did not progress: %v", err)
				}
			})
		}
	}
}

func onlyCancellationLeaves(err, cause error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if !onlyCancellationLeaves(child, cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyCancellationLeaves(wrapped.Unwrap(), cause)
	}
	return err == cause
}

func TestPostgresNativeCancellationPreservesIndependentFailures(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, scenario := range []string{"statement_timeout", "server_cancel", "server_error_then_cancel", "callback_57014", "callback_bad_connection"} {
			t.Run(fmt.Sprintf("read=%t/%s", read, scenario), func(t *testing.T) {
				_, db, _ := testutil.StartPostgres(t)
				b, err := New(db)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var independent error
				run := b.RunTransaction
				if read {
					run = b.RunReadTransaction
				}
				err = run(ctx, func(ctx context.Context, tx *sql.Tx) error {
					switch scenario {
					case "callback_57014":
						independent = &pq.Error{Code: "57014", Message: "independent callback"}
						cancel()
					case "callback_bad_connection":
						independent = driver.ErrBadConn
						cancel()
					case "server_cancel":
						// A real independent CancelRequest is initiated by another
						// session after this exact backend enters its native query.
						var pid int
						if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
							return err
						}
						adminDone := make(chan error, 1)
						go func() {
							for {
								var active bool
								if err := db.QueryRowContext(ctx, "SELECT state='active' AND query='SELECT pg_sleep(30)' FROM pg_stat_activity WHERE pid=$1", pid).Scan(&active); err != nil {
									adminDone <- err
									return
								}
								if active {
									break
								}
							}
							_, err := db.ExecContext(ctx, "SELECT pg_cancel_backend($1)", pid)
							adminDone <- err
						}()
						_, independent = tx.ExecContext(ctx, "SELECT pg_sleep(30)")
						if err := <-adminDone; err != nil {
							return errors.Join(independent, err)
						}
					case "server_error_then_cancel":
						_, independent = tx.ExecContext(ctx, "SELECT 1/0")
						cancel()
					default:
						if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout='20ms'"); err != nil {
							return err
						}
						_, independent = tx.ExecContext(ctx, "SELECT pg_sleep(30)")
					}
					return independent
				})
				if independent == nil || !errors.Is(err, independent) {
					t.Fatalf("independent native/callback failure lost: independent=%v result=%v", independent, err)
				}
				if onlyCancellationLeaves(err, context.Canceled) {
					t.Fatalf("independent failure became owning cancellation: %v", err)
				}
				if scenario == "server_cancel" || scenario == "statement_timeout" {
					var server *pq.Error
					if !errors.As(err, &server) || server.Code != "57014" {
						t.Fatalf("server cancellation lost: %v", err)
					}
				}
			})
		}
	}
}

func TestPostgresNativeStreamingCancellation(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			db.SetMaxOpenConns(1)
			b, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = b.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
				const query = "SELECT i, pg_sleep(0.001) FROM generate_series(1,10000) i"
				var rows *sql.Rows
				var err error
				if prepared {
					stmt, prepErr := tx.PrepareContext(ctx, query)
					if prepErr != nil {
						return prepErr
					}
					defer stmt.Close()
					rows, err = stmt.QueryContext(ctx)
				} else {
					rows, err = tx.QueryContext(ctx, query)
				}
				if err != nil {
					return err
				}
				if !rows.Next() {
					return errors.Join(errors.New("no row before streaming cancellation"), rows.Err(), rows.Close())
				}
				cancel()
				for rows.Next() {
				}
				return errors.Join(rows.Err(), rows.Close())
			})
			if !onlyCancellationLeaves(err, context.Canceled) {
				t.Fatalf("streaming cancellation lost ownership: %v", err)
			}
			progress, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := db.PingContext(progress); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetainedPostgresNativeCancellationPreservesExactSession(t *testing.T) {
	for _, surface := range []string{"exec", "query_row", "prepared_exec", "prepared_rows"} {
		t.Run(surface, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(`CREATE FUNCTION retained_cancel_probe() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'query-active'; PERFORM pg_sleep(30); RETURN 1; END $$`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var beforePID int
			if err := conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
				t.Fatal(err)
			}
			if err := conn.Raw(func(raw any) error {
				pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { cancel() })
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "native-cancel-exact-session")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t err=%v", acquired, err)
			}
			defer lease.Release(context.Background())
			err = lease.RunTransaction(ctx, func(queryCtx context.Context, tx *sql.Tx) error {
				const query = "SELECT retained_cancel_probe()"
				switch surface {
				case "exec":
					_, err := tx.ExecContext(queryCtx, query)
					return err
				case "query_row":
					var value int
					return tx.QueryRowContext(queryCtx, query).Scan(&value)
				default:
					stmt, err := tx.PrepareContext(queryCtx, query)
					if err != nil {
						return err
					}
					defer stmt.Close()
					if surface == "prepared_exec" {
						_, err := stmt.ExecContext(queryCtx)
						return err
					}
					rows, err := stmt.QueryContext(queryCtx)
					if err != nil {
						return err
					}
					for rows.Next() {
					}
					return errors.Join(rows.Err(), rows.Close())
				}
			})
			if !onlyCancellationLeaves(err, context.Canceled) {
				t.Fatalf("retained native cancellation lost: %v", err)
			}
			next, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := lease.ProveCurrent(next); err != nil {
				t.Fatalf("healthy advisory possession lost: %v", err)
			}
			if err := lease.RunTransaction(next, func(ctx context.Context, tx *sql.Tx) error {
				var afterPID int
				if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&afterPID); err != nil {
					return err
				}
				if afterPID != beforePID {
					return fmt.Errorf("session changed: %d -> %d", beforePID, afterPID)
				}
				return nil
			}); err != nil {
				t.Fatalf("next transaction failed: %v", err)
			}
		})
	}
}

func TestPostgresNativeQueryDeadline(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprintf("retained=%t", retained), func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(`CREATE FUNCTION deadline_probe() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'deadline-query-active'; PERFORM pg_sleep(30); RETURN 1; END $$`); err != nil {
				t.Fatal(err)
			}
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			noticed := false
			if err := conn.Raw(func(raw any) error {
				pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { noticed = true })
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			b, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			run := b.RunReadTransaction
			var lease *AdvisoryLockLease
			if retained {
				var acquired bool
				lease, acquired, err = AcquireAdvisoryLockLease(context.Background(), db, "native-deadline")
				if err != nil || !acquired {
					t.Fatalf("acquire=%t err=%v", acquired, err)
				}
				defer lease.Release(context.Background())
				run = lease.RunTransaction
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = run(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var value int
				return tx.QueryRowContext(ctx, "SELECT deadline_probe()").Scan(&value)
			})
			if !noticed {
				t.Fatal("deadline elapsed before native query execution")
			}
			if !onlyCancellationLeaves(err, context.DeadlineExceeded) {
				t.Fatalf("deadline lost native attribution: %v", err)
			}
			if lease != nil {
				if err := lease.ProveCurrent(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
