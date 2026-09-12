package runforkpersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// This trace forwards real SQL and transaction exits; it does not fabricate
// settlement. It observes the exact ordering of session lock completion and BEGIN.
type forkMutationConnector struct {
	driver.Connector
	keyed, lockHeld            bool
	begins, commits, rollbacks int
	orderWrong, contextWrong   bool
	afterLock                  func()
}

func (p *forkMutationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := p.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &forkMutationConn{Conn: c, probe: p}, nil
}

type forkMutationConn struct {
	driver.Conn
	probe *forkMutationConnector
}

func (c *forkMutationConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if strings.Contains(query, "SELECT pg_advisory_lock(") && err == nil {
		c.probe.lockHeld = true
		c.probe.contextWrong = c.probe.contextWrong || ctx.Done() != nil
		if c.probe.afterLock != nil {
			c.probe.afterLock()
		}
	}
	return result, err
}

func (c *forkMutationConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *forkMutationConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *forkMutationConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *forkMutationConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.probe.begins++
	c.probe.orderWrong = c.probe.orderWrong || (c.probe.keyed && !c.probe.lockHeld)
	c.probe.contextWrong = c.probe.contextWrong || ctx.Done() != nil
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &forkMutationTx{Tx: tx, probe: c.probe}, nil
}

type forkMutationTx struct {
	driver.Tx
	probe *forkMutationConnector
}

func (tx *forkMutationTx) Commit() error {
	tx.probe.commits++
	return tx.Tx.Commit()
}

func (tx *forkMutationTx) Rollback() error {
	tx.probe.rollbacks++
	return tx.Tx.Rollback()
}

func TestConversationForkGracefulMutation(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		name := "unkeyed"
		if keyed {
			name = "keyed"
		}
		t.Run(name, func(t *testing.T) {
			for _, scenario := range []string{"success", "before_admission", "during_sql", "before_commit", "panic", "sql_failure", "commit_failure", "after_lock", "lost_lock"} {
				if !keyed && (scenario == "after_lock" || scenario == "lost_lock") {
					continue
				}
				t.Run(scenario, func(t *testing.T) {
					dsn, observer, _ := testutil.StartPostgres(t)
					if _, err := observer.Exec(`CREATE TABLE fork_graceful (n integer UNIQUE DEFERRABLE INITIALLY DEFERRED)`); err != nil {
						t.Fatal(err)
					}
					connector, err := pq.NewConnector(dsn)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					noticed := false
					probe := &forkMutationConnector{Connector: pq.ConnectorWithNoticeHandler(connector, func(*pq.Error) {
						noticed = true
						cancel()
					}), keyed: keyed}
					db := sql.OpenDB(probe)
					db.SetMaxOpenConns(1)
					defer db.Close()
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					store := conversationForkStore{db: backend, dialect: conversationForkPostgres}
					var beforePID int
					if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
						t.Fatal(err)
					}
					if scenario == "before_admission" {
						cancel()
					}
					if scenario == "after_lock" {
						probe.afterLock = cancel
					}
					const key = "fork-graceful-key"
					run := func(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
						if keyed {
							return store.runForkMutation(ctx, key, true, fn)
						}
						return store.runMutation(ctx, false, fn)
					}
					calls := 0
					primary := errors.New("fork callback panic")
					var recovered any
					func() {
						defer func() { recovered = recover() }()
						err = run(ctx, func(sqlCtx context.Context, tx *sql.Tx) error {
							calls++
							var isolation string
							if err := tx.QueryRowContext(sqlCtx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
								return err
							}
							want := "read committed"
							if keyed {
								want = "serializable"
							}
							if isolation != want || sqlCtx.Done() != nil {
								t.Errorf("isolation=%s want=%s detached=%t", isolation, want, sqlCtx.Done() == nil)
							}
							if keyed {
								assertForkLockAvailable(t, observer, key, false)
							}
							if _, err := tx.ExecContext(sqlCtx, "INSERT INTO fork_graceful VALUES (1)"); err != nil {
								return err
							}
							switch scenario {
							case "during_sql":
								// A server NOTICE cancels the logical caller while actual
								// PostgreSQL work is admitted. The second insert must drain.
								if _, err := tx.ExecContext(sqlCtx, `DO $$ BEGIN RAISE NOTICE 'fork admitted'; PERFORM pg_sleep(0.02); INSERT INTO fork_graceful VALUES (2); END $$`); err != nil {
									return err
								}
								var count int
								if err := tx.QueryRowContext(sqlCtx, "SELECT count(*) FROM fork_graceful").Scan(&count); err != nil {
									return err
								}
								if !noticed || count != 2 {
									t.Errorf("admitted work did not drain: notice=%t rows=%d", noticed, count)
								}
							case "before_commit":
								cancel()
							case "panic":
								panic(primary)
							case "sql_failure":
								_, err := tx.ExecContext(sqlCtx, "SELECT 1/0")
								return err
							case "commit_failure":
								_, err := tx.ExecContext(sqlCtx, "INSERT INTO fork_graceful VALUES (1)")
								return err
							case "lost_lock":
								_, err := tx.ExecContext(sqlCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key)
								return err
							}
							return nil
						})
					}()
					if scenario == "panic" && recovered != primary {
						t.Fatalf("panic identity=%v", recovered)
					}
					if scenario != "panic" && recovered != nil {
						t.Fatalf("unexpected panic: %v", recovered)
					}
					canceled := scenario == "before_admission" || scenario == "during_sql" || scenario == "before_commit" || scenario == "after_lock"
					if canceled && !errors.Is(err, context.Canceled) {
						t.Errorf("logical stop lost: %v", err)
					}
					if scenario == "success" && err != nil {
						t.Errorf("healthy work: %v", err)
					}
					if scenario == "sql_failure" || scenario == "commit_failure" {
						var pgErr *pq.Error
						want := pq.ErrorCode("22012")
						if scenario == "commit_failure" {
							want = "23505"
						}
						if !errors.As(err, &pgErr) || pgErr.Code != want {
							t.Errorf("SQL failure lost: %v", err)
						}
					}
					if scenario == "lost_lock" && (err == nil || !strings.Contains(err.Error(), "advisory lock was not held")) {
						t.Errorf("cleanup failure lost: %v", err)
					}
					wantCalls, wantCommits, wantRollbacks := 1, 0, 1
					if scenario == "before_admission" || scenario == "after_lock" {
						wantCalls, wantRollbacks = 0, 0
					}
					if scenario == "success" || scenario == "commit_failure" || scenario == "lost_lock" {
						wantCommits, wantRollbacks = 1, 0
					}
					if calls != wantCalls || probe.begins != wantCalls || probe.commits != wantCommits || probe.rollbacks != wantRollbacks || probe.orderWrong || probe.contextWrong {
						t.Errorf("calls=%d BEGIN=%d COMMIT=%d ROLLBACK=%d orderWrong=%t contextWrong=%t", calls, probe.begins, probe.commits, probe.rollbacks, probe.orderWrong, probe.contextWrong)
					}
					var durable int
					if err := observer.QueryRow("SELECT count(*) FROM fork_graceful").Scan(&durable); err != nil {
						t.Fatal(err)
					}
					wantDurable := 0
					if scenario == "success" || scenario == "lost_lock" {
						wantDurable = 1
					}
					if durable != wantDurable {
						t.Errorf("durable=%d want=%d", durable, wantDurable)
					}
					if keyed && scenario == "commit_failure" {
						// Disposal is local; only server acquisition proves remote release.
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := acquireForkLockAfterDisposal(ctx, observer, key); err != nil {
							t.Fatalf("server acquisition after disposal: %v", err)
						}
					} else {
						assertForkLockAvailable(t, observer, key, true)
					}
					probe.afterLock = nil
					probe.lockHeld = false
					var afterPID int
					if err := run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
						return tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&afterPID)
					}); err != nil {
						t.Fatalf("successor failed: %v", err)
					}
					unsafe := scenario == "commit_failure" || scenario == "lost_lock"
					if (beforePID != afterPID) != unsafe {
						t.Errorf("socket disposition pid %d -> %d unsafe=%t", beforePID, afterPID, unsafe)
					}
					assertForkLockAvailable(t, observer, key, true)
					t.Logf("callbacks=%d durable=%d pid=%d->%d unsafe=%t error=%v", calls, durable, beforePID, afterPID, unsafe, err)
				})
			}
		})
	}
}

func assertForkLockAvailable(t *testing.T, db *sql.DB, key string, want bool) {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var acquired bool
	if err := conn.QueryRowContext(context.Background(), `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, key).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if acquired {
		var unlocked bool
		if err := conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key).Scan(&unlocked); err != nil || !unlocked {
			t.Fatalf("competitor unlock=%t: %v", unlocked, err)
		}
	}
	if acquired != want {
		t.Errorf("competing lock acquired=%t want=%t", acquired, want)
	}
}

func acquireForkLockAfterDisposal(ctx context.Context, db *sql.DB, key string) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		return err
	}
	var unlocked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key).Scan(&unlocked); err != nil {
		return err
	}
	if !unlocked {
		return errors.New("post-disposal observer did not unlock its exact session")
	}
	return nil
}

func TestForkPostDisposalObservationRejectsHeldLock(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	owner, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	const key = "fork-post-disposal-held-control"
	if _, err := owner.ExecContext(context.Background(), `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := acquireForkLockAfterDisposal(ctx, db, key); err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("held-lock observation err=%v deadline=%v", err, ctx.Err())
	}
	var unlocked bool
	if err := owner.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key).Scan(&unlocked); err != nil || !unlocked {
		t.Fatalf("owner unlock=%t: %v", unlocked, err)
	}
	proofCtx, proofCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer proofCancel()
	if err := acquireForkLockAfterDisposal(proofCtx, db, key); err != nil {
		t.Fatalf("observation after acknowledged release: %v", err)
	}
	assertForkLockAvailable(t, db, key, true)
}
