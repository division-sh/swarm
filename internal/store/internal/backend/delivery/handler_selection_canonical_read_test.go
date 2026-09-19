package delivery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

// This observes physical SQL at the native driver, not adapter invocations.
type selectionSQLProbe struct {
	mu               sync.Mutex
	selects, inserts int
	beforeInsert     func()
	commitFailure    func(error)
}

func (p *selectionSQLProbe) observe(query string) {
	q := strings.ToUpper(strings.TrimSpace(query))
	if !strings.Contains(q, "EVENT_DELIVERY_HANDLER_RULE_SELECTIONS") {
		return
	}
	p.mu.Lock()
	var hook func()
	if strings.HasPrefix(q, "SELECT ") {
		p.selects++
	}
	if strings.HasPrefix(q, "INSERT ") {
		p.inserts++
		hook = p.beforeInsert
	}
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (p *selectionSQLProbe) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.selects, p.inserts
}

type selectionConnector struct {
	driver.Connector
	probe *selectionSQLProbe
}

func (c selectionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &selectionConn{Conn: conn, probe: c.probe}, nil
}

type selectionSQLiteConnector struct{ dsn string }

func (c selectionSQLiteConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c selectionSQLiteConnector) Connect(context.Context) (driver.Conn, error) {
	return c.Driver().Open(c.dsn)
}

type selectionConn struct {
	driver.Conn
	probe *selectionSQLProbe
}

func (c *selectionConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.probe.observe(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *selectionConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.probe.observe(query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *selectionConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &selectionTx{Tx: tx, probe: c.probe}, nil
}

type selectionTx struct {
	driver.Tx
	probe *selectionSQLProbe
}

func (tx *selectionTx) Commit() error {
	err := tx.Tx.Commit()
	tx.probe.mu.Lock()
	hook := tx.probe.commitFailure
	tx.probe.mu.Unlock()
	if err != nil && hook != nil {
		hook(err)
	}
	return err
}

// The lower writer fixture isolates its scalar table contract. Real schema,
// terminal lifecycle and revision-finalizer coverage lives in runtimepersistence.
func selectionWriterFixture(t *testing.T, backend string) (*sql.DB, *Adapter, *selectionSQLProbe) {
	t.Helper()
	var connector driver.Connector
	idType := "TEXT"
	if backend == "postgres" {
		dsn, _, _ := testutil.StartEmptyPostgres(t)
		var err error
		connector, err = pq.NewConnector(dsn)
		if err != nil {
			t.Fatal(err)
		}
		idType = "UUID"
	} else {
		connector = selectionSQLiteConnector{dsn: "file:" + filepath.Join(t.TempDir(), "selection.db") + "?_pragma=busy_timeout(1000)"}
	}
	probe := &selectionSQLProbe{}
	db := sql.OpenDB(selectionConnector{Connector: connector, probe: probe})
	t.Cleanup(func() { _ = db.Close() })
	_, err := db.Exec(`CREATE TABLE event_delivery_handler_rule_selections (
		delivery_id ` + idType + ` PRIMARY KEY, selection_context TEXT NOT NULL,
		disposition TEXT NOT NULL, flow_path TEXT, declaration_family TEXT,
		semantic_path TEXT, display_label TEXT NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	dialect := DialectSQLite
	if backend == "postgres" {
		dialect = DialectPostgres
	}
	return db, &Adapter{dialect: dialect}, probe
}

func selectionFact(t *testing.T, label string) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	ref, err := identity.AdmitDeclarationIdentity(".", "handler_rule", `nodes["worker"].handlers["event"].rules[0]`)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := handlerselection.Selected(handlerselection.ContextRules, ref, label)
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func selectionEffects(t *testing.T, got *runforkrevision.Effects, runID string, ids ...string) {
	t.Helper()
	want := runforkrevision.NewEffects()
	for _, id := range ids {
		if err := want.AddFact(runID, runforkrevision.FamilyEventDeliveries, id); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("effects differ from exact delivery keys %v", ids)
	}
}

func TestHandlerSelectionCanonicalReadWriterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, a, probe := selectionWriterFixture(t, backend)
			ctx := context.Background()
			runID := uuid.NewString()
			insertSelects := 1
			noMatch, err := handlerselection.NoMatch(handlerselection.ContextRules)
			if err != nil {
				t.Fatal(err)
			}
			for _, fact := range []handlerselection.HandlerRuleSelectionFact{selectionFact(t, "selected"), noMatch, handlerselection.NotApplicable()} {
				id := uuid.NewString()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				beforeSelects, beforeInserts := probe.counts()
				effects := runforkrevision.NewEffects()
				if err := a.persistHandlerRuleSelection(ctx, tx, effects, runID, id, fact); err != nil {
					t.Fatal(err)
				}
				selects, inserts := probe.counts()
				if selects != beforeSelects+insertSelects || inserts != beforeInserts+1 {
					t.Fatalf("insert SQL: selects=%d inserts=%d", selects-beforeSelects, inserts-beforeInserts)
				}
				selectionEffects(t, effects, runID, id)
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				effects = runforkrevision.NewEffects()
				if err := a.persistHandlerRuleSelection(ctx, tx, effects, runID, id, fact); err != nil {
					t.Fatal(err)
				}
				afterSelects, afterInserts := probe.counts()
				if afterSelects != selects+1 || afterInserts != inserts+1 {
					t.Fatal("equal conflict must retain one fresh SELECT")
				}
				selectionEffects(t, effects, runID, id)
				conflictEffects := runforkrevision.NewEffects()
				if err := a.persistHandlerRuleSelection(ctx, tx, conflictEffects, runID, id, selectionFact(t, "contradiction")); !errors.Is(err, deliverylifecycle.ErrConflict) {
					t.Fatalf("conflict = %v", err)
				}
				selectionEffects(t, conflictEffects, runID)
				persisted, err := a.handlerRuleSelection(ctx, tx, id)
				if err != nil || !persisted.Equal(fact) {
					t.Fatalf("canonical fact changed: %+v %v", persisted, err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			// Execute the former INSERT/SELECT sequence on a distinct row to
			// measure the old physical SQL cost, not infer it from call counts.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			id := uuid.NewString()
			before, _ := probe.counts()
			_, err = tx.ExecContext(ctx, `INSERT INTO event_delivery_handler_rule_selections
				(delivery_id, selection_context, disposition, display_label) VALUES ($1,$2,$3,$4) ON CONFLICT (delivery_id) DO NOTHING`, id, string(noMatch.Context()), string(noMatch.Disposition()), noMatch.DisplayLabel())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.handlerRuleSelection(ctx, tx, id); err != nil {
				t.Fatal(err)
			}
			after, _ := probe.counts()
			if after-before != 1 {
				t.Fatal("baseline physical read missing")
			}
			t.Logf("physical SQL per successful insert: former sequence INSERT=1 SELECT=1; production INSERT=1 SELECT=%d; equal conflict INSERT=1 SELECT=1", insertSelects)
		})
	}
}

func TestHandlerSelectionCanonicalReadRollbackAndCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, a, _ := selectionWriterFixture(t, backend)
			ctx := context.Background()
			runID, id := uuid.NewString(), uuid.NewString()
			fact := selectionFact(t, "retry")
			effects := runforkrevision.NewEffects()
			reset := effects.AttemptReset()
			for attempt := 0; attempt < 2; attempt++ {
				reset()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err := a.persistHandlerRuleSelection(ctx, tx, effects, runID, id, fact); err != nil {
					t.Fatal(err)
				}
				selectionEffects(t, effects, runID, id)
				if attempt == 0 {
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					var count int
					if err := db.QueryRow(`SELECT count(*) FROM event_delivery_handler_rule_selections`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("rolled-back selection survived: %d %v", count, err)
					}
				} else if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`UPDATE event_delivery_handler_rule_selections SET selection_context='corrupt' WHERE delivery_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			badEffects := runforkrevision.NewEffects()
			if err := a.persistHandlerRuleSelection(ctx, tx, badEffects, runID, id, fact); err == nil || !strings.Contains(err.Error(), "hydrate delivery handler rule selection") {
				t.Fatalf("corrupt replay = %v", err)
			}
			selectionEffects(t, badEffects, runID)
			if _, err := a.handlerRuleSelection(ctx, tx, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("missing selection = %v", err)
			}
		})
	}
}

func TestHandlerSelectionCanonicalReadConcurrentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, equal := range []bool{true, false} {
			name := "equal"
			if !equal {
				name = "conflict"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				db, a, probe := selectionWriterFixture(t, backend)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				runID, id := uuid.NewString(), uuid.NewString()
				fact := selectionFact(t, "first")
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err := a.persistHandlerRuleSelection(ctx, tx, runforkrevision.NewEffects(), runID, id, fact); err != nil {
					t.Fatal(err)
				}
				entered := make(chan struct{})
				var once sync.Once
				probe.mu.Lock()
				probe.beforeInsert = func() { once.Do(func() { close(entered) }) }
				probe.mu.Unlock()
				other := fact
				if !equal {
					other = selectionFact(t, "second")
				}
				effects := runforkrevision.NewEffects()
				done := make(chan error, 1)
				go func() {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						done <- err
						return
					}
					defer tx.Rollback()
					err = a.persistHandlerRuleSelection(ctx, tx, effects, runID, id, other)
					if err == nil {
						err = tx.Commit()
					}
					done <- err
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case err := <-done:
					t.Fatalf("contender escaped uncommitted first insert: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				err = <-done
				if equal {
					if err != nil {
						t.Fatal(err)
					}
					selectionEffects(t, effects, runID, id)
				} else {
					if !errors.Is(err, deliverylifecycle.ErrConflict) {
						t.Fatalf("concurrent conflict = %v", err)
					}
					selectionEffects(t, effects, runID)
				}
				selects, inserts := probe.counts()
				wantSelects := 2
				if selects != wantSelects || inserts != 2 {
					t.Fatalf("concurrent SQL selects=%d inserts=%d", selects, inserts)
				}
			})
		}
	}
}

func TestHandlerSelectionCanonicalReadSQLiteNativeBusyRetry(t *testing.T) {
	db, a, probe := selectionWriterFixture(t, "sqlite")
	ctx := context.Background()
	// DELETE journal permits the writer but a native reader blocks COMMIT.
	if _, err := db.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	reader, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var count int
	if err := reader.QueryRow(`SELECT count(*) FROM event_delivery_handler_rule_selections`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	busyCommits := 0
	probe.commitFailure = func(err error) {
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&255 != 5 {
			t.Errorf("not native SQLITE_BUSY: %v", err)
		}
		busyCommits++
		if err := reader.Rollback(); err != nil {
			t.Error(err)
		}
	}
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	runID, rolledBackID, committedID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	fact := selectionFact(t, "native-retry")
	effects := runforkrevision.NewEffects()
	reset := effects.AttemptReset()
	attempts := 0
	err = backend.RunTransaction(ctx, "selection canonical read native busy", func(ctx context.Context, tx *sql.Tx) error {
		reset()
		attempts++
		id := committedID
		if attempts == 1 {
			id = rolledBackID
		}
		return a.persistHandlerRuleSelection(ctx, tx, effects, runID, id, fact)
	})
	if err != nil || attempts != 2 || busyCommits != 1 {
		t.Fatalf("native retry attempts=%d busy=%d err=%v", attempts, busyCommits, err)
	}
	selectionEffects(t, effects, runID, committedID)
	selects, inserts := probe.counts()
	// SQLite preserves each insertion's post-trigger read plus the reader COUNT.
	if selects != 3 || inserts != 2 {
		t.Fatalf("native retry SQL selects=%d inserts=%d", selects, inserts)
	}
	var id string
	if err := db.QueryRow(`SELECT delivery_id FROM event_delivery_handler_rule_selections`).Scan(&id); err != nil || id != committedID {
		t.Fatalf("retry readback=%s err=%v", id, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM event_delivery_handler_rule_selections`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retry count=%d err=%v", count, err)
	}
}
