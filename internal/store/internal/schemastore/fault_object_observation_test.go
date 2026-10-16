package schemastore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type faultObservationOperation struct {
	name  string
	query string
	args  []driver.NamedValue
	read  func(context.Context, *sql.Tx) ([]int, error)
}

func faultObservationOperations(postgres bool, id uuid.UUID) []faultObservationOperation {
	uuidQuery := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=$1`
	diagnosticQuery := `SELECT 0,(SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='fail_delivery_dead_letter_insert')`
	recoveryQuery := `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='fail_node_recovery_poison_update'`
	if postgres {
		uuidQuery = `SELECT (SELECT COUNT(*) FROM pg_trigger WHERE tgname=$1) + (SELECT COUNT(*) FROM pg_proc WHERE proname=$1 AND pronamespace=current_schema()::regnamespace)`
		diagnosticQuery = `SELECT
	(SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='fail_delivery_dead_letter_insert'),
	(SELECT COUNT(*) FROM pg_trigger WHERE tgname='fail_delivery_dead_letter_insert' AND NOT tgisinternal)`
		recoveryQuery = `SELECT
	(SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname='fail_node_recovery_poison_update_fn')+
	(SELECT COUNT(*) FROM pg_trigger WHERE tgname='fail_node_recovery_poison_update' AND NOT tgisinternal)`
	}
	var out []faultObservationOperation
	for _, operation := range []struct {
		prefix string
		read   func(context.Context, *sql.Tx, bool, uuid.UUID) (int, error)
	}{
		{"publication_completion_", CountPublicationCompletionFaultObjectsTx},
		{"publication_materialization_", CountPublicationMaterializationFaultObjectsTx},
		{"publication_replay_", CountPublicationReplayFaultObjectsTx},
	} {
		out = append(out, faultObservationOperation{
			name: operation.prefix, query: uuidQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: operation.prefix + strings.ReplaceAll(id.String(), "-", "")}},
			read: func(ctx context.Context, tx *sql.Tx) ([]int, error) {
				count, err := operation.read(ctx, tx, postgres, id)
				return []int{count}, err
			},
		})
	}
	return append(out, faultObservationOperation{
		name: "diagnostic", query: diagnosticQuery,
		read: func(ctx context.Context, tx *sql.Tx) ([]int, error) {
			objects, err := ReadConformanceDiagnosticFaultObjectsTx(ctx, tx, postgres)
			return []int{objects.Functions, objects.Triggers}, err
		},
	}, faultObservationOperation{
		name: "recovery", query: recoveryQuery,
		read: func(ctx context.Context, tx *sql.Tx) ([]int, error) {
			count, err := CountNodeRecoveryPoisonFaultObjectsTx(ctx, tx, postgres)
			return []int{count}, err
		},
	})
}

func TestFaultObjectObservationExactTransactionIdentityAndErrors(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, id := range []uuid.UUID{uuid.MustParse("12345678-1234-4234-8234-123456789abc"), uuid.MustParse("abcdef01-2345-4678-9123-abcdef012345")} {
			for _, operation := range faultObservationOperations(postgres, id) {
				name := map[bool]string{false: "sqlite", true: "postgres"}[postgres] + "/" + id.String() + "/" + operation.name
				t.Run(name, func(t *testing.T) { proveFaultObservationCuts(t, operation, id) })
			}
		}
	}
}

type faultObservationCut struct {
	name                       string
	values                     []driver.Value
	queryErr, rowErr, closeErr error
	empty                      bool
	want                       []int
	wantErr                    error
	scanErr                    bool
}

func proveFaultObservationCuts(t *testing.T, operation faultObservationOperation, id uuid.UUID) {
	t.Helper()
	queryErr := errors.New("catalog query failed")
	rowErr := errors.New("catalog row failed")
	closeErr := errors.New("catalog row cleanup failed")
	values, want, zero := []driver.Value{int64(7)}, []int{7}, []int{0}
	if operation.name == "diagnostic" {
		values, want, zero = []driver.Value{int64(3), int64(5)}, []int{3, 5}, []int{0, 0}
	}
	badValues := append([]driver.Value(nil), values...)
	badValues[len(badValues)-1] = "not an integer"
	partial := append([]int(nil), want...)
	partial[len(partial)-1] = 0
	for _, cut := range []faultObservationCut{
		{name: "counts", values: values, want: want},
		{name: "query_error", values: values, queryErr: queryErr, want: zero, wantErr: queryErr},
		{name: "row_error", values: values, rowErr: rowErr, want: zero, wantErr: rowErr},
		{name: "close_error", values: values, closeErr: closeErr, want: want, wantErr: closeErr},
		{name: "no_row", values: values, empty: true, want: zero, wantErr: sql.ErrNoRows},
		{name: "scan_error", values: badValues, want: partial, scanErr: true},
	} {
		t.Run(cut.name, func(t *testing.T) { proveFaultObservationCut(t, operation, id, cut) })
	}
}

func proveFaultObservationCut(t *testing.T, operation faultObservationOperation, id uuid.UUID, cut faultObservationCut) {
	t.Helper()
	control := &faultObservationConnector{values: cut.values, queryErr: cut.queryErr, rowErr: cut.rowErr, closeErr: cut.closeErr, empty: cut.empty}
	db := sql.OpenDB(control)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.WithValue(context.Background(), faultObservationContextKey{}, id)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	got, err := operation.read(ctx, tx)
	if !reflect.DeepEqual(got, cut.want) {
		t.Fatalf("counts = %v, want %v (err=%v)", got, cut.want, err)
	}
	if cut.scanErr {
		if err == nil || !strings.Contains(err.Error(), "not an integer") {
			t.Fatalf("scan error was hidden: %v", err)
		}
	} else if !errors.Is(err, cut.wantErr) {
		t.Fatalf("error = %v, want %v", err, cut.wantErr)
	}
	if control.query != operation.query || !reflect.DeepEqual(control.args, operation.args) || control.ctx != ctx {
		t.Fatalf("query identity changed: query=%q args=%v context=%v", control.query, control.args, control.ctx)
	}
	if control.connections != 1 || control.begins != 1 || control.queries != 1 || control.commits != 0 || control.rollbacks != 0 {
		t.Fatalf("reader acquired or settled a transaction: %+v", control)
	}
	if err := tx.Rollback(); err != nil || control.rollbacks != 1 {
		t.Fatalf("caller lost rollback ownership: count=%d err=%v", control.rollbacks, err)
	}
}

func TestFaultObjectObservationEmptyCatalogBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) {
			var db *sql.DB
			if postgres {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				store, err := NewSQLite(filepath.Join(t.TempDir(), "fault-observation.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				db = store.backend.ConstructionHandle()
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			for _, operation := range faultObservationOperations(postgres, uuid.New()) {
				t.Run(operation.name, func(t *testing.T) {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					if _, err := operation.read(canceled, tx); !errors.Is(err, context.Canceled) {
						t.Fatalf("canceled observation = %v", err)
					}
					got, err := operation.read(ctx, tx)
					if err != nil || !reflect.DeepEqual(got, make([]int, len(got))) {
						t.Fatalf("empty catalog counts = %v, %v", got, err)
					}
				})
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			for _, operation := range faultObservationOperations(postgres, uuid.New()) {
				if _, err := operation.read(ctx, tx); !errors.Is(err, sql.ErrTxDone) {
					t.Fatalf("%s used a transaction after caller rollback: %v", operation.name, err)
				}
			}
		})
	}
}

type faultObservationContextKey struct{}

type faultObservationConnector struct {
	values                       []driver.Value
	queryErr, rowErr, closeErr   error
	empty                        bool
	query                        string
	args                         []driver.NamedValue
	ctx                          context.Context
	connections, begins, queries int
	commits, rollbacks           int
}

func (c *faultObservationConnector) Driver() driver.Driver { return faultObservationDriver{} }
func (c *faultObservationConnector) Connect(context.Context) (driver.Conn, error) {
	c.connections++
	return faultObservationConn{c}, nil
}

type faultObservationDriver struct{}

func (faultObservationDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector only")
}

type faultObservationConn struct{ control *faultObservationConnector }

func (faultObservationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (faultObservationConn) Close() error { return nil }
func (c faultObservationConn) Begin() (driver.Tx, error) {
	c.control.begins++
	return faultObservationTx{c.control}, nil
}
func (c faultObservationConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.control.queries++
	c.control.ctx, c.control.query, c.control.args = ctx, query, append([]driver.NamedValue(nil), args...)
	if c.control.queryErr != nil {
		return nil, c.control.queryErr
	}
	return &faultObservationRows{control: c.control}, nil
}

type faultObservationTx struct{ control *faultObservationConnector }

func (tx faultObservationTx) Commit() error   { tx.control.commits++; return nil }
func (tx faultObservationTx) Rollback() error { tx.control.rollbacks++; return nil }

type faultObservationRows struct {
	control *faultObservationConnector
	read    bool
}

func (r *faultObservationRows) Columns() []string { return make([]string, len(r.control.values)) }
func (r *faultObservationRows) Close() error      { return r.control.closeErr }
func (r *faultObservationRows) Next(dest []driver.Value) error {
	if r.control.rowErr != nil {
		return r.control.rowErr
	}
	if r.read || r.control.empty {
		return io.EOF
	}
	r.read = true
	copy(dest, r.control.values)
	return nil
}
