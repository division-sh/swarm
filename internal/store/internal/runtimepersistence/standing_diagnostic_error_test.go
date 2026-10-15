package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

// This local driver controls display-row decoding and cursor failures only; it
// never stands in for native authority, persistence, or inspection ownership.
type standingDisplayConnector struct {
	query                       string
	values                      [][]driver.Value
	queryErr, nextErr, closeErr error
}

func (c standingDisplayConnector) Driver() driver.Driver { return standingDisplayDriver{} }
func (c standingDisplayConnector) Connect(context.Context) (driver.Conn, error) {
	return standingDisplayConn{c}, nil
}

type standingDisplayDriver struct{}

func (standingDisplayDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector only")
}

type standingDisplayConn struct{ config standingDisplayConnector }

func (standingDisplayConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no prepare") }
func (standingDisplayConn) Close() error                        { return nil }
func (standingDisplayConn) Begin() (driver.Tx, error)           { return standingDisplayTx{}, nil }

type standingDisplayTx struct{}

func (standingDisplayTx) Commit() error   { return nil }
func (standingDisplayTx) Rollback() error { return nil }
func (c standingDisplayConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != c.config.query || len(args) != 0 {
		return nil, errors.New("diagnostic query changed shape, global scope, order or arguments")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.config.queryErr != nil {
		return nil, c.config.queryErr
	}
	return &standingDisplayRows{config: c.config}, nil
}

type standingDisplayRows struct {
	config standingDisplayConnector
	index  int
}

func (r *standingDisplayRows) Columns() []string { return make([]string, len(r.config.values[0])) }
func (r *standingDisplayRows) Close() error      { return r.config.closeErr }
func (r *standingDisplayRows) Next(dest []driver.Value) error {
	if r.index == len(r.config.values) {
		if r.config.nextErr != nil {
			return r.config.nextErr
		}
		return io.EOF
	}
	copy(dest, r.config.values[r.index])
	r.index++
	return nil
}

type standingDisplayCase struct {
	name   string
	query  [2]string
	values [][]driver.Value
	read   func(context.Context, *sql.Tx, bool) (any, error)
	want   any
}

func standingDisplayCases() []standingDisplayCase {
	return []standingDisplayCase{
		{"names", [2]string{`SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`, `SELECT event_name, COUNT(*) FROM events GROUP BY event_name ORDER BY event_name`},
			[][]driver.Value{{"a", int64(2)}, {nil, int64(3)}, {"z", int64(4)}},
			func(ctx context.Context, tx *sql.Tx, pg bool) (any, error) {
				return eventrecord.ReadStandingEventNameDiagnosticsTx(ctx, tx, pg)
			}, []eventrecord.StandingEventNameDiagnostic{{Name: "a", Count: 2}, {Name: "z", Count: 4}}},
		{"logs", [2]string{`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`, `SELECT payload::text FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at`},
			[][]driver.Value{{`{"z": 2, "a":1}`}, {nil}, {`{"a":1}`}},
			func(ctx context.Context, tx *sql.Tx, pg bool) (any, error) {
				return eventrecord.ReadStandingRuntimeLogDiagnosticsTx(ctx, tx, pg)
			}, []string{`{"z": 2, "a":1}`, `{"a":1}`}},
		{"deliveries", [2]string{`SELECT event_id, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`, `SELECT event_id::text, subscriber_id, COALESCE(status, '') FROM event_deliveries ORDER BY created_at`},
			[][]driver.Value{{"e1", "s", ""}, {nil, "s", "done"}, {"e2", "s", "done"}},
			func(ctx context.Context, tx *sql.Tx, pg bool) (any, error) {
				return delivery.ReadStandingDeliveryDiagnosticsTx(ctx, tx, pg)
			}, []delivery.StandingDeliveryDiagnostic{{EventID: "e1", Subscriber: "s", Status: ""}, {EventID: "e2", Subscriber: "s", Status: "done"}}},
	}
}

func TestStandingDiagnosticProjectionsPreserveDisplayScanPolicies(t *testing.T) {
	for _, c := range standingDisplayCases() {
		for dialect, pg := range []bool{false, true} {
			t.Run(c.name+map[bool]string{false: "/sqlite", true: "/postgres"}[pg], func(t *testing.T) {
				got, err := readStandingDisplayCase(t, c, pg, standingDisplayConnector{query: c.query[dialect], values: c.values})
				if c.name == "names" && !pg {
					if err == nil || !reflect.ValueOf(got).IsNil() {
						t.Fatalf("SQLite initial scan must refuse without partial names: %v %v", got, err)
					}
				} else if err != nil || !reflect.DeepEqual(got, c.want) {
					t.Fatalf("malformed display row changed valid rows/text: %v want %v err=%v", got, c.want, err)
				}
			})
		}
	}
}

func TestStandingDiagnosticProjectionsDiscardFailedCursorSections(t *testing.T) {
	failure := errors.New("diagnostic read failed")
	for _, c := range standingDisplayCases() {
		for dialect, pg := range []bool{false, true} {
			for _, phase := range []string{"query", "cursor", "close"} {
				t.Run(c.name+map[bool]string{false: "/sqlite/", true: "/postgres/"}[pg]+phase, func(t *testing.T) {
					config := standingDisplayConnector{query: c.query[dialect], values: [][]driver.Value{c.values[0]}}
					switch phase {
					case "query":
						config.queryErr = failure
					case "cursor":
						config.nextErr = failure
					case "close":
						config.closeErr = failure
					}
					got, err := readStandingDisplayCase(t, c, pg, config)
					if !errors.Is(err, failure) || !reflect.ValueOf(got).IsNil() {
						t.Fatalf("failed %s returned provisional section: %v %v", phase, got, err)
					}
				})
			}
		}
	}
}

func readStandingDisplayCase(t *testing.T, c standingDisplayCase, pg bool, config standingDisplayConnector) (any, error) {
	t.Helper()
	db := sql.OpenDB(config)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return c.read(context.Background(), tx, pg)
}

func TestStandingDiagnosticPortsRefuseInvalidCancelledClosedAndForeignOwnersBothStores(t *testing.T) {
	var pg *PostgresStore
	var sq *SQLiteRuntimeStore
	for _, owner := range []any{nil, pg, sq, &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		requireStandingDiagnosticPortRefusal(t, context.Background(), owner, nil)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newWorkflowTargetConstructionFixture(t, backend)
			peer, _ := newWorkflowTargetConstructionFixture(t, backend)
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			requireStandingDiagnosticPortRefusal(t, cancelled, f.store, context.Canceled)
			var retained context.Context
			if err := f.store.(interface {
				InspectSnapshot(context.Context, func(context.Context) error) error
			}).InspectSnapshot(context.Background(), func(ctx context.Context) error {
				retained = ctx
				requireStandingDiagnosticPortRefusal(t, ctx, peer.store, nil)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			requireStandingDiagnosticPortRefusal(t, retained, f.store, nil)
		})
	}
}

func requireStandingDiagnosticPortRefusal(t *testing.T, ctx context.Context, owner any, wanted error) {
	t.Helper()
	for kind := 0; kind < 3; kind++ {
		got, err := readStandingDiagnosticPort(ctx, owner, kind)
		if err == nil || !reflect.ValueOf(got).IsNil() {
			t.Fatalf("invalid/retired/foreign diagnostic kind%d owner%T: %v %v", kind, owner, got, err)
		}
		if wanted != nil && !errors.Is(err, wanted) {
			t.Fatalf("diagnostic kind%d lost exact context refusal: %v want %v", kind, err, wanted)
		}
	}
}

func readStandingDiagnosticPort(ctx context.Context, owner any, kind int) (any, error) {
	switch kind {
	case 0:
		return ReadStandingEventNameDiagnosticsForTest(ctx, owner)
	case 1:
		return ReadStandingRuntimeLogDiagnosticsForTest(ctx, owner)
	default:
		return ReadStandingDeliveryDiagnosticsForTest(ctx, owner)
	}
}
