package delivery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type evidenceCleanupConnector struct {
	scanFailure bool
	closeErr    error
}

func (c evidenceCleanupConnector) Driver() driver.Driver { return evidenceCleanupDriver{} }
func (c evidenceCleanupConnector) Connect(context.Context) (driver.Conn, error) {
	return evidenceCleanupConn{c}, nil
}

type evidenceCleanupDriver struct{}

func (evidenceCleanupDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector only")
}

type evidenceCleanupConn struct{ config evidenceCleanupConnector }

func (evidenceCleanupConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("no prepared statements")
}
func (evidenceCleanupConn) Close() error              { return nil }
func (evidenceCleanupConn) Begin() (driver.Tx, error) { return evidenceCleanupTx{}, nil }

type evidenceCleanupTx struct{}

func (evidenceCleanupTx) Commit() error   { return nil }
func (evidenceCleanupTx) Rollback() error { return nil }
func (c evidenceCleanupConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "FROM event_deliveries d") {
		retries := driver.Value(int64(0))
		if c.config.scanFailure {
			retries = "not an integer"
		}
		return &evidenceCleanupRows{values: []driver.Value{uuid.NewString(), uuid.NewString(), uuid.NewString(), "node", "worker", "in_progress", retries, int64(1), nil, int64(0)}, closeErr: c.config.closeErr}, nil
	}
	if strings.Contains(query, "FROM event_delivery_attempts") {
		var closure driver.Value
		if strings.Contains(query, "COALESCE(closure_kind,'')") {
			closure = ""
		}
		return &evidenceCleanupRows{values: []driver.Value{int64(1), closure, ""}}, nil
	}
	return &evidenceCleanupRows{values: []driver.Value{int64(0)}}, nil
}

type evidenceCleanupRows struct {
	values   []driver.Value
	closeErr error
	read     bool
}

func (r *evidenceCleanupRows) Columns() []string { return make([]string, len(r.values)) }
func (r *evidenceCleanupRows) Close() error      { return r.closeErr }
func (r *evidenceCleanupRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}

func TestIssue2564DeliveryEvidenceJoinsCleanupAndDiscardsPartial(t *testing.T) {
	closeErr := errors.New("evidence row close failed")
	for _, scanFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "after-row", true: "scan-failure"}[scanFailure], func(t *testing.T) {
			db := sql.OpenDB(evidenceCleanupConnector{scanFailure: scanFailure, closeErr: closeErr})
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Rollback(); err != nil {
					t.Error(err)
				}
			}()
			got, err := observeDeliveryEventEvidence(context.Background(), tx, uuid.NewString())
			if !errors.Is(err, closeErr) || !reflect.DeepEqual(got, DeliveryEventEvidence{}) {
				t.Fatalf("partial evidence/cleanup loss: %+v err=%v", got, err)
			}
		})
	}
}

func TestIssue2564DeliveryEvidenceOpenAttemptIsNotSettlement(t *testing.T) {
	db := sql.OpenDB(evidenceCleanupConnector{})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Error(err)
		}
	}()
	got, err := observeDeliveryEventEvidence(context.Background(), tx, uuid.NewString())
	if err != nil || len(got.Deliveries) != 1 || len(got.Deliveries[0].Attempts) != 1 {
		t.Fatalf("open attempt omitted: %+v err=%v", got, err)
	}
	if row := got.Deliveries[0].Attempts[0]; row.ClosureKind != "" || row.Outcome != "" {
		t.Fatalf("open nullable attempt became settlement: %+v", row)
	}
}
