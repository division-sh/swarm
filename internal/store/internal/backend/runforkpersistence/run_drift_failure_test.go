package runforkpersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
)

// Driver faults occur at the shared snapshot query, after run and coordinate
// admission. This exercises cause transport, not native storage semantics.
type runDriftFailureConnector struct {
	location, stage string
	cause           error
}

func (c runDriftFailureConnector) Driver() driver.Driver { return runDriftFailureDriver{} }
func (c runDriftFailureConnector) Connect(context.Context) (driver.Conn, error) {
	return runDriftFailureConn{c}, nil
}

type runDriftFailureDriver struct{}

func (runDriftFailureDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type runDriftFailureConn struct{ runDriftFailureConnector }

func (runDriftFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (runDriftFailureConn) Close() error              { return nil }
func (runDriftFailureConn) Begin() (driver.Tx, error) { return runDriftFailureTx{}, nil }
func (runDriftFailureConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return runDriftFailureTx{}, nil
}

type runDriftFailureTx struct{}

func (runDriftFailureTx) Commit() error   { return nil }
func (runDriftFailureTx) Rollback() error { return nil }

func (c runDriftFailureConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	matches := map[string]bool{
		"snapshot":    strings.Contains(query, "WITH bounded AS"),
		"physical":    strings.Contains(query, "CAST(mutation_id AS TEXT),CAST(entity_id AS TEXT)"),
		"coordinates": strings.Contains(query, "CAST(f.run_id AS TEXT)"),
		"state":       strings.Contains(query, "FROM entity_state"),
	}[c.location]
	if matches {
		switch c.stage {
		case "query":
			return nil, c.cause
		case "iteration":
			return &runDriftFailureRows{nextErr: c.cause}, nil
		case "close":
			return &runDriftFailureRows{closeErr: c.cause}, nil
		case "scan":
			return &runDriftFailureRows{values: []driver.Value{"run", "entity_mutations", "mutation", "not-an-integer", int64(1), []byte(`{}`)}}, nil
		default:
			rows := &runDriftFailureRows{values: []driver.Value{args[0].Value, "entity_mutations", "00000000-0000-0000-0000-000000000002", int64(1), int64(1), []byte(`{}`)}}
			switch c.location {
			case "physical":
				rows.values = []driver.Value{"00000000-0000-0000-0000-000000000002", "entity", "bookkeeping", "removed", "", time.Now().UTC()}
			case "coordinates":
				rows.values = []driver.Value{"foreign-run", "00000000-0000-0000-0000-000000000002", int64(1), true, int64(1)}
			case "state":
				rows.values = []driver.Value{"entity", "waiting", "[]", "{}", "{}", "{}"}
			}
			if c.stage == "admission_close" {
				rows.closeErr = c.cause
			}
			return rows, nil
		}
	}
	if strings.Contains(query, "FROM runs") {
		return &runDriftFailureRows{values: []driver.Value{args[0].Value}}, nil
	}
	if strings.Contains(query, "SELECT last_revision") {
		return &runDriftFailureRows{values: []driver.Value{int64(1)}}, nil
	}
	return &runDriftFailureRows{}, nil
}

type runDriftFailureRows struct {
	values            []driver.Value
	read              bool
	nextErr, closeErr error
}

func (r *runDriftFailureRows) Columns() []string { return make([]string, len(r.values)) }
func (r *runDriftFailureRows) Close() error      { return r.closeErr }
func (r *runDriftFailureRows) Next(dest []driver.Value) error {
	if r.nextErr != nil {
		return r.nextErr
	}
	if r.read || len(r.values) == 0 {
		return io.EOF
	}
	r.read = true
	copy(dest, r.values)
	return nil
}

func TestVerifyRunSnapshotFailuresRetainCause(t *testing.T) {
	for _, location := range []string{"snapshot", "physical", "coordinates", "state"} {
		stages := []string{"query", "iteration", "close", "admission_close"}
		if location == "snapshot" {
			stages = append(stages, "scan", "admission")
		}
		for _, stage := range stages {
			for _, cause := range []error{errors.New("driver transport failure"), context.DeadlineExceeded, context.Canceled} {
				t.Run(location+"/"+stage+"/"+cause.Error(), func(t *testing.T) {
					db := sql.OpenDB(runDriftFailureConnector{location: location, stage: stage, cause: cause})
					t.Cleanup(func() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					})
					const run = "00000000-0000-0000-0000-000000000001"
					report, err := inspectRunDriftTest(t, db, run)
					var history *mutationlog.HistoryError
					if stage == "admission" || stage == "admission_close" {
						if !errors.As(err, &history) || history.RunID != run {
							t.Fatalf("admission error lost coordinates: %+v %v", report, err)
						}
						if location != "state" && history.MutationID != "00000000-0000-0000-0000-000000000002" {
							t.Fatal("mutation coordinate lost")
						}
					} else if err == nil || errors.As(err, &history) {
						t.Fatalf("operational failure became history or clean: %+v %v", report, err)
					}
					if stage != "admission" && stage != "scan" && !errors.Is(err, cause) {
						t.Fatalf("driver cause erased: want=%v got=%v", cause, err)
					}
					if report.EntitiesChecked != 0 || len(report.Rows) != 0 {
						t.Fatalf("failed inspection returned partial verdict: %+v", report)
					}
				})
			}
		}
	}
}
