package runforkrevision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type factInsertCall struct {
	query string
	args  []any
}

func TestRevisionMetadataSubmittedJSONBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("postgres_%v/fail_%v", postgres, fail), func(t *testing.T) {
				var slot transactiontest.Slot
				collector, restore, err := slot.Install(transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				a := slot.Begin(false, false)
				a.Begun()
				ctx := transactiontest.WithAttempt(context.Background(), a)
				facts := make([]revisionFactInsert, 257)
				for index := range facts {
					family := FamilyEvents
					if index%2 == 0 {
						family = FamilyEntityMetadata
					}
					facts[index] = revisionFactInsert{family, fmt.Sprint(index), []byte(`{ "exact" : "submitted" }`), true}
				}
				refusal := errors.New("metadata write refused")
				recorder := &factInsertRecorder{err: refusal}
				if fail {
					recorder.failAt = 2
				}
				writeErr := insertRevisionFacts(ctx, recorder, postgres, "run", 9, facts)
				if fail {
					if !errors.Is(writeErr, refusal) {
						t.Fatalf("metadata lost refusal: %v", writeErr)
					}
					a.RollbackAttempted()
				} else {
					if writeErr != nil {
						t.Fatal(writeErr)
					}
					a.BeforeCommit()
					a.Committed()
				}
				a.Finish(writeErr)
				want := transactiontest.CopyCounts{}
				for index, call := range recorder.calls {
					var bytes, copies uint64
					for row := 0; row < len(call.args); row += 6 {
						if call.args[row+2] != FamilyEntityMetadata {
							continue
						}
						copies++
						// Inspect the actual dialect-specific argument, not JSONB
						// readback or an inferred fact/row multiplication.
						switch body := call.args[row+4].(type) {
						case string:
							bytes += uint64(len(body))
						case []byte:
							bytes += uint64(len(body))
						default:
							t.Fatalf("unexpected submitted JSON argument %T", body)
						}
					}
					want.Calls++
					want.Copies += copies
					want.SubmittedBytes += bytes
					if fail && index+1 == recorder.failAt {
						want.FailedCalls++
						want.FailedBytes += bytes
					} else {
						want.SucceededCalls++
						want.SucceededBytes += bytes
					}
				}
				if fail {
					want.UncommittedBytes, want.RollbackAttemptedBytes = want.SucceededBytes, want.SucceededBytes
				} else {
					want.CommittedBytes = want.SucceededBytes
				}
				got := collector.Snapshot().Total.JSONCopies
				if got.EntityMetadata != want || got.WorkflowHeader != (transactiontest.CopyCounts{}) {
					t.Fatalf("metadata receipt=%+v want=%+v", got, want)
				}
			})
		}
	}
}

type factInsertRecorder struct {
	revisionSQL
	calls  []factInsertCall
	failAt int
	err    error
}

func (r *factInsertRecorder) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.calls = append(r.calls, factInsertCall{query, append([]any(nil), args...)})
	if len(r.calls) == r.failAt {
		return nil, r.err
	}
	return nil, nil
}

func TestRevisionFactInsertBoundedBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, count := range []int{0, 1, 127, 128, 129, 257} {
			t.Run(fmt.Sprintf("postgres_%v/rows_%d", postgres, count), func(t *testing.T) {
				facts := make([]revisionFactInsert, count)
				for i := range facts {
					facts[i] = revisionFactInsert{FamilyEntityMetadata, fmt.Sprintf("key-%03d');--", i), []byte(`{"name":"quoted ' $1"}`), i%2 == 0}
					if !facts[i].present {
						facts[i].fact = []byte(`{}`)
					}
				}
				recorder := &factInsertRecorder{}
				var adapter ledgerAdapter = &sqliteAdapter{tx: recorder}
				if postgres {
					adapter = &postgresAdapter{tx: recorder}
				}
				if err := adapter.insertFacts(context.Background(), "run", 7, facts); err != nil {
					t.Fatal(err)
				}
				if len(recorder.calls) != (count+127)/128 {
					t.Fatalf("physical inserts=%d for %d facts", len(recorder.calls), count)
				}
				for chunk, call := range recorder.calls {
					rows := min(128, count-chunk*128)
					if len(call.args) != 6*rows || len(call.args) > 768 {
						t.Fatalf("chunk %d parameters=%d", chunk, len(call.args))
					}
					values := make([]string, rows)
					for i := 0; i < rows; i++ {
						fact := facts[chunk*128+i]
						var body any = string(fact.fact)
						cast := ""
						if postgres {
							body, cast = fact.fact, "::jsonb"
						}
						want := []any{"run", int64(7), fact.family, fact.key, body, fact.present}
						if !reflect.DeepEqual(call.args[6*i:6*i+6], want) {
							t.Fatalf("changed physical row %d: %#v", chunk*128+i, call.args[6*i:6*i+6])
						}
						values[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d%s,$%d)", 6*i+1, 6*i+2, 6*i+3, 6*i+4, 6*i+5, cast, 6*i+6)
					}
					want := `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ` + strings.Join(values, ",")
					if call.query != want {
						t.Fatalf("chunk %d changed statement or placeholders: %s", chunk, call.query)
					}
				}
			})
		}
	}
}

func TestRevisionFactInsertFailureStopsAndWrapsBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, count := range []int{1, 257} {
			t.Run(fmt.Sprintf("postgres_%v/rows_%d", postgres, count), func(t *testing.T) {
				failure := errors.New("native constraint refusal")
				recorder := &factInsertRecorder{failAt: 1, err: failure}
				if count > 1 {
					recorder.failAt = 2
				}
				facts := make([]revisionFactInsert, count)
				for i := range facts {
					facts[i] = revisionFactInsert{FamilyEntityMetadata, fmt.Sprintf("key-%03d", i), []byte(`{}`), false}
				}
				err := insertRevisionFacts(context.Background(), recorder, postgres, "run", 9, facts)
				if !errors.Is(err, failure) || len(recorder.calls) != recorder.failAt {
					t.Fatalf("failure=%v calls=%d", err, len(recorder.calls))
				}
				if count == 1 {
					want := fmt.Sprintf("record run fork %s fact key-000 at revision 9: %s", FamilyEntityMetadata, failure)
					if err.Error() != want {
						t.Fatalf("singleton error changed: %v", err)
					}
				} else if !strings.Contains(err.Error(), "key-128") || !strings.Contains(err.Error(), "key-255") || strings.Contains(err.Error(), "key-256") {
					t.Fatalf("failed batch coordinates lost or later chunk executed: %v", err)
				}
			})
		}
	}
}
