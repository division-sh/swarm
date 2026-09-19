package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

// Test-only goroutine identity checks that the native transaction stays on the
// caller; the production decoder has no queryer or SQL callback to delegate.
func batchDecodeCallerID() uint64 {
	var raw [64]byte
	runtime.Stack(raw[:], false)
	var id uint64
	if _, err := fmt.Sscanf(string(raw[:]), "goroutine %d ", &id); err != nil {
		panic(err)
	}
	return id
}

type batchDecodeCallerQueryer struct {
	eventReadQueryer
	caller    uint64
	beforeRow func()
	mu        sync.Mutex
	calls     []string
	foreign   bool
}

func (q *batchDecodeCallerQueryer) record(call string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.foreign = q.foreign || batchDecodeCallerID() != q.caller
	q.calls = append(q.calls, call)
}

func (q *batchDecodeCallerQueryer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.record("batch")
	return q.eventReadQueryer.QueryContext(ctx, query, args...)
}

func (q *batchDecodeCallerQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.record(fmt.Sprintf("owner:%v", args[0]))
	if q.beforeRow != nil {
		q.beforeRow()
	}
	return q.eventReadQueryer.QueryRowContext(ctx, query, args...)
}

func TestFanOutBatchDecodeC1FirstErrorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture, ctx, group, _ := prepareP16CommittedGroup(t, owner.(selectedFanOutLifecycleOwner), db, backend)
			if err := group.Close(ctx); err != nil {
				t.Fatal(err)
			}
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			var ids []string
			rows, err := db.QueryContext(ctx, `SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 ORDER BY ordinal`, fixture.runID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if len(ids) != 2 {
				t.Fatalf("C1 requires complete two-member node-produced group, got %d", len(ids))
			}
			for i := 2; i < 4; i++ {
				event := fanOutBarrierChildEvent(t, fixture, i, base.Add(time.Duration(i)*time.Microsecond))
				if err := commitSemanticEventFixtureWithRoutes(ctx, owner, event, nil); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, event.ID())
			}
			original, err := m29LoadBatch(ctx, db, postgres, ids)
			if err != nil {
				t.Fatal(err)
			}
			ownerBad, err := eventrecord.FromAdmitted(original[0].Event, original[0].Settlement)
			if err != nil {
				t.Fatal(err)
			}
			declaration, err := identity.AdmitDeclarationIdentity(fixture.flowPath, "fan_out", fixture.semanticPath)
			if err != nil {
				t.Fatal(err)
			}
			origin, err := events.NewInheritedFanOutOrigin(fixture.runID, uuid.NewString(), fixture.eventID,
				uuid.NewString(), declaration, fixture.bundleHash, "sha256:"+strings.Repeat("3", 64), 0)
			if err != nil {
				t.Fatal(err)
			}
			ownerBad.Class, ownerBad.SourceEventID = events.EventAdmissionInheritedFanOut, ""
			ownerBad.InheritedFanOutOrigin, err = json.Marshal(origin)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := ownerBad.DecodeWithSettlement(); err != nil {
				t.Fatalf("C1 inherited mismatch must reach the SQL owner: %v", err)
			}
			codecBad, err := eventrecord.FromAdmitted(original[1].Event, original[1].Settlement)
			if err != nil {
				t.Fatal(err)
			}
			codecBad.Payload = []byte(`{"corrupt":null}`)
			missing := uuid.NewString()
			for _, tc := range []struct {
				name      string
				requested []string
				wantID    string
				ownerRead bool
				cancel    bool
			}{
				{"missing_before_corrupt_owner", []string{missing, ids[1], ids[0], ids[2]}, missing, false, false},
				{"corrupt_before_missing_owner", []string{ids[1], missing, ids[0], ids[2]}, ids[1], false, false},
				{"owner_before_corrupt_missing", []string{ids[0], ids[1], missing, ids[2]}, ids[0], true, false},
				{"valid_prefix_then_owner", []string{ids[2], ids[0], ids[1], missing}, ids[0], true, false},
				{"valid_prefix_then_corrupt", []string{ids[2], ids[1], ids[0], missing}, ids[1], false, false},
				{"valid_prefix_then_missing", []string{ids[2], missing, ids[0], ids[1]}, missing, false, false},
				{"cancellation_at_owner_before_corrupt", []string{ids[2], ids[0], ids[1], missing}, ids[0], true, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					writeJointSourceReadRecord(t, ctx, tx, postgres, ownerBad)
					writeJointSourceReadRecord(t, ctx, tx, postgres, codecBad)
					callCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					q := &batchDecodeCallerQueryer{eventReadQueryer: tx, caller: batchDecodeCallerID()}
					if tc.cancel {
						q.beforeRow = cancel
					}
					got, err := m29LoadBatch(callCtx, q, postgres, tc.requested)
					if got != nil {
						t.Fatal("C1 exposed admitted prefix on failure")
					}
					if tc.wantID == missing {
						var absent *eventrecord.MissingError
						if !errors.As(err, &absent) || absent.EventID != missing {
							t.Fatalf("C1 missing must precede later codec/owner errors: %v", err)
						}
					} else {
						assertJointSourceReadCorrupt(t, tc.wantID, err)
					}
					if tc.cancel && !errors.Is(err, context.Canceled) {
						t.Fatalf("C1 owner cancellation lost original cause: %v", err)
					}
					wantCalls := []string{"batch"}
					if tc.ownerRead {
						wantCalls = append(wantCalls, "owner:"+ids[0])
					}
					q.mu.Lock()
					calls, foreign := append([]string(nil), q.calls...), q.foreign
					q.mu.Unlock()
					if foreign || !reflect.DeepEqual(calls, wantCalls) {
						t.Fatalf("C1 SQL moved off caller or changed first-error order: calls=%v want=%v foreign=%v", calls, wantCalls, foreign)
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					fresh, err := m29LoadBatch(ctx, db, postgres, ids)
					if err != nil || len(fresh) != len(ids) {
						t.Fatalf("C1 later read reused failed/canceled admission: %v", err)
					}
					for i := range fresh {
						if fresh[i].Event.ID() != ids[i] || fresh[i].Event.Event().AdmissionClass() != original[i].Event.Event().AdmissionClass() {
							t.Fatalf("C1 fresh record%d changed order or retained hostile class", i)
						}
					}
				})
			}
		})
	}
}
