package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestServedDeliveryStatusCountPreservesExactPhysicalPredicatesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			first, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			if _, err := fixture.store.SettleSuccess(ctx, first.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			cases := []struct {
				name, event, kind, recipient string
				statuses                     []string
				want                         int
			}{
				{"terminal_history", first.Snapshot.EventID, "agent", "agent-a", []string{"delivered"}, 1},
				{"sibling_pending", sibling.Snapshot.EventID, "agent", "agent-a", []string{"in_progress"}, 1},
				{"no_event", uuid.NewString(), "agent", "agent-a", nil, 0},
				{"omitted_type", first.Snapshot.EventID, "", "agent-a", nil, 1},
				{"blank_type", first.Snapshot.EventID, " \t", "agent-a", nil, 1},
				{"other_type", first.Snapshot.EventID, "node", "agent-a", nil, 0},
				{"lexical_type", first.Snapshot.EventID, " agent ", "agent-a", nil, 0},
				{"lexical_recipient", first.Snapshot.EventID, "agent", " agent-a ", nil, 0},
				{"empty_recipient", first.Snapshot.EventID, "agent", "", nil, 0},
				{"blank_status", first.Snapshot.EventID, "agent", "agent-a", []string{" \t", "delivered"}, 1},
				{"repeated_status", first.Snapshot.EventID, "agent", "agent-a", []string{"delivered", "delivered"}, 1},
				{"contradictory_status", first.Snapshot.EventID, "agent", "agent-a", []string{"delivered", "in_progress"}, 0},
				{"lexical_status", first.Snapshot.EventID, "agent", "agent-a", []string{" delivered "}, 0},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					want, err := servedDeliveryStatusCountOracle(ctx, fixture.db, backend.name, tc.event, tc.kind, tc.recipient, tc.statuses...)
					if err != nil || want != tc.want {
						t.Fatalf("fixture/oracle=%d want=%d err=%v", want, tc.want, err)
					}
					got, err := ReadServedDeliveryStatusCountForTest(ctx, fixture.store, tc.event, tc.kind, tc.recipient, tc.statuses...)
					if err != nil || got != want {
						t.Fatalf("count=%d want=%d err=%v", got, want, err)
					}
				})
			}
			if counts := probe.Snapshot(); counts.Total.Begun != uint64(len(cases)) || counts.Total.ReadCommits != uint64(len(cases)) || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("read escaped original coordinator: %+v", counts)
			}
			for _, lexical := range []string{strings.ToUpper(first.Snapshot.EventID), "bad-event", ""} {
				want, wantErr := servedDeliveryStatusCountOracle(ctx, fixture.db, backend.name, lexical, "agent", "agent-a")
				got, err := ReadServedDeliveryStatusCountForTest(ctx, fixture.store, lexical, "agent", "agent-a")
				if got != want || (err == nil) != (wantErr == nil) {
					t.Fatalf("native lookup %q changed: %d/%v want %d/%v", lexical, got, err, want, wantErr)
				}
				if err != nil && err.Error() != wantErr.Error() {
					t.Fatalf("native lookup error changed: %v want %v", err, wantErr)
				}
			}
		})
	}
}

func TestServedDeliveryStatusCountRefusesRawCancelledClosedAndUnavailableOwnersBothStores(t *testing.T) {
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadServedDeliveryStatusCountForTest(context.Background(), owner, uuid.NewString(), "agent", "agent-a"); got != 0 || err == nil {
			t.Fatalf("invalid owner %T returned evidence: %d %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"cancelled", "closed", "unavailable"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				first := seedDeliveryRecoveryClaim(t, fixture, ctx)
				switch cut {
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "closed":
					if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "unavailable":
					if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `ALTER TABLE event_deliveries RENAME TO unavailable_status_count_deliveries`)
						return err
					}); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_status_count_deliveries RENAME TO event_deliveries`)
							return err
						}); err != nil {
							t.Error(err)
						}
					}()
				}
				got, err := ReadServedDeliveryStatusCountForTest(ctx, fixture.store, first.Snapshot.EventID, "agent", "agent-a")
				if err == nil || got != 0 || cut == "cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("failed read returned evidence: %d %v", got, err)
				}
			})
		}
	}
}

// Frozen pre-migration oracle; independent of the new reader and its builder.
func servedDeliveryStatusCountOracle(ctx context.Context, db *sql.DB, backend, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	where, args := []string{}, []any{}
	switch backend {
	case "postgres":
		where, args = append(where, "event_id = $1::uuid"), append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where, args = append(where, fmt.Sprintf("subscriber_type = $%d", len(args)+1)), append(args, subscriberType)
		}
		where, args = append(where, fmt.Sprintf("subscriber_id = $%d", len(args)+1)), append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where, args = append(where, fmt.Sprintf("status = $%d", len(args)+1)), append(args, status)
		}
	case "sqlite":
		where, args = append(where, "event_id = ?"), append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where, args = append(where, "subscriber_type = ?"), append(args, subscriberType)
		}
		where, args = append(where, "subscriber_id = ?"), append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where, args = append(where, "status = ?"), append(args, status)
		}
	default:
		return 0, fmt.Errorf("unknown oracle backend %q", backend)
	}
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_deliveries WHERE "+strings.Join(where, " AND "), args...).Scan(&count)
	return count, err
}
