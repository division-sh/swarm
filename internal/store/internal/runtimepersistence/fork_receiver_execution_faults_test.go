package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestForkReceiverHeaderDoneFaultExactAndRollbackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, name := range []string{"exact", "wrong_run", "wrong_entity", "inactive", "multiple"} {
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				runID, entityID := seedWorkflowProjectionFaultCut(t, fixture, ctx)
				seedWorkflowProjectionFaultCut(t, fixture, ctx)
				if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET current_state='active' WHERE run_id=$1`, runID); err != nil {
					t.Fatal(err)
				}
				requestedRun, requestedEntity := runID, entityID
				switch name {
				case "wrong_run":
					requestedRun = uuid.NewString()
				case "wrong_entity":
					requestedEntity = uuid.NewString()
				case "inactive":
					if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET current_state='queued' WHERE run_id=$1`, runID); err != nil {
						t.Fatal(err)
					}
				case "multiple":
					// The admitted schema forbids duplicate headers. A deliberately
					// unconstrained physical copy tests the writer's own cardinality fence.
					if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
						if _, err := tx.ExecContext(ctx, `ALTER TABLE flow_instances RENAME TO retained_fault_headers`); err != nil {
							return err
						}
						_, err := tx.ExecContext(ctx, `CREATE TABLE flow_instances AS SELECT * FROM retained_fault_headers`)
						return err
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
							if _, err := tx.ExecContext(ctx, `DROP TABLE flow_instances`); err != nil {
								return err
							}
							_, err := tx.ExecContext(ctx, `ALTER TABLE retained_fault_headers RENAME TO flow_instances`)
							return err
						}); err != nil {
							t.Error(err)
						}
					})
					if _, err := fixture.db.ExecContext(ctx, `INSERT INTO flow_instances
						(run_id,entity_id,instance_path,current_state,flow_template,mode,status,config,stage_defined,gates,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at)
						SELECT run_id,entity_id,'second-path',current_state,flow_template,mode,status,config,stage_defined,gates,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at
						FROM flow_instances WHERE run_id=$1`, runID); err != nil {
						t.Fatal(err)
					}
				}
				before := forkReceiverFaultSnapshot(t, ctx, fixture.store)
				probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				err = InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, fixture.store, requestedRun, requestedEntity)
				assertForkReceiverFaultTransaction(t, probe, name == "exact", err)
				restore()
				after := forkReceiverFaultSnapshot(t, ctx, fixture.store)
				if name != "exact" {
					if !reflect.DeepEqual(before, after) {
						t.Fatal("rejected header fault changed storage, including a multiple-header partial write")
					}
					return
				}
				var state string
				if err := fixture.db.QueryRowContext(ctx, `SELECT current_state FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&state); err != nil || state != "done" {
					t.Fatalf("exact header fault did not set done: %q %v", state, err)
				}
				assertForkReceiverFaultOnlyColumns(t, before, after, []forkReceiverFaultEdit{
					{table: "flow_instances", key: "entity_id", value: entityID, columns: []string{"current_state"}},
				})
			})
		}
	}
}

func TestExactDeliveryClaimAgeFaultExactAndRollbackBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, name := range []string{"exact", "wrong_run", "wrong_version", "wrong_token", "closed_attempt", "missing_attempt", "closed_second_row", "multiple_attempts", "second_statement_error"} {
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				fixture, ctx := backend.open(t), testAuthorActivityContext()
				claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
				sibling := seedDeliveryRecoveryClaim(t, fixture, ctx)
				owner := fixture.store.(deliveryFixtureStore)
				claim := claimed.Claim
				if err := prepareExactClaimFaultRefusal(t, ctx, fixture, claimed, sibling, name); err != nil {
					t.Fatal(err)
				}
				before := forkReceiverFaultSnapshot(t, ctx, fixture.store)
				probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				lower := time.Now().UTC().Add(-2 * time.Hour)
				err = ExpireExactDeliveryClaimFaultForTest(ctx, fixture.store, claim)
				upper := time.Now().UTC().Add(-2 * time.Hour)
				assertForkReceiverFaultTransaction(t, probe, name == "exact", err)
				restore()
				after := forkReceiverFaultSnapshot(t, ctx, fixture.store)
				if name != "exact" {
					if !reflect.DeepEqual(before, after) {
						t.Fatal("rejected exact claim fault left a partial obligation/attempt write")
					}
					return
				}
				var raw [5]any
				if err := fixture.db.QueryRowContext(ctx, `SELECT d.created_at,d.started_at,d.updated_at,a.started_at,a.lease_expires_at
					FROM event_deliveries d JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version
					WHERE d.delivery_id=$1`, claim.DeliveryID()).Scan(&raw[0], &raw[1], &raw[2], &raw[3], &raw[4]); err != nil {
					t.Fatal(err)
				}
				var times [5]time.Time
				for index, value := range raw {
					parsed, present, err := sqliteTimeValue(value)
					if err != nil || !present {
						t.Fatalf("physical age cut %d is not a timestamp: %v %v", index, value, err)
					}
					times[index] = parsed
				}
				created, started, updated, attemptStarted, expires := times[0], times[1], times[2], times[3], times[4]
				if !created.Equal(started) || !created.Equal(attemptStarted) || !updated.Equal(expires) || !expires.Equal(created.Add(time.Hour)) ||
					created.Before(lower.Add(-time.Microsecond)) || created.After(upper.Add(time.Microsecond)) {
					t.Fatalf("claim fault changed the fixed UTC age cuts: %v %v %v %v %v, bounds %v..%v", created, started, updated, attemptStarted, expires, lower, upper)
				}
				assertForkReceiverFaultOnlyColumns(t, before, after, []forkReceiverFaultEdit{
					{table: "event_deliveries", key: "delivery_id", value: claim.DeliveryID(), columns: []string{"created_at", "started_at", "updated_at"}},
					{table: "event_delivery_attempts", key: "delivery_id", value: claim.DeliveryID(), columns: []string{"started_at", "lease_expires_at"}},
				})
				if snapshot, err := owner.Snapshot(ctx, claim.DeliveryID()); err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != claim.Version() {
					t.Fatalf("fault changed claim generation or settled its obligation: %+v %v", snapshot, err)
				}
				if outcomes, err := owner.Outcomes(ctx, claim.DeliveryID()); err != nil || len(outcomes) != 0 {
					t.Fatalf("fault manufactured settlement: %+v %v", outcomes, err)
				}
			})
		}
	}
}

func prepareExactClaimFaultRefusal(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, claimed, sibling deliverylifecycle.ClaimedObligation, name string) error {
	t.Helper()
	if name == "exact" {
		return nil
	}
	if name == "closed_attempt" {
		_, err := fixture.store.(deliveryFixtureStore).SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, deliverylifecycle.NotApplicableHandlerRuleSelection())
		return err
	}
	switch name {
	case "missing_attempt", "closed_second_row", "multiple_attempts", "second_statement_error":
		return prepareExactClaimSecondStatementFault(t, ctx, fixture, claimed.Claim, name)
	}
	foreignEventID := ""
	if name == "wrong_run" {
		// An admitted foreign event/run pair keeps native relational constraints
		// intact while testing the original claim's run fence.
		foreignEventID = uuid.NewString()
		if err := commitSemanticParentFixture(ctx, fixture.store, sibling.Claim.RunID(), foreignEventID, time.Now().UTC()); err != nil {
			return err
		}
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		switch name {
		case "wrong_run":
			_, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET run_id=$1,event_id=$2 WHERE delivery_id=$3`, sibling.Claim.RunID(), foreignEventID, claimed.Claim.DeliveryID())
			return err
		case "wrong_version":
			if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET claim_version=claim_version+1,current_attempt_version=current_attempt_version+1 WHERE delivery_id=$1`, claimed.Claim.DeliveryID()); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET claim_version=claim_version+1 WHERE delivery_id=$1`, claimed.Claim.DeliveryID())
			return err
		case "wrong_token":
			_, err := tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET claim_token=$1 WHERE delivery_id=$2`, uuid.NewString(), claimed.Claim.DeliveryID())
			return err
		}
		return errors.New("unknown exact-claim fault control")
	})
}

func prepareExactClaimSecondStatementFault(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, claim deliverylifecycle.Claim, name string) error {
	t.Helper()
	// Retain the constraint-backed ledger intact. This isolated physical copy
	// challenges the fault writer independently of normal schema admission.
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE event_delivery_attempts RENAME TO retained_fault_attempts`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TABLE event_delivery_attempts AS SELECT * FROM retained_fault_attempts`)
		return err
	}); err != nil {
		return err
	}
	t.Cleanup(func() {
		if err := runUnrevisionedEventFixtureTransactionForTest(context.WithoutCancel(ctx), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `DROP TABLE event_delivery_attempts`); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `ALTER TABLE retained_fault_attempts RENAME TO event_delivery_attempts`)
			return err
		}); err != nil {
			t.Error(err)
		}
	})
	return runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		switch name {
		case "missing_attempt":
			_, err = tx.ExecContext(ctx, `DELETE FROM event_delivery_attempts WHERE delivery_id=$1`, claim.DeliveryID())
		case "closed_second_row":
			_, err = tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET open_marker=FALSE WHERE delivery_id=$1`, claim.DeliveryID())
		case "multiple_attempts":
			_, err = tx.ExecContext(ctx, `INSERT INTO event_delivery_attempts SELECT * FROM retained_fault_attempts WHERE delivery_id=$1`, claim.DeliveryID())
		case "second_statement_error":
			_, err = tx.ExecContext(ctx, `ALTER TABLE event_delivery_attempts RENAME COLUMN lease_expires_at TO unavailable_expiry`)
		}
		return err
	})
}

func TestForkReceiverFaultsRefuseInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			runID, entityID := seedWorkflowProjectionFaultCut(t, fixture, ctx)
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			before := forkReceiverFaultSnapshot(t, ctx, fixture.store)
			for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
				if err := InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, selected, runID, entityID); err == nil {
					t.Fatal("header fault accepted a raw or uninitialized owner")
				}
				if err := ExpireExactDeliveryClaimFaultForTest(ctx, selected, claimed.Claim); err == nil {
					t.Fatal("claim fault accepted a raw or uninitialized owner")
				}
			}
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), " " + runID, strings.ToUpper("abcdef01-1111-4111-8111-111111111111")} {
				if err := InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, fixture.store, invalid, entityID); err == nil {
					t.Fatal("header fault accepted a noncanonical run")
				}
				if err := InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, fixture.store, runID, invalid); err == nil {
					t.Fatal("header fault accepted a noncanonical entity")
				}
			}
			if err := ExpireExactDeliveryClaimFaultForTest(ctx, fixture.store, deliverylifecycle.Claim{}); err == nil {
				t.Fatal("claim fault accepted an unminted claim")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := InstallActiveForkReceiverHeaderDoneFaultForTest(cancelled, fixture.store, runID, entityID); !errors.Is(err, context.Canceled) {
				t.Fatalf("header fault lost original cancellation: %v", err)
			}
			if err := ExpireExactDeliveryClaimFaultForTest(cancelled, fixture.store, claimed.Claim); !errors.Is(err, context.Canceled) {
				t.Fatalf("claim fault lost original cancellation: %v", err)
			}
			if after := forkReceiverFaultSnapshot(t, ctx, fixture.store); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid or cancelled faults mutated storage")
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := InstallActiveForkReceiverHeaderDoneFaultForTest(ctx, fixture.store, runID, entityID); err == nil {
				t.Fatal("header fault accepted a closed owner")
			}
			if err := ExpireExactDeliveryClaimFaultForTest(ctx, fixture.store, claimed.Claim); err == nil {
				t.Fatal("claim fault accepted a closed owner")
			}
		})
	}
}

func forkReceiverFaultSnapshot(t *testing.T, ctx context.Context, selected any) map[string]SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertForkReceiverFaultTransaction(t *testing.T, probe *transactiontest.Collector, committed bool, err error) {
	t.Helper()
	counts := probe.Snapshot()
	if counts.Total.Begun != 1 || counts.Active != 0 || counts.Total.ReadCommits != 0 || counts.Total.CleanupFailures != 0 {
		t.Fatalf("fault escaped original joined writer: %+v", counts)
	}
	if committed {
		if err != nil || counts.Total.WriteCommits != 1 || counts.Total.Failed != 0 {
			t.Fatalf("exact fault did not commit once: %+v %v", counts, err)
		}
	} else if err == nil || counts.Total.WriteCommits != 0 || counts.Total.Failed != 1 || counts.Total.RollbackAttempts != 1 {
		t.Fatalf("rejected fault did not roll back once: %+v %v", counts, err)
	}
}

type forkReceiverFaultEdit struct {
	table, key, value string
	columns           []string
}

// Whole-store equality permits only the named cells on one exact physical row.
func assertForkReceiverFaultOnlyColumns(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, edits []forkReceiverFaultEdit) {
	t.Helper()
	for _, edit := range edits {
		old, new := before[edit.table], after[edit.table]
		changed := 0
		for i, raw := range old.Rows {
			var values []any
			if err := json.Unmarshal([]byte(raw), &values); err != nil {
				t.Fatal(err)
			}
			key := sortColumnIndex(t, old.Columns, edit.key)
			if values[key] != edit.value {
				continue
			}
			changed++
			matches := 0
			for _, newRaw := range new.Rows {
				var newValues []any
				if err := json.Unmarshal([]byte(newRaw), &newValues); err != nil {
					t.Fatal(err)
				}
				if newValues[key] != edit.value {
					continue
				}
				matches++
				for _, column := range edit.columns {
					index := sortColumnIndex(t, old.Columns, column)
					if reflect.DeepEqual(values[index], newValues[index]) {
						t.Fatalf("fault did not change required cell %s.%s", edit.table, column)
					}
					values[index] = newValues[index]
				}
			}
			if matches != 1 {
				t.Fatalf("fault result lacks one exact %s row: %d", edit.table, matches)
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			old.Rows[i] = string(encoded)
		}
		if changed != 1 {
			t.Fatalf("fault fixture lacks one exact %s row: %d", edit.table, changed)
		}
		sort.Strings(old.Rows)
		before[edit.table] = old
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("fault changed other rows, business fields, revisions, clocks, companions or history")
	}
}

func sortColumnIndex(t *testing.T, columns []string, name string) int {
	t.Helper()
	for index, column := range columns {
		if column == name {
			return index
		}
	}
	t.Fatalf("physical fault evidence lacks column %s", name)
	return -1
}
