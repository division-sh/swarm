package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

// The currentness fixture supplies exact selected-store activation/install
// evidence. These are cleanup-owner transaction proofs, not public producers.
func proveNativeResetTransaction(t *testing.T, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, admission channelnative.Admission,
	setting channelnative.Setting, uncertain, postgres bool, now time.Time) {
	t.Helper()
	req := destructivereset.CleanupRequest{ActorTokenID: "native-reset-proof", RequestedAt: now,
		Result: destructivereset.Result{OperationName: destructivereset.DefaultOperationName,
			PlannedAt: now, Plan: destructivereset.Plan{CleanupRunSetKnown: true}},
		Quiescence: destructivereset.QuiescenceResult{OperationName: destructivereset.DefaultOperationName, AppliedAt: now}}
	apply := func(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest) error {
		if postgres {
			_, err := storeadmin.ApplyDestructiveResetCleanupInRetainedTransaction(selected.(*PostgresStore).destructiveResetPostgresOwner, ctx, tx, req)
			return err
		}
		_, err := storeadmin.ApplyDestructiveResetSQLiteCleanupInRetainedTransaction(ctx, tx, req)
		return err
	}
	read := func(ctx context.Context, tx *sql.Tx) ([5]string, error) {
		var row [5]string
		err := tx.QueryRowContext(ctx, `SELECT s.setting_id,s.install_operation_id,COALESCE(s.readback_hash,''),s.state,c.state
			FROM channel_native_settings s JOIN channel_native_setting_consumers c ON c.setting_id=s.setting_id
			WHERE c.activation_id=$1`, admission.ActivationID).Scan(&row[0], &row[1], &row[2], &row[3], &row[4])
		return row, err
	}
	var before [5]string
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		var err error
		before, err = read(ctx, tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := before
	want[3], want[4] = "retired", "retired"
	if uncertain {
		want[3] = "uncertain"
	}
	check := func(expected [5]string, activationCount int) {
		t.Helper()
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			row, err := read(ctx, tx)
			if err != nil || row != expected {
				return fmt.Errorf("native reset evidence=%v want=%v: %w", row, expected, err)
			}
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM connected_channel_activations WHERE activation_id=$1`, admission.ActivationID).Scan(&count); err != nil {
				return err
			}
			if count != activationCount {
				return fmt.Errorf("native reset activation count=%d want=%d", count, activationCount)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	dry := req
	dry.Result.DryRun = true
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, dry) }); err != nil {
		t.Fatal(err)
	}
	check(before, 1)
	rollback := errors.New("native cleanup commit interrupted")
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		if err := apply(ctx, tx, req); err != nil {
			return err
		}
		if row, err := read(ctx, tx); err != nil || row != want {
			return fmt.Errorf("cleanup did not atomically retire consumers: %v %w", row, err)
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("cleanup interruption=%v", err)
	}
	check(before, 1)
	for attempt := 0; attempt < 2; attempt++ {
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, req) }); err != nil {
			t.Fatal(err)
		}
		check(want, 0)
	}
	if _, err := selected.(channelnative.Store).AttachNativeInboxSetting(context.Background(), admission); err == nil {
		t.Fatal("preserved activation coordinate granted a new current consumer")
	}
	if before[0] != setting.SettingID || before[1] != setting.InstallOperationID {
		t.Fatal("reset proof did not retain its original install identity")
	}
}
