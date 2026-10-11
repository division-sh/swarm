package sessionpersistence

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestPrivateSessionInitializationRetainsFullDurability(t *testing.T) {
	ctx := context.Background()
	owner, err := OpenSDKFixture(ctx, filepath.Join(t.TempDir(), "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	var mode, version string
	var synchronous, foreignKeys int
	if err := owner.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if err := owner.db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if err := owner.db.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if err := owner.db.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("private SQLite version=%s mode=%s synchronous=%d foreign_keys=%d", version, mode, synchronous, foreignKeys)
	if mode != "wal" || synchronous != 2 || foreignKeys != 1 || owner.db.Stats().MaxOpenConnections != 1 {
		t.Fatal("private initialization did not retain one serialized, FULL-durable WAL connection")
	}
}

func TestCaptureSchemaInitializationRollsBackRefusedSecondTable(t *testing.T) {
	ctx := context.Background()
	owner, err := OpenCaptureFixture(filepath.Join(t.TempDir(), "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := owner.db.ExecContext(ctx, `CREATE VIEW whatsapp_callback_failures AS SELECT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Captures(ctx, "334d3c37-e7aa-4d12-aa88-4a72386b6d6a"); err == nil {
		t.Fatal("contradictory callback schema was accepted")
	}
	var tables int
	if err := owner.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name='whatsapp_incoming_capture'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("failed capture initialization committed a partial schema")
	}
}

func TestPrivateSessionInitializationRejectsWeakModesAndCancellation(t *testing.T) {
	for _, cell := range []string{"synchronous", "foreign_keys", "canceled"} {
		t.Run(cell, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			owner, err := OpenCaptureFixture(filepath.Join(t.TempDir(), "provider.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			switch cell {
			case "synchronous":
				if _, err := owner.db.ExecContext(ctx, `PRAGMA synchronous=NORMAL`); err != nil {
					t.Fatal(err)
				}
			case "foreign_keys":
				if _, err := owner.db.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel(errors.New("private initialization canceled"))
			}
			err = owner.configureDurability(ctx)
			if err == nil || cell == "canceled" && !errors.Is(err, context.Cause(ctx)) {
				t.Fatal("invalid initialization acquired durable readiness", err)
			}
		})
	}
}

func TestCaptureSchemaInitializationCanceledCreatesNothing(t *testing.T) {
	owner, err := OpenCaptureFixture(filepath.Join(t.TempDir(), "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := owner.Captures(ctx, "334d3c37-e7aa-4d12-aa88-4a72386b6d6a"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled capture initialization acquired authority", err)
	}
	var tables int
	if err := owner.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM sqlite_schema WHERE name IN ('whatsapp_incoming_capture','whatsapp_callback_failures')`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("canceled capture preparation published schema")
	}
}
