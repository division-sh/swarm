package releasee2e

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"modernc.org/sqlite"
	sqlitelib "modernc.org/sqlite/lib"
)

func TestStandingOperatorFaultDDLSQLiteNativeContention(t *testing.T) {
	for _, cancelOnBusy := range []bool{false, true} {
		t.Run(map[bool]string{false: "release", true: "cancel"}[cancelOnBusy], func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fault.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := db.ExecContext(ctx, `CREATE TABLE standing_services (value TEXT); INSERT INTO standing_services VALUES ('unchanged')`); err != nil {
				t.Fatal(err)
			}
			blocker, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Close()
			if _, err := blocker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
				t.Fatal(err)
			}
			held := true
			release := func() {
				t.Helper()
				if held {
					if _, err := blocker.ExecContext(context.Background(), `ROLLBACK`); err != nil {
						t.Fatal(err)
					}
					held = false
				}
			}
			defer release()
			const trigger = `CREATE TRIGGER fail_standing_operator BEFORE UPDATE ON standing_services BEGIN SELECT RAISE(ABORT, 'injected standing operator failure'); END`
			_, err = db.ExecContext(ctx, trigger)
			var native *sqlite.Error
			if !errors.As(err, &native) || native.Code()&0xff != sqlitelib.SQLITE_BUSY {
				t.Fatalf("native control did not reproduce fault-setup contention: %v", err)
			}
			observed := 0
			err = execStandingFaultDDL(ctx, db, trigger, func() {
				observed++
				if cancelOnBusy {
					cancel()
				} else {
					// Release in the actual BUSY path, without an observer scheduling dependency.
					release()
				}
			})
			if observed != 1 {
				t.Fatalf("native BUSY observations=%d, want exactly one", observed)
			}
			if cancelOnBusy {
				if !errors.Is(err, context.Canceled) || !errors.As(err, &native) {
					t.Fatalf("cancelled setup lost cancellation/native evidence: %v", err)
				}
				release()
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='trigger'`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("cancelled setup left trigger evidence: count=%d err=%v", count, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE standing_services SET value='changed'`); err == nil || !strings.Contains(err.Error(), "injected standing operator failure") {
				t.Fatalf("installed trigger did not inject the exact operation failure: %v", err)
			}
			var value string
			if err := db.QueryRowContext(ctx, `SELECT value FROM standing_services`).Scan(&value); err != nil || value != "unchanged" {
				t.Fatalf("fault changed business evidence: value=%q err=%v", value, err)
			}
			if err := execStandingFaultDDL(ctx, db, `DROP TRIGGER fail_standing_operator`, nil); err != nil {
				t.Fatal(err)
			}
			observed = 0
			if err := execStandingFaultDDL(ctx, db, `INVALID SQL`, func() { observed++ }); err == nil || observed != 0 {
				t.Fatalf("non-BUSY failure was retried or swallowed: %v retries=%d", err, observed)
			}
		})
	}
}
