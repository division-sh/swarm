package serveapp

import (
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelNativeInstallAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, cut := range []string{"launched", "settled"} {
			t.Run(string(backend)+"/"+cut, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				driver := "sqlite"
				if backend == servedparity.BackendExplicitPostgres {
					driver = "postgres"
				}
				db, err := sql.Open(driver, h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				var arrived <-chan struct{}
				var release func()
				if cut == "launched" {
					arrived, release = h.provider.PauseNextCommandApply()
					t.Cleanup(release)
				}
				runChannelOnboardingCLIJourney(t, h.opts.ConfigPath, h.endpoint, h.provider, "connect", "native-crash-token", 1001, "private", 0)
				if arrived != nil {
					select {
					case <-arrived:
					case <-time.After(20 * time.Second):
						t.Fatalf("native install never reached provider barrier\n%s", first.output.String())
					}
				}
				state := "settled"
				if cut == "launched" {
					state = "launched"
				}
				operation, setting := waitChannelNativeProcessAttempt(t, db, state)
				if err := first.kill(); err != nil || first.waitError() == nil {
					t.Fatalf("native process did not die abruptly: %v / %v", err, first.waitError())
				}
				if release != nil {
					release()
				}
				second := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = second.endpoint(t)
				wantAttempt, wantSetting := "settled", "installed"
				if cut == "launched" {
					wantAttempt, wantSetting = "outcome_uncertain", "uncertain"
				}
				afterOperation, afterSetting := waitChannelNativeProcessAttempt(t, db, wantAttempt)
				readback := waitChannelNativeProcessSetting(t, db, setting, wantSetting)
				if operation != afterOperation || setting != afterSetting || readback.Valid != (cut == "settled") {
					t.Fatalf("native recovery lost exact outcome: op=%s/%s setting=%s/%s readback=%v want=%s", operation, afterOperation, setting, afterSetting, readback, wantSetting)
				}
				if writes := h.provider.CommandWrites(); len(writes) != 1 {
					t.Fatalf("startup replayed native install: %d writes", len(writes))
				}
				if err := second.stop(); err != nil {
					t.Fatalf("stop native recovery process: %v\n%s", err, second.output.String())
				}
			})
		}
	}
}

func waitChannelNativeProcessSetting(t *testing.T, db *sql.DB, setting, want string) sql.NullString {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var state string
		var readback sql.NullString
		if err := db.QueryRow(`SELECT state,readback_hash FROM channel_native_settings WHERE setting_id=$1`, setting).Scan(&state, &readback); err != nil {
			t.Fatal(err)
		}
		if state == want {
			return readback
		}
		if time.Now().After(deadline) {
			t.Fatalf("native setting state=%s want=%s readback=%v", state, want, readback)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelNativeProcessAttempt(t *testing.T, db *sql.DB, state string) (string, string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var operation, setting, actual string
		err := db.QueryRow(`SELECT CAST(o.operation_id AS TEXT),CAST(s.setting_id AS TEXT),a.state
			FROM channel_native_settings s JOIN runtime_external_effect_operations o ON o.operation_id=s.install_operation_id
			JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.authority_kind='channel_native_setting'`).Scan(&operation, &setting, &actual)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err == nil && actual == state {
			return operation, setting
		}
		if time.Now().After(deadline) {
			t.Fatalf("native install attempt=%s want=%s: %v", actual, state, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
