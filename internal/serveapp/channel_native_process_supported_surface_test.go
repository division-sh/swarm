package serveapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestChannelNativeInstallAbruptProcessDeathPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, cut := range []string{"launched", "settled"} {
			t.Run(string(backend)+"/"+cut, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				first := startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = first.endpoint(t)
				reader := channelProcessStorageObservation(t, h)
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
				operation, setting := waitChannelNativeProcessAttempt(t, reader, state)
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
				afterOperation, afterSetting := waitChannelNativeProcessAttempt(t, reader, wantAttempt)
				readback := waitChannelNativeProcessSetting(t, reader, setting, wantSetting)
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

func waitChannelNativeProcessSetting(t *testing.T, reader storetest.ChannelProcessObservation, setting, want string) sql.NullString {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		observed, err := reader.ReadChannelNativeProcessSettingStorage(context.Background(), setting)
		if err != nil {
			t.Fatal(err)
		}
		if observed.State == want {
			return observed.Readback
		}
		if time.Now().After(deadline) {
			t.Fatalf("native setting state=%s want=%s readback=%v", observed.State, want, observed.Readback)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitChannelNativeProcessAttempt(t *testing.T, reader storetest.ChannelProcessObservation, state string) (string, string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		observed, err := reader.ReadChannelNativeProcessAttemptStorage(context.Background())
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err == nil && observed.State == state {
			return observed.OperationID, observed.SettingID
		}
		if time.Now().After(deadline) {
			t.Fatalf("native install attempt=%s want=%s: %v", observed.State, state, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
