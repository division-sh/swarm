//go:build linux || darwin

package serveapp

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func startServedNativeDiagnosticTrace(t *testing.T) {
	t.Helper()
	directory := os.Getenv(servedNativeTraceDirectoryEnv)
	if directory == "" {
		return
	}
	if !filepath.IsAbs(directory) {
		t.Fatal("native diagnostic trace requires an absolute test-owned directory")
	}
	path := filepath.Join(directory, fmt.Sprintf("native-%d.trace", os.Getpid()))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := trace.Start(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		trace.Stop()
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestServedWhatsAppStartupPhaseDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts, _ := servedNativeProcessOptions(t, backend)
			process := startServedNativeSDKProcess(t, opts, peer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			began := time.Now()
			t.Logf("native initial request began=%s", began.UTC().Format(time.RFC3339Nano))
			var result channelonboarding.Result
			func() {
				defer func() {
					t.Logf("native initial request ended=%s interval=%s", time.Now().UTC().Format(time.RFC3339Nano), time.Since(began))
				}()
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
					"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "native-startup-diagnostic"}, &result)
			}()
			if result.Operation.Phase != channelonboarding.PhaseActivatingProvider ||
				result.Operation.Coordinate.TargetGeneration != 0 || result.Operation.SessionAccount.AccountRef != "" ||
				(result.Readiness != nil && result.Readiness.Ready) {
				t.Fatal("unpaired preparation manufactured executable authority", result.Operation.Phase)
			}
			if err := process.stop(); err != nil {
				t.Fatal("unpaired diagnostic process did not join", err, process.output.String())
			}
		})
	}
}
