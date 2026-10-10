//go:build linux || darwin

package serveapp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/testutil"
	"go.mau.fi/whatsmeow"
)

// These are public peer trust coordinates, not SDK state or launch permission.
type servedNativeClientTrust struct {
	Address     string
	Certificate []byte
	NoiseRoot   [32]byte
}

type servedNativeProcessConfig struct {
	ConfigPath, SourceRoot, PlatformSpecPath, SwarmDir, StoreMode string
	Trust                                                         servedNativeClientTrust
}

const servedNativeProcessEnv = "TEST_SERVED_WHATSAPP_PROCESS"

func startServedNativeSDKProcess(t *testing.T, opts cliapp.ServeOptions, peer *serveNativeProtocolPeer) *channelOnboardingCrashServeProcess {
	t.Helper()
	config, err := json.Marshal(servedNativeProcessConfig{ConfigPath: opts.ConfigPath, SourceRoot: opts.SourceRoot,
		PlatformSpecPath: opts.PlatformSpecPath, SwarmDir: opts.SwarmDir, StoreMode: opts.StoreMode, Trust: peer.clientTrust})
	if err != nil {
		t.Fatal(err)
	}
	return startServedCrashProcess(t, "TestServedWhatsAppSDKProcessHelper", []string{servedNativeProcessEnv + "=" + string(config)})
}

func TestServedWhatsAppSDKProcessHelper(t *testing.T) {
	encoded := os.Getenv(servedNativeProcessEnv)
	if encoded == "" {
		t.Skip("subprocess helper")
	}
	var config servedNativeProcessConfig
	if err := json.Unmarshal([]byte(encoded), &config); err != nil {
		t.Fatal(err)
	}
	host, _, err := net.SplitHostPort(config.Trust.Address)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("SDK process peer must be loopback", err)
	}
	certificate, err := x509.ParseCertificate(config.Trust.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(certificate)
	base := http.DefaultTransport
	transport := base.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12}
	dial := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "web.whatsapp.com:443" {
			return dial.DialContext(ctx, network, config.Trust.Address)
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("SDK process refuses external destination %q", address)
		}
		return dial.DialContext(ctx, network, address)
	}
	http.DefaultTransport = transport
	root := whatsmeow.WACertPubKey
	whatsmeow.WACertPubKey = config.Trust.NoiseRoot
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		http.DefaultTransport = base
		whatsmeow.WACertPubKey = root
	})
	opts := cliapp.DefaultServeOptions()
	opts.ConfigPath, opts.SourceRoot, opts.PlatformSpecPath = config.ConfigPath, config.SourceRoot, config.PlatformSpecPath
	opts.StoreMode, opts.StoreModeSet = config.StoreMode, true
	opts.SwarmDir, opts.SwarmDirSet = config.SwarmDir, true
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.SelfCheck = true
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("native SDK served process exited %d", code)
	}
}

func TestServedWhatsAppUnpairedSDKProcessRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts := cliapp.ServeOptions{SourceRoot: filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply"),
				PlatformSpecPath: defaultPlatformSpecPath, SwarmDir: t.TempDir(), StoreMode: backend}
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "native-process.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			process := startServedNativeSDKProcess(t, opts, peer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			var result channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
				"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "native-process-pairing"}, &result)
			operationID := result.Operation.OperationID
			for iteration := range 2 {
				deadline := time.Now().Add(5 * time.Second)
				for result.Pairing == nil || result.Pairing.Code == "" {
					if time.Now().After(deadline) {
						t.Fatal("real SDK subprocess did not disclose original authorized pairing", result)
					}
					time.Sleep(10 * time.Millisecond)
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": operationID}, &result)
				}
				if result.Operation.OperationID != operationID || result.Operation.Coordinate.TargetGeneration != 0 || result.Operation.SessionAccount.AccountRef != "" {
					t.Fatal("pairing minted business/account authority or replaced the original operation", result.Operation)
				}
				if err := process.stop(); err != nil {
					t.Fatal("real SDK subprocess did not join", err, process.output.String())
				}
				if iteration == 0 {
					process = startServedNativeSDKProcess(t, opts, peer)
					endpoint = process.endpoint(t) + "/v1/rpc"
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": operationID}, &result)
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				peer.mu.Lock()
				connections, disconnected := peer.connections, peer.disconnected
				peer.mu.Unlock()
				if connections == 2 && connections == disconnected {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("original SDK socket work outlived subprocess joining", connections, disconnected)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}
