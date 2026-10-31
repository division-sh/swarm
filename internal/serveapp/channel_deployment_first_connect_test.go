//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/testutil"
)

// The public CLI, running serve and real SDK must own the entire signed
// pairing -> stream end -> authenticated successor -> registration journey.
func TestDeploymentWhatsAppFirstConnectThroughRunningServeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			peer.loginHandoff = true
			opts := cliapp.DefaultServeOptions()
			opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
			opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			opts.StoreMode, opts.StoreModeSet = backend, true
			opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
			opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "first-connect.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			config, err := os.ReadFile(opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			config = append(config, []byte(fmt.Sprintf("\npaths:\n  swarm_dir: %q\n", opts.SwarmDir))...)
			writeStandingCandidateFile(t, opts.ConfigPath, string(config))
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			defer func() {
				if code := process.stop(); code != 0 {
					t.Errorf("served first-connect shutdown exit=%d", code)
				}
			}()
			target, err := url.Parse("http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()))
			if err != nil {
				t.Fatal(err)
			}
			// Observe the authentic response without changing any request, result
			// or authority. Only the external peer uses the disclosed QR material.
			qr := make(chan channelonboarding.Result, 1)
			forward := httputil.NewSingleHostReverseProxy(target)
			forward.ModifyResponse = func(response *http.Response) error {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				_ = response.Body.Close()
				response.Body = io.NopCloser(bytes.NewReader(body))
				var observed struct {
					Result channelonboarding.Result `json:"result"`
				}
				if json.Unmarshal(body, &observed) == nil && observed.Result.Pairing != nil && observed.Result.Pairing.Code != "" {
					select {
					case qr <- observed.Result:
					default:
					}
				}
				return nil
			}
			proxy := httptest.NewServer(forward)
			defer proxy.Close()
			token := filepath.Join(t.TempDir(), "api-token")
			writeStandingCandidateFile(t, token, apiv1.DefaultLoopbackAPIToken+"\n")
			writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\nconnection:\n  api_server: %q\n  api_token_file: %q\n", proxy.URL, token))
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			out, errOut := &lockedBuffer{}, &lockedBuffer{}
			done := make(chan int, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
			}()
			go func() {
				done <- cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, out, errOut, nil, nil, OpenChannelRegistration)
			}()
			var pairing channelonboarding.Result
			select {
			case pairing = <-qr:
			case code := <-done:
				joined = true
				t.Fatalf("public connect exited before QR: exit=%d err=%s", code, errOut.String())
			case <-ctx.Done():
				t.Fatal("public connect did not disclose its SDK QR")
			}
			if !pairing.Operation.DeploymentRegistration || pairing.Operation.AccountConfirmed || pairing.Pairing.Paired {
				t.Fatal("first QR already claimed a registered or paired account")
			}
			peer.pair(pairing.Pairing.Code)
			select {
			case code := <-done:
				joined = true
				if code != 0 {
					t.Fatalf("public signed first-connect exit=%d err=%s", code, errOut.String())
				}
			case <-ctx.Done():
				t.Fatalf("public signed first-connect stalled: err=%s", errOut.String())
			}
			if !strings.Contains(out.String(), "Registered whatsapp account +15551234567.") || !strings.Contains(out.String(), "Linked to +15551234567") {
				t.Fatal("public connect omitted linked-account approval or registration")
			}
			var listing struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			var listed, listErr bytes.Buffer
			if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &listed, &listErr, nil, nil, OpenChannelRegistration); code != 0 || json.Unmarshal(listed.Bytes(), &listing) != nil || len(listing.Accounts) != 1 {
				t.Fatalf("public account-list exit=%d err=%s", code, listErr.String())
			}
			op := listing.Accounts[0].Operation
			if op.OperationID != pairing.Operation.OperationID || op.SessionConnectionID != pairing.Operation.SessionConnectionID ||
				op.Phase != channelonboarding.PhaseSucceeded || !op.AccountConfirmed || op.AccountReference != "+15551234567" || op.IdentityOperationID != "" {
				t.Fatal("first connect replaced its device/operation or failed to register the approved receptionist account")
			}
			peer.mu.Lock()
			connections, logins, uploads, closed := peer.connections, peer.postLogins, peer.prekeyUploads, peer.disconnected
			peer.mu.Unlock()
			if connections != 2 || logins != 1 || uploads != 1 || closed != 1 {
				t.Fatalf("pairing/login ownership: connections=%d logins=%d uploads=%d closed=%d", connections, logins, uploads, closed)
			}
		})
	}
}
