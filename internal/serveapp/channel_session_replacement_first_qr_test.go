//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/testutil"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type servedReplacementQRBatchCut struct {
	held, release, originalJoined chan struct{}
}

func TestServedReplacementHeldFirstQRReadOnlyProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			requireServedReplacementHeldFirstQR(t, backend)
		})
	}
}

func requireServedReplacementHeldFirstQR(t *testing.T, backend string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	peer := newServeNativeProtocolPeer(t)
	cut := &servedReplacementQRBatchCut{held: make(chan struct{}), release: make(chan struct{}), originalJoined: make(chan struct{})}
	peer.mu.Lock()
	peer.replacementQR = cut
	peer.mu.Unlock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(cut.release) }) }
	t.Cleanup(release)
	var selections, releases atomic.Int64
	prior := newServeSessionBootstrap
	owners := make(chan *serveSessionBootstrap, 1)
	newServeSessionBootstrap = func(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store,
		credentials operatorchannel.CredentialCurrentness, directory string, invocation *sessionprovider.InvocationState,
	) (serveSessionBootstrapOwner, error) {
		selected := func(ctx context.Context, candidate channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, processbinding.Binding, func() error, error) {
			owned, incoming, binding, done, err := selectRuntime(ctx, candidate)
			if err != nil {
				return owned, incoming, binding, done, err
			}
			selections.Add(1)
			return owned, incoming, binding, func() error { releases.Add(1); return done() }, nil
		}
		owner, err := prior(selected, store, credentials, directory, invocation)
		if err == nil {
			owners <- owner.(*serveSessionBootstrap)
		}
		return owner, err
	}
	t.Cleanup(func() { newServeSessionBootstrap = prior })
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
			filepath.Join(t.TempDir(), "held-replacement.sqlite"), channelOnboardingHostWorkspaceFields())
	} else {
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
	}
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	owner := <-owners
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	var original channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
		"provider": "whatsapp", "verb": "connect", "save_proof": false}, &original)
	original = requireReadOnlyReplacementQR(t, endpoint, original, time.Now().Add(5*time.Second))
	previous := owner.connection(original.Operation.OperationID)
	if previous == nil || previous.CheckBootstrap(t.Context()) != nil || selections.Load() != 1 || releases.Load() != 1 {
		t.Fatal("original QR lacks its one completed native owner")
	}
	peer.mu.Lock()
	wire := peer.wire
	peer.mu.Unlock()
	if err := wire.Send(peer.ctx, waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{
		"id": "HELD_REPLACEMENT_EXHAUSTED", "type": "set", "from": types.ServerJID},
		Content: []waBinary.Node{{Tag: "pair-device"}}}); err != nil {
		t.Fatal("expire original SDK batch", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var expired channelonboarding.Result
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": original.Operation.OperationID}, &expired)
		if expired.Pairing != nil && expired.Pairing.Status == "qr_expired" {
			if expired.Pairing.Code != "" || expired.Pairing.Paired || expired.Pairing.Connected || !reflect.DeepEqual(expired.Operation, original.Operation) {
				t.Fatal("original exhaustion changed its scope or retained code")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original SDK batch did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	proxy, retryCalls := observeReplacementRPC(t, endpoint, original.Operation.OperationID)
	token := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(token, []byte(apiv1.DefaultLoopbackAPIToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(ctx, []string{"channel", "resume", original.Operation.OperationID,
		"--api-server", proxy.URL, "--api-token-file", token, "--json"}, &stdout, &stderr, nil, nil)
	var held channelonboarding.Result
	if code != 0 || json.Unmarshal(stdout.Bytes(), &held) != nil {
		t.Fatal("one explicit replacement CLI call refused", code)
	}
	select {
	case <-cut.held:
	case <-ctx.Done():
		t.Fatal("replacement did not reach its real peer's held first batch")
	}
	select {
	case <-cut.originalJoined:
	case <-ctx.Done():
		t.Fatal("original peer did not retire/join")
	}
	current := owner.connection(original.Operation.OperationID)
	owner.mu.Lock()
	attempt := owner.connections[original.Operation.OperationID]
	owner.mu.Unlock()
	if attempt == nil {
		t.Fatal("replacement lost its owned attempt")
	}
	attempt.mu.Lock()
	connectComplete := attempt.connected && !attempt.connecting && attempt.connectErr == nil
	attempt.mu.Unlock()
	peer.mu.Lock()
	connections := peer.connections
	peer.mu.Unlock()
	if current == nil || current == previous || current.ConnectionID() != previous.ConnectionID() || current.CheckBootstrap(ctx) != nil ||
		previous.CheckBootstrap(ctx) == nil || previous.Close(ctx) != nil || !connectComplete ||
		selections.Load() != 2 || releases.Load() != 2 || connections != 2 || retryCalls.Load() != 1 {
		t.Fatal("replacement construction/connect/join discriminator differs", connectComplete, selections.Load(), releases.Load(), connections, retryCalls.Load())
	}
	requireHeldReplacementPending(t, held, original)
	// This is the exact old aid's RED predicate, after a successful CLI resume.
	if held.Pairing.Status == "awaiting_qr" && held.Pairing.Code != "" {
		t.Fatal("held replacement already satisfies the immediate current-code assertion")
	}
	t.Log("replacement discriminator: original_joined=true original_scope=true one_new_owner=true one_new_sdk_connection=true connect_complete=true held_preparing=true old_aid_current_code=false")
	var readback channelonboarding.Result
	getEndpoint := proxy.URL + "/v1/rpc"
	requireServedJSONRPCResult(t, getEndpoint, "channel.onboarding_get", map[string]any{"operation_id": original.Operation.OperationID}, &readback)
	requireHeldReplacementPending(t, readback, original)
	release()
	deadline, _ = ctx.Deadline()
	renewed := requireReadOnlyReplacementQR(t, getEndpoint, original, deadline)
	if renewed.Pairing.Code == original.Pairing.Code || !renewed.Pairing.ExpiresAt.After(original.Pairing.ExpiresAt) || retryCalls.Load() != 1 || owner.connection(original.Operation.OperationID) != current {
		t.Fatal("read-only release changed owner, repeated resume or reused original material")
	}
	if _, err := previous.ChannelExecution(t.Context()); err == nil {
		t.Fatal("retired original gained execution authority")
	}
	if _, err := current.ChannelExecution(t.Context()); err == nil {
		t.Fatal("unpaired replacement gained execution authority")
	}
	if code := process.stop(); code != 0 {
		t.Fatal("held replacement serving did not join", code)
	}
}

func requireReadOnlyReplacementQR(t *testing.T, endpoint string, original channelonboarding.Result, deadline time.Time) channelonboarding.Result {
	t.Helper()
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal("read-only replacement first batch exceeded its existing bound")
		}
		var current channelonboarding.Result
		response := requestServedJSONRPCWithTimeout(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": original.Operation.OperationID}, remaining)
		if response.Error != nil || json.Unmarshal(response.Result, &current) != nil {
			t.Fatal("authenticated replacement readback refused")
		}
		if !reflect.DeepEqual(current.Operation, original.Operation) || !reflect.DeepEqual(current.Candidate, original.Candidate) || current.Binding != nil || current.IdentityOperation != nil || current.Readiness != nil && current.Readiness.Ready {
			t.Fatal("read-only replacement progress changed scope/declaration or granted authority")
		}
		if current.Pairing != nil && current.Pairing.Status == "awaiting_qr" && current.Pairing.Code != "" && current.Pairing.ExpiresAt.After(time.Now()) && !current.Pairing.Paired && !current.Pairing.Connected {
			return current
		}
		if pending, err := current.SessionPreparationPending(); err != nil || !pending || time.Now().After(deadline) {
			t.Fatal("read-only replacement first batch refused or exceeded its existing bound")
		}
		time.Sleep(time.Millisecond)
	}
}

func requireHeldReplacementPending(t *testing.T, result, original channelonboarding.Result) {
	t.Helper()
	pending, err := result.SessionPreparationPending()
	if err != nil || !pending || !reflect.DeepEqual(result.Operation, original.Operation) ||
		!reflect.DeepEqual(result.Candidate, original.Candidate) || result.IdentityOperation != nil ||
		result.Binding != nil || result.Readiness != nil && result.Readiness.Ready {
		t.Fatal("held replacement lost canonical non-executable progress or its exact scope")
	}
}

func observeReplacementRPC(t *testing.T, endpoint, operation string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	target, err := url.Parse(strings.TrimSuffix(endpoint, "/v1/rpc"))
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	var retries atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			t.Error("read observed RPC")
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		var request struct {
			Method string `json:"method"`
			Params struct {
				OperationID string `json:"operation_id"`
			} `json:"params"`
		}
		if json.Unmarshal(body, &request) != nil || request.Params.OperationID != operation || r.Header.Get("Authorization") != "Bearer "+apiv1.DefaultLoopbackAPIToken ||
			(request.Method != "channel.onboarding_get" && request.Method != "channel.onboarding_retry") {
			t.Error("replacement request changed method, authentication or original operation")
			http.Error(w, "scope refused", http.StatusBadRequest)
			return
		}
		if request.Method == "channel.onboarding_retry" && retries.Add(1) != 1 {
			t.Error("replacement issued another explicit resume")
			http.Error(w, "repeat refused", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, &retries
}
