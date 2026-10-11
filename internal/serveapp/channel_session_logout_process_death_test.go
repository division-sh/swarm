//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type servedNativeLogoutReservationCut struct {
	serveSessionBootstrapOwner
}

func (s servedNativeLogoutReservationCut) DispatchSessionLogout(ctx context.Context, op channelonboarding.TeardownOperation) error {
	if op.Kind != channelonboarding.TeardownLogout || op.Phase != channelonboarding.TeardownAuthorityRetired ||
		op.Logout == nil || op.Logout.Validate() != nil {
		return channelonboarding.ErrConflict
	}
	fmt.Fprintln(os.Stdout, "NATIVE_LOGOUT_RESERVED "+op.TeardownID)
	<-ctx.Done()
	return context.Cause(ctx)
}

func installServedNativeLogoutReservationCut(t *testing.T) {
	t.Helper()
	previous := newServeSessionBootstrap
	newServeSessionBootstrap = func(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store,
		credentials operatorchannel.CredentialCurrentness, directory string,
	) (serveSessionBootstrapOwner, error) {
		owner, err := previous(selectRuntime, store, credentials, directory)
		if err != nil {
			return nil, err
		}
		return servedNativeLogoutReservationCut{serveSessionBootstrapOwner: owner}, nil
	}
	t.Cleanup(func() { newServeSessionBootstrap = previous })
}

func TestServedWhatsAppLogoutReservationProcessDeathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts, _ := servedNativeProcessOptions(t, backend)
			process := startServedNativeSDKProcessWithLogoutCut(t, opts, peer, true)
			endpoint := process.endpoint(t) + "/v1/rpc"
			ready := requireServedNativeProcessPairing(t, endpoint, peer)
			peer.mu.Lock()
			connections := peer.connections
			peer.mu.Unlock()
			params := map[string]any{"operation_id": ready.Operation.OperationID, "expected_revision": ready.Operation.Revision,
				"idempotency_key": "reserved-logout-process-death"}
			done := beginServedNativeInterruptedLogout(t, endpoint, params)
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(process.output.String(), "NATIVE_LOGOUT_RESERVED ") {
				if time.Now().After(deadline) {
					t.Fatal("public logout missed its committed reservation boundary", process.output.String())
				}
				time.Sleep(5 * time.Millisecond)
			}
			var reserved channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &reserved)
			if reserved.Logout == nil || reserved.Logout.Teardown.Phase != channelonboarding.TeardownAuthorityRetired ||
				reserved.Operation.Phase != channelonboarding.PhaseRetired || reserved.Logout.Teardown.Logout != nil {
				t.Fatal("reservation lost private-target-free frozen responsibility", reserved)
			}
			select {
			case frame := <-peer.logouts:
				t.Fatal("reservation-only cut already sent unlink", frame)
			default:
			}
			if err := process.kill(); err != nil {
				t.Fatal("reserved logout process did not die and join", err)
			}
			requireServedNativeDisconnect(t, peer)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("reservation-only death fabricated successful logout")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("dead reservation left the original caller unresolved")
			}
			enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
			for range 2 {
				process = startServedNativeSDKProcess(t, opts, peer)
				endpoint = process.endpoint(t) + "/v1/rpc"
				var historical channelonboarding.Result
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &historical)
				if historical.Logout == nil || historical.Logout.Teardown.TeardownID != reserved.Logout.Teardown.TeardownID ||
					historical.Logout.EffectOperationID != reserved.Logout.EffectOperationID ||
					historical.Logout.Teardown.Revision != reserved.Logout.Teardown.Revision ||
					historical.Logout.Teardown.Phase != channelonboarding.TeardownAuthorityRetired ||
					historical.Operation.Phase != channelonboarding.PhaseRetired ||
					(historical.Readiness != nil && historical.Readiness.Ready) {
					t.Fatal("reservation-only recovery fabricated settlement or fresh execution", historical)
				}
				requireServedJSONRPCError(t, endpoint, "channel.logout", params)
				var unchanged channelonboarding.Result
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &unchanged)
				if unchanged.Logout == nil || unchanged.Logout.Teardown.Revision != reserved.Logout.Teardown.Revision ||
					unchanged.Logout.Teardown.Phase != channelonboarding.TeardownAuthorityRetired {
					t.Fatal("missing original SDK owner rewrote pending responsibility", unchanged)
				}
				if err := process.stop(); err != nil {
					t.Fatal("reservation-only recovery process did not join", err, process.output.String())
				}
				peer.mu.Lock()
				currentConnections := peer.connections
				peer.mu.Unlock()
				if currentConnections != connections {
					t.Fatal("reservation-only recovery reconnected or adopted a successor", currentConnections, connections)
				}
				select {
				case frame := <-peer.logouts:
					t.Fatal("reservation-only recovery/replay sent unlink", frame)
				default:
				}
			}
		})
	}
}

func TestServedWhatsAppLogoutLaunchedProcessDeathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts, _ := servedNativeProcessOptions(t, backend)
			process := startServedNativeSDKProcess(t, opts, peer)
			endpoint := process.endpoint(t) + "/v1/rpc"
			ready := requireServedNativeProcessPairing(t, endpoint, peer)
			peer.mu.Lock()
			peer.withholdLogout = true
			connections := peer.connections
			peer.mu.Unlock()
			params := map[string]any{"operation_id": ready.Operation.OperationID, "expected_revision": ready.Operation.Revision,
				"idempotency_key": "launched-logout-process-death"}
			done := beginServedNativeInterruptedLogout(t, endpoint, params)
			select {
			case <-peer.logouts:
			case err := <-done:
				t.Fatal("public logout returned before the unacknowledged unlink frame", err)
			case <-time.After(5 * time.Second):
				t.Fatal("public logout never reached the actual SDK unlink frame")
			}
			var launched channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &launched)
			if launched.Logout == nil || launched.Logout.Teardown.Phase != channelonboarding.TeardownAuthorityRetired ||
				launched.Operation.Phase != channelonboarding.PhaseRetired || launched.Logout.Teardown.Logout != nil {
				t.Fatal("unacknowledged SDK effect lost exact private-target-free responsibility", launched)
			}
			if err := process.kill(); err != nil {
				t.Fatal("launched logout process did not die and join", err)
			}
			requireServedNativeDisconnect(t, peer)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("process death fabricated a successful public logout response")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("dead logout process left the original public caller unresolved")
			}
			enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
			for range 2 {
				process = startServedNativeSDKProcess(t, opts, peer)
				endpoint = process.endpoint(t) + "/v1/rpc"
				requireServedNativeUncertainLogout(t, endpoint, params, launched)
				peer.mu.Lock()
				currentConnections := peer.connections
				peer.mu.Unlock()
				if currentConnections != connections {
					t.Fatal("uncertain logout reconnected the original or adopted a successor", currentConnections, connections)
				}
				if err := process.stop(); err != nil {
					t.Fatal("recovered uncertain logout process did not join", err, process.output.String())
				}
				select {
				case frame := <-peer.logouts:
					t.Fatal("restart/replay sent another unlink frame", frame)
				default:
				}
			}
		})
	}
}

func requireServedNativeUncertainLogout(t *testing.T, endpoint string, params map[string]any, original channelonboarding.Result) {
	t.Helper()
	var historical channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": original.Operation.OperationID}, &historical)
	if historical.Logout == nil || historical.Logout.EffectOperationID != original.Logout.EffectOperationID ||
		historical.Logout.Teardown.TeardownID != original.Logout.Teardown.TeardownID ||
		historical.Logout.Teardown.Revision != original.Logout.Teardown.Revision+1 ||
		historical.Logout.Teardown.Phase != channelonboarding.TeardownFailed || historical.Logout.Teardown.FailureCode != "logout_outcome_uncertain" ||
		historical.Operation.Phase != channelonboarding.PhaseRetired || (historical.Readiness != nil && historical.Readiness.Ready) {
		t.Fatal("restart lost truthful uncertain logout history or restored business authority", historical)
	}
	var replay channelonboarding.SessionLogoutReadback
	requireServedJSONRPCResult(t, endpoint, "channel.logout", params, &replay)
	if replay.EffectOperationID != historical.Logout.EffectOperationID || replay.Teardown.TeardownID != historical.Logout.Teardown.TeardownID ||
		replay.Teardown.Revision != historical.Logout.Teardown.Revision || replay.Teardown.Phase != channelonboarding.TeardownFailed {
		t.Fatal("uncertain public replay replaced history or fabricated success", replay)
	}
	cli := requireServedNativeCLILogout(t, endpoint, original.Operation.OperationID, params["idempotency_key"].(string))
	if cli.EffectOperationID != replay.EffectOperationID || cli.Teardown.TeardownID != replay.Teardown.TeardownID ||
		cli.Teardown.Phase != replay.Teardown.Phase || cli.Teardown.Revision != replay.Teardown.Revision {
		t.Fatal("CLI replay changed uncertain logout history", cli)
	}
	changed := map[string]any{"operation_id": params["operation_id"], "expected_revision": params["expected_revision"], "idempotency_key": "changed-logout-key"}
	requireServedJSONRPCError(t, endpoint, "channel.logout", changed)
}

func beginServedNativeInterruptedLogout(t *testing.T, endpoint string, params map[string]any) <-chan error {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "interrupted-logout", "method": "channel.logout", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	done, joined := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(joined)
		response, err := http.DefaultClient.Do(req)
		if response != nil {
			_, readErr := io.Copy(io.Discard, response.Body)
			err = errors.Join(err, readErr, response.Body.Close())
		}
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("original logout HTTP caller did not join")
		}
	})
	return done
}
