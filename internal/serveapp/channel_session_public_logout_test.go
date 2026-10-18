//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
)

func TestRunServeWhatsAppPublicLogoutBothStores(t *testing.T) {
	testRunServeWhatsAppSignedPairing(t, servedNativeJourneyProof{explicitLogout: true})
}

func requireServedNativeExplicitLogout(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, op channelonboarding.Operation) {
	t.Helper()
	_, restore := nativeJourneyDiagnosticScope(t, "logout")
	defer restore()
	for index, params := range []map[string]any{
		{"operation_id": op.OperationID, "expected_revision": op.Revision + 1, "idempotency_key": "native-logout-stale"},
		{"operation_id": op.OperationID, "expected_revision": op.Revision, "account": "caller-cannot-select-account", "idempotency_key": "native-logout-account"},
	} {
		if response := requestServedJSONRPC(t, endpoint, "channel.logout", params); response.Error == nil {
			site := "logout_stale_refusal"
			if index == 1 {
				site = "logout_account_refusal"
			}
			logNativeJourneyFailure(t, "logout", site, map[string]bool{"rpc_error": false}, nil)
			t.Fatal("public logout accepted stale or caller-selected authority", response)
		}
	}
	params := map[string]any{"operation_id": op.OperationID, "expected_revision": op.Revision, "idempotency_key": "native-explicit-logout"}
	result := requireServedNativeCLILogout(t, endpoint, op.OperationID, "native-explicit-logout")
	validationErr := result.Validate()
	if validationErr != nil || result.OperationID != op.OperationID || result.ExpectedRevision != op.Revision ||
		result.Teardown.Phase != channelonboarding.TeardownSucceeded || result.Teardown.Logout != nil {
		logNativeJourneyFailure(t, "logout", "logout_settlement", map[string]bool{"valid": validationErr == nil,
			"operation_equal": result.OperationID == op.OperationID, "revision_equal": result.ExpectedRevision == op.Revision,
			"teardown_succeeded": result.Teardown.Phase == channelonboarding.TeardownSucceeded, "private_target_absent": result.Teardown.Logout == nil}, validationErr)
		t.Fatal("public logout lost truthful private-target-free settlement", result)
	}
	var replay channelonboarding.SessionLogoutReadback
	requireServedJSONRPCResult(t, endpoint, "channel.logout", params, &replay)
	if replay.EffectOperationID != result.EffectOperationID || replay.Teardown.TeardownID != result.Teardown.TeardownID ||
		replay.Teardown.Revision != result.Teardown.Revision || replay.Teardown.Phase != result.Teardown.Phase {
		logNativeJourneyFailure(t, "logout", "logout_replay", map[string]bool{"effect_equal": replay.EffectOperationID == result.EffectOperationID,
			"teardown_equal": replay.Teardown.TeardownID == result.Teardown.TeardownID, "revision_equal": replay.Teardown.Revision == result.Teardown.Revision,
			"phase_equal": replay.Teardown.Phase == result.Teardown.Phase}, nil)
		t.Fatal("public idempotency replay changed logout responsibility", replay)
	}
	var historical channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": op.OperationID}, &historical)
	if historical.Operation.Phase != channelonboarding.PhaseRetired || historical.Logout == nil ||
		historical.Logout.EffectOperationID != result.EffectOperationID || historical.Logout.Teardown.Revision != result.Teardown.Revision {
		flags := map[string]bool{"retired": historical.Operation.Phase == channelonboarding.PhaseRetired, "logout_present": historical.Logout != nil}
		if historical.Logout != nil {
			flags["effect_equal"] = historical.Logout.EffectOperationID == result.EffectOperationID
			flags["revision_equal"] = historical.Logout.Teardown.Revision == result.Teardown.Revision
		}
		logNativeJourneyFailure(t, "logout", "logout_history", flags, nil)
		t.Fatal("authenticated readback lost exact logout history", historical)
	}
	requireServedNativeDisconnect(t, peer)
}

func requireServedNativeCLILogout(t *testing.T, endpoint, operationID, key string) channelonboarding.SessionLogoutReadback {
	t.Helper()
	server, err := url.Parse(endpoint)
	if err != nil || server.Path != "/v1/rpc" || server.RawQuery != "" || server.Fragment != "" {
		flags := map[string]bool{"parsed": err == nil}
		if server != nil {
			flags["path_exact"] = server.Path == "/v1/rpc"
			flags["query_absent"] = server.RawQuery == ""
			flags["fragment_absent"] = server.Fragment == ""
		}
		logNativeJourneyFailure(t, "logout", "logout_cli_endpoint", flags, err)
		t.Fatal("CLI proof requires the exact served RPC endpoint", endpoint, err)
	}
	server.Path = ""
	token := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(token, []byte(apiv1.DefaultLoopbackAPIToken+"\n"), 0o600); err != nil {
		logNativeJourneyFailure(t, "logout", "logout_cli_token_file", nil, err)
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := executeCLI(ctx, []string{"channel", "logout", operationID, "--idempotency-key", key,
		"--api-server", server.String(), "--api-token-file", token, "--json"}, &stdout, &stderr, nil)
	if code != 0 {
		logNativeJourneyFailure(t, "logout", "logout_cli_exit", map[string]bool{"exit_zero": code == 0}, nil)
		t.Fatal("public CLI logout failed", code, stdout.String(), stderr.String())
	}
	var result channelonboarding.SessionLogoutReadback
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		logNativeJourneyFailure(t, "logout", "logout_cli_result", nil, err)
		t.Fatal("public CLI lost exact machine logout result", err, stdout.String())
	}
	return result
}
