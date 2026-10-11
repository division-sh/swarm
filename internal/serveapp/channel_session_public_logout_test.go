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
	for _, params := range []map[string]any{
		{"operation_id": op.OperationID, "expected_revision": op.Revision + 1, "idempotency_key": "native-logout-stale"},
		{"operation_id": op.OperationID, "expected_revision": op.Revision, "account": "caller-cannot-select-account", "idempotency_key": "native-logout-account"},
	} {
		if response := requestServedJSONRPC(t, endpoint, "channel.logout", params); response.Error == nil {
			t.Fatal("public logout accepted stale or caller-selected authority", response)
		}
	}
	params := map[string]any{"operation_id": op.OperationID, "expected_revision": op.Revision, "idempotency_key": "native-explicit-logout"}
	result := requireServedNativeCLILogout(t, endpoint, op.OperationID, "native-explicit-logout")
	if result.Validate() != nil || result.OperationID != op.OperationID || result.ExpectedRevision != op.Revision ||
		result.Teardown.Phase != channelonboarding.TeardownSucceeded || result.Teardown.Logout != nil {
		t.Fatal("public logout lost truthful private-target-free settlement", result)
	}
	var replay channelonboarding.SessionLogoutReadback
	requireServedJSONRPCResult(t, endpoint, "channel.logout", params, &replay)
	if replay.EffectOperationID != result.EffectOperationID || replay.Teardown.TeardownID != result.Teardown.TeardownID ||
		replay.Teardown.Revision != result.Teardown.Revision || replay.Teardown.Phase != result.Teardown.Phase {
		t.Fatal("public idempotency replay changed logout responsibility", replay)
	}
	var historical channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": op.OperationID}, &historical)
	if historical.Operation.Phase != channelonboarding.PhaseRetired || historical.Logout == nil ||
		historical.Logout.EffectOperationID != result.EffectOperationID || historical.Logout.Teardown.Revision != result.Teardown.Revision {
		t.Fatal("authenticated readback lost exact logout history", historical)
	}
	requireServedNativeDisconnect(t, peer)
}

func requireServedNativeCLILogout(t *testing.T, endpoint, operationID, key string) channelonboarding.SessionLogoutReadback {
	t.Helper()
	server, err := url.Parse(endpoint)
	if err != nil || server.Path != "/v1/rpc" || server.RawQuery != "" || server.Fragment != "" {
		t.Fatal("CLI proof requires the exact served RPC endpoint", endpoint, err)
	}
	server.Path = ""
	token := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(token, []byte(apiv1.DefaultLoopbackAPIToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := executeCLI(ctx, []string{"channel", "logout", operationID, "--idempotency-key", key,
		"--api-server", server.String(), "--api-token-file", token, "--json"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatal("public CLI logout failed", code, stdout.String(), stderr.String())
	}
	var result channelonboarding.SessionLogoutReadback
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal("public CLI lost exact machine logout result", err, stdout.String())
	}
	return result
}
