//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

func logServedNativeWait(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, phase string) {
	t.Helper()
	peer.mu.Lock()
	connections, closed, uploads, logins := peer.connections, peer.disconnected, peer.prekeyUploads, peer.postLogins
	peer.mu.Unlock()
	t.Logf("native wait phase=%s connections=%d closed=%d uploads=%d post_logins=%d queued_receipts=%d queued_sends=%d",
		phase, connections, closed, uploads, logins, len(peer.receipts), len(peer.sent))
	var stacks bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
		t.Log("native wait stack diagnostic:", err)
	} else {
		t.Logf("native wait stacks at original deadline:\n%s", stacks.String())
	}
	// Read-only diagnostic, never a retry of the failed request or a timing waiver.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "native-wait-readback", "method": "channel.list", "params": map[string]any{}})
	if err != nil {
		t.Log("native readback encode:", err)
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Log("native readback request:", err)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Log("native readback disposition:", err)
		return
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	t.Logf("native readback status=%d error=%v body=%s", response.StatusCode, err, raw)
}
