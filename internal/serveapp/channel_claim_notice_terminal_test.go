package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
)

// This is the public Go CLI against the real served API/SDK peer, not the
// separate peer-trust cmd/swarm artifact or a physical phone.
func requireServedClaimCodeNoticeTerminal(t *testing.T, endpoint, operationID string, deliver func()) {
	t.Helper()
	token := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(token, []byte(apiv1.DefaultLoopbackAPIToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out, diagnostic := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	go func() {
		done <- cliapp.Execute(ctx, []string{"channel", "resume", operationID, "--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"), "--api-token-file", token}, out, diagnostic, nil, nil, ChannelRegistrations())
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	for !strings.Contains(out.String(), "Waiting for an authenticated claimant") {
		select {
		case code := <-done:
			joined = true
			t.Fatal("public notice observer exited before claim wait", code, diagnostic.String())
		case <-ctx.Done():
			t.Fatal("public notice observer did not reach the open ceremony", diagnostic.String())
		case <-time.After(time.Millisecond):
		}
	}
	deliver()
	for !strings.Contains(diagnostic.String(), "error: bad code\n") && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if strings.Count(diagnostic.String(), "error: bad code\n") != 1 {
		t.Fatal("numeric customer message lost/coalesced the wrong terminal notice", diagnostic.String())
	}
	cancel()
	if code := <-done; code == 0 {
		t.Error("cancelled notice observer claimed success")
	}
	joined = true
}
