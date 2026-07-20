package builder

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/gorilla/websocket"
)

func TestBuilderHealthSubscriptionReplacementFence(t *testing.T) {
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "builder-health-runtime",
		BundleHash:        "builder-health-bundle",
	})
	if err != nil {
		t.Fatalf("new runtime occurrence: %v", err)
	}
	h := &handler{
		health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		version:          "test",
		processWorkOwner: process,
		runtimeAcquirer: &testRuntimeAcquirer{
			runtime: &runtimepkg.Runtime{},
			owner:   owner,
			process: process,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	server := httptest.NewServer(mux)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial builder websocket: %v", err)
	}
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "engine:health"}); err != nil {
		t.Fatalf("subscribe health: %v", err)
	}
	var frame WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read health frame: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("health channel = %q", frame.Channel)
	}
	if got := owner.ActiveCount(); got != 1 {
		t.Fatalf("active health subscription generation work = %d, want 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := owner.RetireAndWait(ctx); err != nil {
		t.Fatalf("retire health runtime occurrence: %v", err)
	}
	if got := owner.ActiveCount(); got != 0 {
		t.Fatalf("active health subscription generation work after retirement = %d, want 0", got)
	}
	_ = conn.Close()
	server.Close()
	if _, err := process.Join(ctx); err != nil {
		t.Fatalf("join builder process: %v", err)
	}
}
