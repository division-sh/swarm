package serveapp

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

var retiredDashboardPaths = []string{
	"/api", "/api/health", "/api/healthz", "/api/status", "/api/events",
	"/api/events/id", "/api/events/stream", "/api/runtime/logs",
	"/api/runtime/logs/stream", "/api/runtime/incidents", "/api/runtime/actions",
	"/api/agents", "/api/agents/id", "/api/agents/id/directive",
	"/api/agents/id/restart", "/api/conversations", "/api/conversations/id",
	"/api/instances", "/api/instances/id", "/api/mailbox", "/api/mailbox/id",
	"/rpc", "/api/rpc", "/ws", "/api/ws",
}

func assertServedRetiredDashboardRefusal(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	counts := func() [5]int {
		t.Helper()
		var out [5]int
		for i, table := range []string{"runs", "entity_state", "agents", "mailbox", "api_idempotency"} {
			if err := rt.DB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&out[i]); err != nil {
				t.Fatalf("retired route state snapshot: %v", err)
			}
		}
		return out
	}
	before := counts()
	client := http.Client{Timeout: 5 * time.Second}
	for _, path := range retiredDashboardPaths {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, auth := range []string{"", "Bearer invalid", "Bearer " + apiv1.DefaultLoopbackAPIToken, "Bearer retired"} {
				req, err := http.NewRequest(method, strings.TrimSuffix(rt.Endpoint, "/v1/rpc")+path, strings.NewReader(`{"action":"reset_state"}`))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", auth)
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil || response.StatusCode != http.StatusNotFound {
					t.Fatalf("%s retired %s %s: %d read=%v close=%v", rt.Backend, method, path, response.StatusCode, readErr, closeErr)
				}
			}
		}
	}
	if after := counts(); after != before {
		t.Fatalf("%s retired routes mutated domain state: %v -> %v", rt.Backend, before, after)
	}
}

func TestRetiredDashboardTransportCannotReturn(t *testing.T) {
	if err := checkRetiredDashboardSource(repoRootForTest()); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "list", "-deps", "./cmd/swarm")
	cmd.Dir = repoRootForTest()
	deps, err := cmd.Output()
	if err != nil {
		t.Fatalf("production dependency graph: %v", err)
	}
	if strings.Contains(string(deps), "/internal/dashboard") {
		t.Fatalf("retired package in production dependency graph: %s", deps)
	}
	var hits atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusAccepted)
	})
	for _, isReady := range []bool{false, true} {
		var ready atomic.Bool
		ready.Store(isReady)
		mux := newAPIServer(&ready, handler, handler).Handler
		for _, path := range retiredDashboardPaths {
			for _, transport := range []string{"GET", "POST", "upgrade"} {
				for _, auth := range []string{"", "Bearer invalid", "Bearer configured", "Bearer retired"} {
					method := transport
					if transport == "upgrade" {
						method = http.MethodGet
					}
					req := httptest.NewRequest(method, path, strings.NewReader(`{"action":"reset_state"}`))
					req.Header.Set("Authorization", auth)
					if transport == "upgrade" {
						req.Header.Set("Connection", "Upgrade")
						req.Header.Set("Upgrade", "websocket")
					}
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusNotFound || hits.Load() != 0 {
						t.Fatalf("ready=%t %s %s auth=%q: status=%d downstream hits=%d", isReady, transport, path, auth, rec.Code, hits.Load())
					}
				}
			}
		}
	}
}

func checkRetiredDashboardSource(root string) error {
	path := filepath.Join(root, "internal", "dashboard")
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("retired dashboard package path exists: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, sourceRoot := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, sourceRoot), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imp := range file.Imports {
				value, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					return err
				}
				if value == "github.com/division-sh/swarm/internal/dashboard" || strings.HasPrefix(value, "github.com/division-sh/swarm/internal/dashboard/") {
					return fmt.Errorf("retired dashboard import in %s: %s", path, value)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func TestRetiredDashboardBoundaryRejectsReintroduction(t *testing.T) {
	for _, cell := range []struct{ name, path, source string }{
		{"package", "internal/dashboard/server/server.go", "package server\n"},
		{"production-import", "cmd/main.go", "package main\nimport _ \"github.com/division-sh/swarm/internal/dashboard/server\"\n"},
		{"test-import", "internal/probe_test.go", "package probe\nimport _ \"github.com/division-sh/swarm/internal/dashboard/server\"\n"},
		{"invalid-source", "internal/probe.go", "not valid Go\n"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			root := t.TempDir()
			for _, dir := range []string{"cmd", "internal", filepath.Dir(cell.path)} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if err := checkRetiredDashboardSource(root); err != nil && cell.name != "package" {
				t.Fatalf("empty control: %v", err)
			}
			if err := os.WriteFile(filepath.Join(root, cell.path), []byte(cell.source), 0600); err != nil {
				t.Fatal(err)
			}
			if err := checkRetiredDashboardSource(root); err == nil {
				t.Fatal("retirement boundary admitted reintroduction")
			}
		})
	}
}
