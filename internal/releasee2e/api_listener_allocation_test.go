package releasee2e

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReleaseAPIListenerEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, output, want string
		invalid            bool
	}{
		{"missing", "other boot evidence\n", "", false},
		{"partial", "api_listener=127.0.0.1:123", "", false},
		{"boot", "boot http_listener_bind api_listener=127.0.0.1:1234 api_routes=/readyz\n", "127.0.0.1:1234", false},
		{"summary", "listeners api 127.0.0.1:1234 (routes)\n", "127.0.0.1:1234", false},
		{"consistent", "api_listener=127.0.0.1:1234\nlisteners api 127.0.0.1:1234\n", "127.0.0.1:1234", false},
		{"conflicting", "api_listener=127.0.0.1:1234\napi_listener=127.0.0.1:1235\n", "", true},
		{"zero", "api_listener=127.0.0.1:0\n", "", true},
		{"non_loopback", "api_listener=0.0.0.0:1234\n", "", true},
		{"hostname", "api_listener=localhost:1234\n", "", true},
		{"malformed", "api_listener=nope\n", "", true},
		{"empty", "api_listener=\n", "", true},
		{"ipv6", "api_listener=[::1]:1234\n", "[::1]:1234", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := releaseAPIListener(tc.output)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("listener = %q, %v; want %q invalid=%v", got, err, tc.want, tc.invalid)
			}
		})
	}
}

func TestReleaseEndpointHandoffConsumerCensus(t *testing.T) {
	// These APIs require explicit nonzero/fixed addresses. They do not consume
	// the serve harness's ephemeral endpoint contract. Keep this list exact.
	allowed := map[string]string{
		"TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults":     "foreground --api-port and default MCP contract",
		"TestClaudeCLIPaidAgenticLifecycleFromReleaseBinaryDefaults": "optional live foreground --api-port/default MCP contract",
		"TestRunStartForegroundObserverOverflowFromReleaseBinary":    "foreground --api-port with blocked stdout observer",
		"TestChannelOnboardingReleaseBinaryJourneys":                 "externally registered fixed public webhook callback",
	}
	files, err := filepath.Glob(filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok || name.Name != "freeReleaseTCPPort" {
					return true
				}
				seen[fn.Name.Name]++
				if allowed[fn.Name.Name] == "" {
					t.Errorf("unowned endpoint handoff in %s", fn.Name.Name)
				}
				return true
			})
		}
	}
	for name := range allowed {
		if seen[name] != 1 {
			t.Errorf("fixed-contract census %s = %d calls, want 1", name, seen[name])
		}
	}
}

func TestReleaseReadinessRequiresCurrentChildEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	for _, stage := range []string{"before_publication", "after_publication", "missing", "valid"} {
		t.Run(stage, func(t *testing.T) {
			p := &releaseServeProcess{output: &releaseProcessOutput{}, exited: make(chan struct{}), rpc: &releaseRPCClient{}, apiBase: server.URL}
			if stage == "after_publication" || stage == "valid" {
				p.output.Write([]byte("api_listener=" + strings.TrimPrefix(server.URL, "http://") + "\n"))
			}
			if stage == "before_publication" || stage == "after_publication" {
				close(p.exited)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			err := p.waitReady(ctx)
			if (err == nil) != (stage == "valid") {
				t.Fatalf("readiness = %v", err)
			}
			if stage != "valid" && p.rpc.endpoint != "" {
				t.Fatal("unready child exposed RPC")
			}
			if stage == "valid" && p.rpc.endpoint != server.URL+"/v1/rpc" {
				t.Fatal("wrong child endpoint")
			}
		})
	}
}

func requireReleasedAPIListener(t *testing.T, base string) {
	t.Helper()
	listener, err := net.Listen("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatalf("joined child retained socket %s: %v", base, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseServeOwnsAPIEndpointAcrossChildrenAndRestart(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	makeSpec := func(name string) releaseProcessSpec {
		project := filepath.Join(root, name)
		source := filepath.Join(project, "bundle")
		copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/node_identity"), source)
		store := goldenSQLiteStore(project)
		config, token := filepath.Join(project, "swarm.yaml"), filepath.Join(project, "api-token")
		writeReleaseFile(t, config, goldenRuntimeConfig(store))
		writeReleaseFile(t, token, goldenAPIToken+"\n")
		return releaseProcessSpec{BinaryPath: binary, WorkingDir: project, ConfigPath: config, Source: source, Store: "sqlite", TokenFile: token, Token: goldenAPIToken, Env: goldenProcessEnv(t, project, store.passwordEnv, 0)}
	}
	firstSpec, secondSpec := makeSpec("first"), makeSpec("second")
	// A competing listener owns the number the old parent handoff would pass.
	// An explicitly fixed address must still fail rather than select another.
	negativeSpec := firstSpec
	negativeSpec.APIPort = occupied.Addr().(*net.TCPAddr).Port
	negative := startReleaseServe(t, negativeSpec)
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	if err := negative.waitReady(ctx); err == nil || !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("fixed occupied API must fail: %v", err)
	}
	first, second := startReleaseServe(t, firstSpec), startReleaseServe(t, secondSpec)
	for _, child := range []*releaseServeProcess{first, second} {
		if child.apiBase != "" || child.rpc.endpoint != "" {
			t.Fatal("client exposed before readiness")
		}
		ready, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
		err := child.waitReady(ready)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		goldenServedBundleHash(t, child.rpc, "live")
		request, err := http.NewRequest(http.MethodPost, child.rpc.endpoint, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"health.check","params":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer wrong-token")
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong auth accepted: %d", response.StatusCode)
		}
	}
	if first.apiBase == second.apiBase || first.apiBase == "http://"+occupied.Addr().String() || second.apiBase == "http://"+occupied.Addr().String() {
		t.Fatal("children did not own distinct ephemeral endpoints")
	}
	initialHash := goldenServedBundleHash(t, first.rpc, "live")
	if err := first.stopAndWait(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	// Keep the old endpoint occupied across restart: only the retained store and
	// credentials may be reused, never the stopped child's client address.
	old, err := net.Listen("tcp", strings.TrimPrefix(first.apiBase, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	restarted := startReleaseServe(t, firstSpec)
	ready, readyCancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer readyCancel()
	if err := restarted.waitReady(ready); err != nil {
		t.Fatal(err)
	}
	if restarted.apiBase == first.apiBase || restarted.apiBase == second.apiBase {
		t.Fatal("restart reused another listener's endpoint")
	}
	if got := goldenServedBundleHash(t, restarted.rpc, "live"); got != initialHash {
		t.Fatalf("retained bundle changed: %s -> %s", initialHash, got)
	}
	for _, child := range []*releaseServeProcess{second, restarted} {
		if err := child.stopAndWait(10 * time.Second); err != nil {
			t.Fatal(err)
		}
		requireReleasedAPIListener(t, child.apiBase)
	}
}
