package releasee2e

import (
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
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

func TestRequestBindingCompiledRetryBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store compiled request retry", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	lifecycle := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			selected := goldenSQLiteStore(root)
			if backend == "postgres" {
				selected = goldenPostgresStore(t, dsn)
			}
			project := filepath.Join(root, "project")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), filepath.Join(project, "contracts"))
			raw, err := os.ReadFile(filepath.Join(project, "contracts/data/items.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			rows := strings.Split(strings.TrimSpace(string(raw)), "\n")
			writeReleaseFile(t, filepath.Join(project, "input.jsonl"), rows[0]+"\n")
			writeReleaseFile(t, filepath.Join(project, "different.jsonl"), rows[1]+"\n")
			writeReleaseFile(t, filepath.Join(project, ".swarm/swarm.yaml"), goldenRuntimeConfig(selected))
			writeReleaseFile(t, filepath.Join(project, "api-token"), goldenAPIToken+"\n")
			writeReleaseFile(t, filepath.Join(root, "home/.config/swarm/swarm.yaml"), "connection:\n  api_token_file: api-token\n")
			env := goldenProcessEnv(t, root, selected.passwordEnv, 0)
			p := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, InternalMockLifecycleBinary: lifecycle,
				WorkingDir: project, Source: "contracts", ConfigPath: ".swarm/swarm.yaml", Store: backend,
				TokenFile: "api-token", Token: goldenAPIToken, Env: env, ShutdownGrace: goldenShutdownGrace})
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			if err := p.waitReady(ctx); err != nil {
				t.Fatal(err)
			}
			hash := goldenServedBundleHash(t, p.rpc, "mock_only")
			start := func(connect, runID string, operands ...string) releaseCommandResult {
				args := []string{"run", "start", "--connect", connect, "--run-id", runID,
					"--idempotency-key", "binding-" + runID, "--no-follow"}
				return runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary, append(args, operands...)...)
			}
			requireCreated := func(result releaseCommandResult, id string) {
				if result.err != nil || !strings.Contains(result.output, "run_id="+id) {
					t.Fatalf("compiled request creation/retry: %v\n%s", result.err, result.output)
				}
			}
			importID, pinID := uuid.NewString(), uuid.NewString()
			requireCreated(start(p.apiBase, importID, "--data", "item.registered=input.jsonl"), importID)
			requireCreated(start(p.apiBase, pinID, "--pin", "item.registered@head"), pinID)
			refs := []map[string]any{receiptRestartRef("run_creation", importID), receiptRestartRef("run_creation", pinID)}
			before := captureReceiptRestartOperations(t, ctx, p.rpc, refs)
			var head receiptRestartVersionSummary
			if err := p.rpc.call(ctx, "data.show", map[string]any{"view": "version", "declaration": receiptRestartDeclaration(),
				"selector": map[string]any{"kind": "head"}}, &head); err != nil {
				t.Fatal(err)
			}
			advanced := receiptRestartSource(t, ctx, p.rpc, hash, "import", receiptRestartHead{State: "version", VersionID: head.VersionID}, rows[2]+"\n", "accepted")
			if advanced.Candidate.VersionID == head.VersionID {
				t.Fatal("head advancement control did not change the version")
			}
			requireCreated(start(p.apiBase, importID, "--data", "item.registered=input.jsonl"), importID)
			requireCreated(start(p.apiBase, pinID, "--pin", "item.registered@head"), pinID)
			for _, test := range []struct {
				name string
				id   string
				args []string
				want string
			}{
				{"intent", importID, []string{"--data", "item.registered=input.jsonl", "--event", "item.registered"}, "event initiation contradicts its permanent request"},
				{"payload", importID, []string{"--data", "item.registered=different.jsonl"}, "conflict"},
				{"explicit_head", pinID, []string{"--pin", "item.registered@" + advanced.Candidate.VersionID}, "explicit pin 0 contradicts its permanent request"},
			} {
				t.Run(test.name, func(t *testing.T) {
					result := start(p.apiBase, test.id, test.args...)
					if result.err == nil || !strings.Contains(strings.ToLower(result.output), test.want) {
						t.Fatalf("compiled %s refusal: %v\n%s", test.name, result.err, result.output)
					}
				})
			}
			// Forward the real host unchanged except for a transport failure at the
			// binding read. No successful RPC result or store fact is synthesized.
			target, err := url.Parse(p.apiBase)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			var refusedReads, forwardedStarts atomic.Int32
			fault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "fixture request read failed", http.StatusInternalServerError)
					return
				}
				_ = r.Body.Close()
				r.Body = io.NopCloser(strings.NewReader(string(raw)))
				var request struct {
					Method string `json:"method"`
					Params struct {
						Detail string `json:"detail"`
					} `json:"params"`
				}
				if json.Unmarshal(raw, &request) == nil {
					if request.Method == "data.show" && request.Params.Detail == "request_binding" {
						refusedReads.Add(1)
						http.Error(w, "binding read transport unavailable", http.StatusServiceUnavailable)
						return
					}
					if request.Method == "run.start" {
						forwardedStarts.Add(1)
					}
				}
				proxy.ServeHTTP(w, r)
			}))
			defer fault.Close()
			refused := start(fault.URL, importID, "--data", "item.registered=input.jsonl")
			if refused.err == nil || refusedReads.Load() != 1 || forwardedStarts.Load() != 0 {
				t.Fatalf("non-absence read failed open: reads=%d starts=%d err=%v\n%s", refusedReads.Load(), forwardedStarts.Load(), refused.err, refused.output)
			}
			if after := captureReceiptRestartOperations(t, ctx, p.rpc, refs); !reflect.DeepEqual(before, after) {
				t.Fatalf("retry/refusal changed permanent receipts: before=%s after=%s", before, after)
			}
			var current receiptRestartVersionSummary
			if err := p.rpc.call(ctx, "data.show", map[string]any{"view": "version", "declaration": receiptRestartDeclaration(),
				"selector": map[string]any{"kind": "head"}}, &current); err != nil || current.VersionID != advanced.Candidate.VersionID {
				t.Fatalf("reconstruction/refusal changed current head: %+v, %v", current, err)
			}
			if err := p.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("compiled retry host shutdown: %v\n%s", err, p.output.String())
			}
		})
	}
}
