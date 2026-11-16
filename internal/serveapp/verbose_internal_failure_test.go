package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
)

type verboseFailureRunReader struct{ apiv1.RunReadStore }

func (r verboseFailureRunReader) LoadRunHeader(context.Context, string) (operatorread.RunHeader, error) {
	return operatorread.RunHeader{}, errors.Join(fmt.Errorf("wrapped: %w", errors.New("synthetic-chain-secret")), errors.New("joined branch"))
}

func proveServeVerboseInternalFailure(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var ordinaryResponse string
			for _, verbose := range []bool{false, true} {
				t.Run(fmt.Sprintf("verbose=%t", verbose), func(t *testing.T) {
					prior := buildSelectedAPICapabilities
					buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
						caps, err := prior(owner, req)
						caps.Runs = verboseFailureRunReader{caps.Runs}
						return caps, err
					}
					t.Cleanup(func() { buildSelectedAPICapabilities = prior })
					if backend == "postgres" {
						configureCompletion2270PostgresFixture(t)
						stubServeRuntimeWorkspaceLifecycle(t)
					}
					var diagnostics lockedBuffer
					previousLog := log.Writer()
					log.SetOutput(&diagnostics)
					t.Cleanup(func() { log.SetOutput(previousLog) })
					serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
						ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "diagnostics.sqlite"), nil),
						SourceRoot: filepath.Join("tests", "tier8-boot-verification", "test-boot-success"), StoreMode: backend, StoreModeSet: true,
						APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: verbose,
					})
					serve.waitForReadyLine()
					endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, serve.outputString()) + "/v1/rpc"
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(`{"jsonrpc":"2.0","id":"verbose-wire","method":"run.get","params":{"run_id":"00000000-0000-4000-8000-000000000001"}}`))
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
					req.Header.Set("X-Correlation-ID", "verbose-wire")
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					raw, readErr := io.ReadAll(resp.Body)
					closeErr := resp.Body.Close()
					if readErr != nil || closeErr != nil || resp.StatusCode != http.StatusOK {
						t.Fatalf("RPC read=%v close=%v status=%d", readErr, closeErr, resp.StatusCode)
					}
					if !strings.Contains(string(raw), "unclassified_runtime_error") || strings.Contains(string(raw), "synthetic-chain-secret") {
						t.Fatalf("caller failure changed: %s", raw)
					}
					if verbose && string(raw) != ordinaryResponse {
						t.Fatalf("verbose changed caller bytes: %s != %s", raw, ordinaryResponse)
					}
					ordinaryResponse = string(raw)
					if code := serve.stop(); code != 0 {
						t.Fatalf("serve shutdown=%d: %s", code, serve.outputString())
					}
					var records []map[string]json.RawMessage
					for _, line := range strings.Split(diagnostics.String(), "\n") {
						if strings.Contains(line, `"correlation_id":"verbose-wire"`) {
							_, detail, _ := strings.Cut(line, "detail=")
							var record map[string]json.RawMessage
							if err := json.Unmarshal([]byte(detail), &record); err != nil {
								t.Fatal(err)
							}
							records = append(records, record)
						}
					}
					if len(records) != 1 {
						t.Fatalf("API failure records=%d, want one", len(records))
					}
					_, present := records[0]["error_diagnostics"]
					if present != verbose {
						t.Fatalf("verbose=%t diagnostic presence=%t", verbose, present)
					}
					if verbose && (!strings.Contains(string(records[0]["error_diagnostics"]), "synthetic-chain-secret") || !strings.Contains(string(records[0]["error_diagnostics"]), "(*Handler).logInternalFallback")) {
						t.Fatalf("missing full message or reporting stack: %s", records[0]["error_diagnostics"])
					}
					if !verbose && strings.Contains(diagnostics.String(), "synthetic-chain-secret") {
						t.Fatal("ordinary process log exposed the cause")
					}
				})
			}
		})
	}
}
