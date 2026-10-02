package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func Test2376EventPublishDerivesExactRuntimeSource(t *testing.T) {
	for _, condition := range []string{"new", "existing", "invalid health"} {
		t.Run(condition, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			var methods []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				methods = append(methods, req.Method)
				if req.Method == "health.check" {
					if condition == "invalid health" {
						writeJSONRPCResult(t, w, req.ID, map[string]any{})
						return
					}
					writeJSONRPCResult(t, w, req.ID, map[string]any{"alive": true, "ready": true, "db_ok": true, "runtime_ok": true, "bundle": map[string]any{"bundle_hash": hash, "workflow_name": ".", "workflow_version": hash}})
					return
				}
				if req.Method != eventPublishMethod {
					t.Error(req.Method)
					return
				}
				if condition == "existing" {
					if _, present := req.Params["bundle_hash"]; present {
						t.Error("existing run rebound to health source")
					}
				} else if req.Params["bundle_hash"] != hash {
					t.Errorf("new work lacked exact runtime source: %v", req.Params)
				}
				writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(condition != "existing"))
			}))
			defer server.Close()
			args := []string{"event", "publish", "work.requested", "--payload-json", `{}`}
			if condition == "existing" {
				args = append(args, "--run-id", testPublishedRunID)
			}
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, testRootCommandOptions(server))
			want := "health.check,event.publish"
			if condition == "existing" {
				want = "event.publish"
			}
			if condition == "invalid health" {
				want = "health.check"
			}
			if strings.Join(methods, ",") != want || (code != 0) != (condition == "invalid health") {
				t.Fatalf("code=%d methods=%v %s", code, methods, &errOut)
			}
		})
	}
}
