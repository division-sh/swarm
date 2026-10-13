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

func TestForkCommandRunStartRequestAndReadback(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	const sourceRunID = "11111111-1111-1111-1111-111111111111"
	for _, jsonOutput := range []bool{false, true} {
		name := "human"
		if jsonOutput {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			var captured jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Errorf("decode request: %v", err)
				}
				result := validRunForkResult(sourceRunID, validBundleHash("a"))
				result["fork_point_kind"], result["fork_revision"] = "run_start", 7
				delete(result, "fork_event_id")
				writeJSONRPCResult(t, w, captured.ID, result)
			}))
			defer server.Close()
			args := []string{"run", "fork", sourceRunID, "--at-start", "--allow-source-freeze"}
			if jsonOutput {
				args = append(args, "--json")
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 || stderr.Len() != 0 {
				t.Fatalf("start command code=%d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
			}
			assertForkRequest(t, captured, map[string]any{"source_run_id": sourceRunID, "at_start": true, "allow_source_freeze": true})
			if jsonOutput {
				var result map[string]any
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result["fork_point_kind"] != "run_start" || result["fork_revision"] != float64(7) {
					t.Fatalf("start JSON result=%s, error %v", stdout.String(), err)
				}
				if _, exists := result["fork_event_id"]; exists {
					t.Fatal("start JSON invented an event coordinate")
				}
			} else if !strings.Contains(stdout.String(), "fork_point=run_start@7") || strings.Contains(stdout.String(), "fork_event_id=") {
				t.Fatalf("start human readback = %s", stdout.String())
			}
		})
	}
}

func TestForkCommandExplicitFalseStartPreservesDefaultSelector(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	const sourceRunID = "11111111-1111-1111-1111-111111111111"
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validRunForkResult(sourceRunID, validBundleHash("a")))
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "fork", sourceRunID, "--at-start=false", "--allow-source-freeze"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("default command code=%d stderr=%s", code, stderr.String())
	}
	assertForkRequest(t, captured, map[string]any{"source_run_id": sourceRunID, "allow_source_freeze": true})
}
