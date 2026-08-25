package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	operatorChannelCLISelector  = "provider.telegram.hitl_channel@operator-channel-cli-generation"
	operatorChannelCLIToken     = "operator-channel-cli-token"
	operatorChannelCLIOperation = "00000000-0000-4000-8000-000000000224"
	operatorChannelCLIChallenge = "SWARM-AAAAAAAAAAAAAAAA"
)

func TestOperatorChannelCLIUsesAuthenticatedAPIAndExactSelectors(t *testing.T) {
	t.Run("list output modes", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			if request.Method != "channel.list" {
				t.Fatalf("method = %q, want channel.list", request.Method)
			}
			return operatorChannelCLIListResult("current", nil)
		})
		for _, test := range []struct {
			name  string
			args  []string
			check func(*testing.T, string)
		}{
			{name: "human", args: []string{"channel", "list"}, check: func(t *testing.T, output string) {
				for _, want := range []string{"provider.telegram.hitl_channel", "current", "@m***o", operatorChannelCLISelector} {
					if !strings.Contains(output, want) {
						t.Fatalf("human output missing %q:\n%s", want, output)
					}
				}
			}},
			{name: "json", args: []string{"channel", "list", "--json"}, check: func(t *testing.T, output string) {
				var result channelListResult
				if err := json.Unmarshal([]byte(output), &result); err != nil {
					t.Fatal(err)
				}
				if result.PrincipalID != "principal-1" || len(result.Channels) != 1 || result.Channels[0].Interface.Selector != operatorChannelCLISelector {
					t.Fatalf("JSON output = %#v", result)
				}
			}},
			{name: "quiet", args: []string{"channel", "list", "--quiet"}, check: func(t *testing.T, output string) {
				if output != operatorChannelCLISelector+"\n" {
					t.Fatalf("quiet output = %q", output)
				}
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				stdout, stderr, code := runOperatorChannelCLI(t, server, test.args...)
				if code != 0 || strings.TrimSpace(stderr) != "" {
					t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				test.check(t, stdout)
			})
		}
	})

	t.Run("connect waits and confirms", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			methods = append(methods, request.Method)
			switch request.Method {
			case "channel.list":
				if call == 0 {
					return operatorChannelCLIListResult("unbound", nil)
				}
				pending := operatorChannelCLIOperationResult("awaiting_confirmation", 2, 0)
				pending["account_presentation"] = "41********90"
				pending["external_account_reference"] = "41********90"
				return operatorChannelCLIListResult("unbound", pending)
			case "channel.connect":
				if request.Params["interface"] != operatorChannelCLISelector || request.Params["expected_revision"] != float64(0) || request.Params["save_proof"] != true {
					t.Fatalf("channel.connect params = %#v", request.Params)
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("awaiting_claim", 1, 0)}
			case "channel.confirm":
				if request.Params["operation_id"] != operatorChannelCLIOperation || request.Params["expected_revision"] != float64(2) || request.Params["approve"] != true {
					t.Fatalf("channel.confirm params = %#v", request.Params)
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLI(t, server, "channel", "connect", operatorChannelCLISelector, "--yes")
		if code != 0 || strings.TrimSpace(stderr) != "" {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		for _, want := range []string{operatorChannelCLIChallenge, "Waiting for an authenticated claimant", "Claimed by 41********90 in a shared conversation", "Connected provider.telegram.hitl_channel"} {
			if !strings.Contains(stdout, want) {
				t.Fatalf("connect output missing %q:\n%s", want, stdout)
			}
		}
		wantMethods := []string{"channel.list", "channel.connect", "channel.list", "channel.confirm"}
		if strings.Join(methods, ",") != strings.Join(wantMethods, ",") {
			t.Fatalf("methods = %v, want %v", methods, wantMethods)
		}
	})

	t.Run("no-save remains explicit through confirmation", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			switch request.Method {
			case "channel.list":
				if call == 0 {
					return operatorChannelCLIListResult("unbound", nil)
				}
				return operatorChannelCLIListResult("unbound", operatorChannelCLIOperationResult("awaiting_confirmation", 2, 0))
			case "channel.connect":
				if request.Params["save_proof"] != false {
					t.Fatalf("channel.connect save_proof = %#v, want false", request.Params["save_proof"])
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("awaiting_claim", 1, 0)}
			case "channel.confirm":
				return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLI(t, server, "channel", "connect", operatorChannelCLISelector, "--no-save", "--yes")
		if code != 0 || strings.TrimSpace(stderr) != "" || !strings.Contains(stdout, "Connected provider.telegram.hitl_channel") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	})

	t.Run("interactive rejection remains terminal and unapproved", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			switch request.Method {
			case "channel.list":
				if call == 0 {
					return operatorChannelCLIListResult("unbound", nil)
				}
				return operatorChannelCLIListResult("unbound", operatorChannelCLIOperationResult("awaiting_confirmation", 2, 0))
			case "channel.connect":
				return map[string]any{"operation": operatorChannelCLIOperationResult("awaiting_claim", 1, 0)}
			case "channel.confirm":
				if request.Params["approve"] != false {
					t.Fatalf("channel.confirm approve = %#v, want false", request.Params["approve"])
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("rejected", 3, 0), "binding": map[string]any{}}
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		setCLIAPITestToken(t, operatorChannelCLIToken)
		opts := testRootCommandOptions(server)
		opts.channelConnectWait = time.Second
		opts.channelConnectPoll = time.Millisecond
		opts.input = strings.NewReader("n\n")
		opts.stdinIsTerminal = func() bool { return true }
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"channel", "connect", operatorChannelCLISelector}, &stdout, &stderr, opts)
		if code != 0 || !strings.Contains(stdout.String(), "rejected") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	})

	t.Run("short selector fails before mutation", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			methods = append(methods, request.Method)
			return operatorChannelCLIListResult("unbound", nil)
		})
		_, stderr, code := runOperatorChannelCLI(t, server, "channel", "connect", "provider.telegram.hitl_channel", "--yes")
		if code != CLIExitValidation || !strings.Contains(stderr, "is not active") {
			t.Fatalf("code=%d stderr=%q", code, stderr)
		}
		if strings.Join(methods, ",") != "channel.list" {
			t.Fatalf("methods = %v, want readback only", methods)
		}
	})

	t.Run("unbind uses exact revision", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			methods = append(methods, request.Method)
			switch request.Method {
			case "channel.list":
				return operatorChannelCLIListResult("current", nil)
			case "channel.unbind":
				if request.Params["interface"] != operatorChannelCLISelector || request.Params["expected_revision"] != float64(1) {
					t.Fatalf("channel.unbind params = %#v", request.Params)
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("unbound", 1, 2), "binding": map[string]any{"revision": 2}}
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLI(t, server, "channel", "unbind", operatorChannelCLISelector)
		if code != 0 || strings.TrimSpace(stderr) != "" || !strings.Contains(stdout, "The machine proof was not revoked") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		if strings.Join(methods, ",") != "channel.list,channel.unbind" {
			t.Fatalf("methods = %v", methods)
		}
	})
}

func newOperatorChannelCLIServer(t *testing.T, respond func(*testing.T, jsonRPCRequest, int) map[string]any) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" || r.Header.Get("Authorization") != "Bearer "+operatorChannelCLIToken {
			t.Errorf("request path/auth = %q/%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		index := call
		call++
		result := respond(t, request, index)
		mu.Unlock()
		writeJSONRPCResult(t, w, request.ID, result)
	}))
	t.Cleanup(server.Close)
	return server
}

func runOperatorChannelCLI(t *testing.T, server *httptest.Server, args ...string) (string, string, int) {
	t.Helper()
	setCLIAPITestToken(t, operatorChannelCLIToken)
	opts := testRootCommandOptions(server)
	opts.channelConnectWait = time.Second
	opts.channelConnectPoll = time.Millisecond
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
	return stdout.String(), stderr.String(), code
}

func operatorChannelCLIListResult(status string, pending map[string]any) map[string]any {
	row := map[string]any{
		"interface": map[string]any{
			"interface_ref": "swarm.hitl-channel/v2", "channel_pack_id": "provider.telegram.hitl_channel",
			"channel_pack_version": "1.0.0", "channel_manifest_hash": "sha256:" + strings.Repeat("a", 64),
			"semantic_generation": "operator-channel-cli-generation", "selector": operatorChannelCLISelector,
		},
		"status": status, "binding_revision": 1, "account_presentation": "@m***o", "conversation_scope": "shared",
	}
	if status == "unbound" {
		row["binding_revision"] = 0
		row["account_presentation"] = ""
		row["conversation_scope"] = ""
	}
	if pending != nil {
		row["pending_operation"] = pending
	}
	return map[string]any{"principal_id": "principal-1", "channels": []any{row}}
}

func operatorChannelCLIOperationResult(state string, revision, bindingRevision int64) map[string]any {
	return map[string]any{
		"operation_id": operatorChannelCLIOperation, "kind": "connect",
		"interface": map[string]any{
			"interface_ref": "swarm.hitl-channel/v2", "channel_pack_id": "provider.telegram.hitl_channel",
			"channel_pack_version": "1.0.0", "channel_manifest_hash": "sha256:" + strings.Repeat("a", 64),
			"semantic_generation": "operator-channel-cli-generation", "selector": operatorChannelCLISelector,
		},
		"challenge": operatorChannelCLIChallenge, "state": state, "revision": revision, "binding_revision": bindingRevision,
		"account_presentation": "@m***o", "conversation_scope": "shared", "expires_at": "2026-08-24T23:59:59Z",
	}
}
