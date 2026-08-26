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
				if result.PrincipalID != "principal-1" || len(result.Channels) != 1 || result.Channels[0].Identity.Interface.Selector != operatorChannelCLISelector {
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

	t.Run("connect uses durable onboarding and confirms nested identity", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			methods = append(methods, request.Method)
			switch request.Method {
			case "channel.onboarding_start":
				if request.Params["provider"] != "telegram" || request.Params["verb"] != "connect" || request.Params["save_proof"] != true || request.Params["provider_credential"] != "bot-token" {
					t.Fatalf("channel.onboarding_start params = %#v", request.Params)
				}
				return channelOnboardingCLIResult("awaiting_external_identity", "awaiting_claim", false)
			case "channel.onboarding_get":
				return channelOnboardingCLIResult("awaiting_operator_confirmation", "awaiting_confirmation", false)
			case "channel.confirm":
				if request.Params["operation_id"] != operatorChannelCLIOperation || request.Params["expected_revision"] != float64(2) || request.Params["approve"] != true {
					t.Fatalf("channel.confirm params = %#v", request.Params)
				}
				return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
			case "channel.onboarding_retry":
				return channelOnboardingCLIResult("succeeded", "bound", true)
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLIWithInput(t, server, "bot-token\n", "channel", "connect", "telegram", "--yes")
		if code != 0 || strings.TrimSpace(stderr) != "" {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		for _, want := range []string{operatorChannelCLIChallenge, "Waiting for an authenticated claimant", "Claimed by @m***o in a shared conversation", "Connected telegram channel READY"} {
			if !strings.Contains(stdout, want) {
				t.Fatalf("connect output missing %q:\n%s", want, stdout)
			}
		}
		wantMethods := []string{"channel.onboarding_start", "channel.onboarding_get", "channel.confirm", "channel.onboarding_retry"}
		if strings.Join(methods, ",") != strings.Join(wantMethods, ",") {
			t.Fatalf("methods = %v, want %v", methods, wantMethods)
		}
	})

	t.Run("no-save remains explicit on connect", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			switch request.Method {
			case "channel.onboarding_start":
				if request.Params["save_proof"] != false {
					t.Fatalf("channel.onboarding_start save_proof = %#v, want false", request.Params["save_proof"])
				}
				return channelOnboardingCLIResult("awaiting_external_identity", "awaiting_claim", false)
			case "channel.onboarding_get":
				return channelOnboardingCLIResult("awaiting_operator_confirmation", "awaiting_confirmation", false)
			case "channel.confirm":
				return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
			case "channel.onboarding_retry":
				return channelOnboardingCLIResult("succeeded", "bound", true)
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLIWithInput(t, server, "bot-token\n", "channel", "connect", "telegram", "--no-save", "--yes")
		if code != 0 || strings.TrimSpace(stderr) != "" || !strings.Contains(stdout, "Connected telegram channel READY") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	})

	t.Run("explicit rebind rejection remains unapproved", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, call int) map[string]any {
			switch request.Method {
			case "channel.onboarding_start":
				return channelOnboardingCLIResult("awaiting_operator_confirmation", "awaiting_confirmation", false)
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
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"channel", "rebind", "telegram"}, &stdout, &stderr, opts)
		if code != CLIExitValidation || !strings.Contains(stderr.String(), "rejected") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	})

	t.Run("exact selectors are forwarded without lower identity lookup", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			methods = append(methods, request.Method)
			if request.Params["bundle"] != "bundle-v1:sha256:"+strings.Repeat("b", 64) || request.Params["interface"] != operatorChannelCLISelector || request.Params["target"] != "ingress:support:telegram:telegram" {
				t.Fatalf("exact selectors = %#v", request.Params)
			}
			return channelOnboardingCLIResult("succeeded", "bound", true)
		})
		_, stderr, code := runOperatorChannelCLI(t, server, "channel", "reconnect", "telegram", "--bundle", "bundle-v1:sha256:"+strings.Repeat("b", 64), "--interface", operatorChannelCLISelector, "--target", "ingress:support:telegram:telegram")
		if code != 0 || stderr != "" {
			t.Fatalf("code=%d stderr=%q", code, stderr)
		}
		if strings.Join(methods, ",") != "channel.onboarding_start" {
			t.Fatalf("methods = %v", methods)
		}
	})

	t.Run("reconnect admits an explicit replacement credential", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			if request.Method != "channel.onboarding_start" || request.Params["verb"] != "reconnect" || request.Params["provider_credential"] != "replacement-token" {
				t.Fatalf("replacement start = %s %#v", request.Method, request.Params)
			}
			return channelOnboardingCLIResult("succeeded", "bound", true)
		})
		stdout, stderr, code := runOperatorChannelCLIWithInput(t, server, "replacement-token\n", "channel", "reconnect", "telegram", "--credential-stdin")
		if code != 0 || stderr != "" || !strings.Contains(stdout, "Connected telegram channel READY") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	})

	t.Run("resume continues one exact durable operation", func(t *testing.T) {
		var methods []string
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			methods = append(methods, request.Method)
			switch request.Method {
			case "channel.onboarding_get":
				if request.Params["operation_id"] != operatorChannelCLIOperation {
					t.Fatalf("resume operation = %#v", request.Params)
				}
				return channelOnboardingCLIResult("awaiting_operator_confirmation", "awaiting_confirmation", false)
			case "channel.confirm":
				return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
			case "channel.onboarding_retry":
				return channelOnboardingCLIResult("succeeded", "bound", true)
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLI(t, server, "channel", "resume", operatorChannelCLIOperation, "--yes")
		if code != 0 || stderr != "" || !strings.Contains(stdout, "Connected telegram channel READY") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		if got := strings.Join(methods, ","); got != "channel.onboarding_get,channel.confirm,channel.onboarding_retry" {
			t.Fatalf("resume methods = %s", got)
		}
	})

	t.Run("resume can supply a missing operation credential", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			switch request.Method {
			case "channel.onboarding_get":
				return channelOnboardingCLIResult("preparing", "", false)
			case "channel.onboarding_retry":
				if request.Params["provider_credential"] != "replacement-token" {
					t.Fatalf("resume credential params = %#v", request.Params)
				}
				return channelOnboardingCLIResult("succeeded", "bound", true)
			default:
				t.Fatalf("unexpected method %q", request.Method)
			}
			return nil
		})
		stdout, stderr, code := runOperatorChannelCLIWithInput(t, server, "replacement-token\n", "channel", "resume", operatorChannelCLIOperation, "--yes", "--credential-stdin")
		if code != 0 || stderr != "" || !strings.Contains(stdout, "Connected telegram channel READY") {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	})

	t.Run("legacy replace flag is rejected", func(t *testing.T) {
		server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
			t.Fatalf("legacy flag reached API method %q", request.Method)
			return nil
		})
		_, stderr, code := runOperatorChannelCLIWithInput(t, server, "bot-token\n", "channel", "connect", "telegram", "--replace", "--yes")
		if code == 0 || !strings.Contains(stderr, "unknown flag: --replace") {
			t.Fatalf("code=%d stderr=%q", code, stderr)
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
	return runOperatorChannelCLIWithInput(t, server, "", args...)
}

func runOperatorChannelCLIWithInput(t *testing.T, server *httptest.Server, input string, args ...string) (string, string, int) {
	t.Helper()
	setCLIAPITestToken(t, operatorChannelCLIToken)
	opts := testRootCommandOptions(server)
	opts.channelConnectWait = time.Second
	opts.channelConnectPoll = time.Millisecond
	opts.input = strings.NewReader(input)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, opts)
	return stdout.String(), stderr.String(), code
}

func channelOnboardingCLIResult(phase, identityState string, ready bool) map[string]any {
	operation := map[string]any{
		"operation_id": operatorChannelCLIOperation,
		"verb":         "connect", "provider": "telegram", "phase": phase,
		"revision": 6, "binding_revision": 1, "activation_revision": 1,
	}
	result := map[string]any{
		"operation": operation,
		"candidate": map[string]any{
			"provider":  "telegram",
			"interface": map[string]any{"selector": operatorChannelCLISelector},
			"target":    map[string]any{"selector": "ingress:support:telegram:telegram"},
		},
		"readiness": operatorChannelCLIReadiness(ready),
	}
	if identityState != "" {
		identity := operatorChannelCLIOperationResult(identityState, 2, 1)
		result["identity_operation"] = identity
	}
	return result
}

func operatorChannelCLIListResult(status string, pending map[string]any) map[string]any {
	identity := map[string]any{
		"interface": map[string]any{
			"interface_ref": "swarm.hitl-channel/v2", "channel_pack_id": "provider.telegram.hitl_channel",
			"channel_pack_version": "1.0.0", "channel_manifest_hash": "sha256:" + strings.Repeat("a", 64),
			"semantic_generation": "operator-channel-cli-generation", "selector": operatorChannelCLISelector,
		},
		"status": status, "binding_revision": 1, "account_presentation": "@m***o", "conversation_scope": "shared",
	}
	if status == "unbound" {
		identity["binding_revision"] = 0
		identity["account_presentation"] = ""
		identity["conversation_scope"] = ""
	}
	if pending != nil {
		identity["pending_operation"] = pending
	}
	row := map[string]any{"identity": identity, "readiness": operatorChannelCLIReadiness(status == "current")}
	return map[string]any{"principal_id": "principal-1", "channels": []any{row}}
}

func operatorChannelCLIReadiness(ready bool) map[string]any {
	activationRevision := 0
	bindingRevision := 0
	if ready {
		activationRevision = 1
		bindingRevision = 1
	}
	return map[string]any{
		"ready": ready, "reason": map[bool]string{true: "ready", false: "activation_not_current"}[ready],
		"coordinate": map[string]any{
			"bundle_hash": "bundle-v1:sha256:" + strings.Repeat("b", 64), "bundle_source": "persisted",
			"bundle_identity": "operator-channel-cli@1.0.0#fixture", "pack_inventory_generation": "pack-generation",
			"context_publication_generation": 1, "plan_generation": "sha256:" + strings.Repeat("c", 64), "target_generation": 1,
		},
		"activation_revision": activationRevision, "binding_revision": bindingRevision, "observed_at": "2026-08-24T23:59:59Z",
	}
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
