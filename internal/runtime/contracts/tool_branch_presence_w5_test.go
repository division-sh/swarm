package contracts

import (
	"fmt"
	"strings"
	"testing"
)

func TestW5ModuleFieldPresenceAndBranchMatrix(t *testing.T) {
	base := map[string]string{
		"handler_type": "wasm", "path": "modules/pinned.wasm", "abi": "core-json-v1", "entry": "compute",
		"digest":        "sha256:" + strings.Repeat("0", 64),
		"input_schema":  "{type: object, properties: {value: {type: integer}}}",
		"output_schema": "{type: object, properties: {value: {type: integer}}}",
		"limits":        "{gas: 100, memory_pages: 16, output_bytes: 1024}",
	}
	states := []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"}
	for _, row := range []struct{ field, scalar, mapping, admitted string }{
		{"handler_type", "wasm", "{kind: wasm}", "scalar"},
		{"path", "modules/pinned.wasm", "{path: pinned.wasm}", "scalar"},
		{"abi", "core-json-v1", "{abi: core-json-v1}", "scalar"},
		{"entry", "compute", "{entry: compute}", "scalar"},
		{"digest", base["digest"], "{digest: pinned}", "scalar"},
		{"input_schema", "object", base["input_schema"], "mapping"},
		{"output_schema", "object", base["output_schema"], "mapping"},
		{"limits", "100", base["limits"], "mapping"},
		{"source_path", "modules/source.rs", "{path: source.rs}", "missing,scalar"},
		{"source_hash", base["digest"], "{hash: pinned}", "missing,scalar"},
		{"runtime", "python", "{interpreter: pinned}", "missing,empty_mapping,mapping"},
	} {
		for _, state := range states {
			t.Run(row.field+"/"+state, func(t *testing.T) {
				fields := make(map[string]string, len(base))
				for key, value := range base {
					fields[key] = value
				}
				delete(fields, row.field)
				if state != "missing" {
					fields[row.field] = map[string]string{"null": "null", "empty": "''", "scalar": row.scalar, "empty_sequence": "[]", "sequence": "[value]", "empty_mapping": "{}", "mapping": row.mapping}[state]
				}
				var body strings.Builder
				body.WriteString("module:\n")
				for _, key := range sortedContractKeys(fields) {
					fmt.Fprintf(&body, "  %s: %s\n", key, fields[key])
				}
				entries, err := admitW5Tools(t, body.String())
				want := strings.Contains(","+row.admitted+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t want=%t: %v", err == nil, want, err)
				}
				if err == nil {
					module, ok := entries["module"].Module()
					if !ok || module.Kind != "wasm" || entries["module"].AgentExposable() {
						t.Fatal("module lost its kind or acquired agent authority")
					}
					if row.field == "runtime" && state == "mapping" && module.Runtime.Interpreter != "pinned" {
						t.Fatal("runtime identity lost")
					}
				}
			})
		}
	}
	for _, field := range []string{"gas", "memory_pages", "output_bytes"} {
		for _, value := range []string{"null", "''", "0", "-1", "1.0", "1.5", "'1'", ".inf", "[]", "{}"} {
			t.Run("limits/"+field+"/"+value, func(t *testing.T) {
				fields := map[string]string{"gas": "100", "memory_pages": "16", "output_bytes": "1024"}
				fields[field] = value
				var body strings.Builder
				body.WriteString("module:\n")
				for _, key := range sortedContractKeys(base) {
					if key != "limits" {
						fmt.Fprintf(&body, "  %s: %s\n", key, base[key])
					}
				}
				fmt.Fprintf(&body, "  limits: {gas: %s, memory_pages: %s, output_bytes: %s}\n", fields["gas"], fields["memory_pages"], fields["output_bytes"])
				if _, err := admitW5Tools(t, body.String()); err == nil {
					t.Fatal("invalid module limit accepted")
				}
			})
		}
	}
	for _, field := range []string{"interpreter", "interpreter_digest", "snapshot_digest", "harness_abi"} {
		for _, value := range []string{"null", "''", "[]", "[pinned]", "{}", "{value: pinned}", "7"} {
			var body strings.Builder
			body.WriteString("module:\n")
			for _, key := range sortedContractKeys(base) {
				fmt.Fprintf(&body, "  %s: %s\n", key, base[key])
			}
			fmt.Fprintf(&body, "  runtime: {%s: %s}\n", field, value)
			if _, err := admitW5Tools(t, body.String()); err == nil {
				t.Fatalf("runtime.%s=%s accepted", field, value)
			}
		}
	}
	for _, name := range []string{"category", "permission", "effect_class", "rate_limit", "rate_limit_max_wait", "http", "response_mapping", "response_success", "credentials", "managed_credential"} {
		for _, value := range []string{"null", "''", "[]", "{}"} {
			var body strings.Builder
			body.WriteString("module:\n")
			for _, key := range sortedContractKeys(base) {
				fmt.Fprintf(&body, "  %s: %s\n", key, base[key])
			}
			fmt.Fprintf(&body, "  %s: %s\n", name, value)
			if _, err := admitW5Tools(t, body.String()); err == nil {
				t.Fatalf("module acquired forbidden %s=%s", name, value)
			}
		}
	}
}

func TestW5ToolNestedTransportFieldPresenceMatrix(t *testing.T) {
	for _, row := range []struct{ parent, field, scalar, sequence, mapping, admitted, siblings string }{
		{"http", "method", "POST", "[POST]", "{method: POST}", "scalar", "url: 'https://example.invalid'"},
		{"http", "url", "'https://example.invalid'", "[url]", "{url: target}", "scalar", "method: POST"},
		{"http", "headers", "value", "[value]", "{x: value}", "missing,empty_mapping,mapping", "method: POST, url: 'https://example.invalid'"},
		{"http", "timeout_seconds", "5", "[5]", "{seconds: 5}", "missing,scalar", "method: POST, url: 'https://example.invalid'"},
		{"response_success", "kind", "json_field_equals", "[json_field_equals]", "{kind: json_field_equals}", "scalar", "path: response.body.ok, equals: false"},
		{"response_success", "path", "response.body.ok", "[response.body.ok]", "{path: response.body.ok}", "scalar", "kind: json_field_equals, equals: false"},
		{"response_success", "equals", "false", "[false]", "{ok: false}", "empty,scalar", "kind: json_field_equals, path: response.body.ok"},
		{"managed_credential", "key", "token", "[token]", "{key: token}", "scalar", ""},
		{"managed_credential", "header", "Authorization", "[Authorization]", "{header: Authorization}", "missing,empty,scalar", "key: token"},
		{"managed_credential", "prefix", "Bearer", "[Bearer]", "{prefix: Bearer}", "missing,empty,scalar", "key: token, header: Authorization"},
		{"managed_credential", "grant_type", "client_credentials", "[client_credentials]", "{grant: value}", "missing,empty,scalar", "key: token"},
		{"managed_credential", "grant_model", "scope_grant", "[scope_grant]", "{grant: value}", "missing,empty,scalar", "key: token"},
		{"managed_credential", "installation_id_input", "installation_id", "[installation_id]", "{input: value}", "scalar", "key: token, grant_type: github_app_installation"},
		{"managed_credential", "scopes", "scope", "[scope]", "{scope: true}", "missing,empty_sequence,sequence", "key: token"},
		{"managed_credential", "token_request", "body", "[body]", "{body: json}", "missing,empty_mapping,mapping", "key: token"},
		{"token_request", "client_auth", "post", "[post]", "{auth: post}", "missing,empty,scalar", ""},
		{"token_request", "body", "json", "[json]", "{body: json}", "missing,empty,scalar", ""},
		{"token_request", "static_headers", "value", "[value]", "{x: value}", "missing,empty_mapping,mapping", ""},
	} {
		for _, state := range []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"} {
			t.Run(row.parent+"/"+row.field+"/"+state, func(t *testing.T) {
				members := row.siblings
				if state != "missing" {
					if members != "" {
						members += ", "
					}
					members += row.field + ": " + map[string]string{"null": "null", "empty": "''", "scalar": row.scalar, "empty_sequence": "[]", "sequence": row.sequence, "empty_mapping": "{}", "mapping": row.mapping}[state]
				}
				body := "worker:\n  handler_type: http\n"
				if row.parent != "http" {
					body += "  http: {method: POST, url: 'https://example.invalid'}\n"
				}
				if row.parent == "token_request" {
					body += "  managed_credential: {key: token, token_request: {" + members + "}}\n"
				} else {
					body += "  " + row.parent + ": {" + members + "}\n"
				}
				_, err := admitW5Tools(t, body)
				want := strings.Contains(","+row.admitted+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t want=%t: %v", err == nil, want, err)
				}
			})
		}
	}
}
