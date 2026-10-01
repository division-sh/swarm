package contracts

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func admitW5Tools(t testing.TB, body string) (map[string]ToolSchemaEntry, error) {
	t.Helper()
	source, err := yamlsource.Load([]byte(body))
	if err != nil {
		return nil, err
	}
	return projectToolDeclarationsValue(source.Document("tools.yaml").Root())
}

func TestW5ToolFieldPresenceMatrix(t *testing.T) {
	states := []string{"missing", "null", "empty", "scalar", "empty_sequence", "sequence", "empty_mapping", "mapping"}
	for _, row := range []struct{ field, scalar, sequence, mapping, admitted, base string }{
		{"description", "business intent", "[intent]", "{intent: true}", "missing,empty,scalar", ""},
		{"category", "provider_connector", "[provider_connector]", "{category: provider_connector}", "missing,empty,scalar", ""},
		{"handler_type", "mcp", "[mcp]", "{handler: mcp}", "missing,empty,scalar", ""},
		{"effect_class", "read_only", "[read_only]", "{effect: read_only}", "missing,empty,scalar", ""},
		{"permission", "execute", "[execute]", "{permission: execute}", "missing,empty,scalar", ""},
		{"input_schema", "object", "[{type: string}]", "{type: string}", "missing,empty_mapping,mapping", ""},
		{"output_schema", "object", "[{type: string}]", "{type: string}", "missing,empty_mapping,mapping", ""},
		{"credentials", "secret", "[secret]", "{key: secret}", "missing,empty_sequence,sequence", ""},
		{"http", "GET", "[GET]", "{method: GET, url: 'https://example.invalid'}", "mapping", "  handler_type: http\n"},
		{"response_mapping", "response.body", "[response.body]", "{body: '{{response.body}}'}", "missing,empty_mapping,mapping", "  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n"},
		{"response_success", "http_status_2xx", "[http_status_2xx]", "{kind: http_status_2xx}", "missing,mapping", "  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n"},
		{"managed_credential", "token", "[token]", "{key: token}", "missing,mapping", "  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n"},
		{"rate_limit", "3/1s", "[3/1s]", "{limit: 3}", "scalar", "  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n  rate_limit_max_wait: 1s\n"},
		{"rate_limit_max_wait", "1s", "[1s]", "{wait: 1}", "scalar", "  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'}\n  rate_limit: 3/1s\n"},
	} {
		for _, state := range states {
			t.Run(row.field+"/"+state, func(t *testing.T) {
				value := map[string]string{"null": "null", "empty": "''", "scalar": row.scalar, "empty_sequence": "[]", "sequence": row.sequence, "empty_mapping": "{}", "mapping": row.mapping}[state]
				body := "worker:\n" + row.base
				if state != "missing" {
					body += "  " + row.field + ": " + value + "\n"
				}
				if state == "missing" && row.base == "" {
					body = "worker: {}\n"
				}
				entries, err := admitW5Tools(t, body)
				want := strings.Contains(","+row.admitted+",", ","+state+",")
				if (err == nil) != want {
					t.Fatalf("admit=%t want=%t: %v", err == nil, want, err)
				}
				if err != nil {
					return
				}
				entry := entries["worker"]
				_, authored := entry.admissionProvenance[row.field]
				if authored && state == "missing" && row.field != "input_schema" && row.field != "output_schema" {
					t.Fatal("missing field became authored")
				}
				if state == "empty_mapping" && strings.HasSuffix(row.field, "schema") && entry.InputSchema().Kind() != ToolSchemaAny && entry.OutputSchema().Kind() != ToolSchemaAny {
					t.Fatal("Any collapsed to default object")
				}
				if state == "mapping" && row.field == "http" {
					spec, ok := entry.HTTP()
					if !ok || spec.Method != "GET" || spec.URL != "https://example.invalid" {
						t.Fatal("HTTP contract lost")
					}
				}
				if state == "sequence" && row.field == "credentials" && !reflect.DeepEqual(entry.Credentials(), []string{"secret"}) {
					t.Fatal("credential keys lost")
				}
			})
		}
	}
}

func TestW5ToolNestedClosedBranchesAndRetirement(t *testing.T) {
	for _, bad := range []string{
		"<<: {parameters: null}", "returns: {}", "endpoint: ''", "type: null", "required_permission: []", "kind: wasm", "unknown: null",
		"input_schema: {type: object}", "output_schema: {type: object}",
		"http: {method: GET, url: 'https://example.invalid', extra: null}",
		"http: {method: 7, url: 'https://example.invalid'}", "http: {method: GET, url: 'https://example.invalid', headers: {x: 7}}",
		"http: {method: GET, url: 'https://example.invalid', timeout_seconds: 1.5}",
		"managed_credential: {key: token, extra: null}", "managed_credential: {key: token, scopes: [7]}",
		"managed_credential: {key: token, token_request: {body: form, extra: null}}",
		"managed_credential: {key: token, token_request: {static_headers: {x: 7}}}",
		"response_success: {kind: http_status_2xx, path: ''}", "response_success: {kind: http_status_2xx, equals: null}",
		"response_success: {kind: json_field_equals, path: response.body.ok}",
		"response_success: {kind: json_field_equals, path: response.body.ok, equals: null}",
		"response_success: {kind: http_status_2xx, extra: null}",
	} {
		t.Run(bad, func(t *testing.T) {
			base := "worker:\n  handler_type: http\n"
			if !strings.HasPrefix(bad, "http:") {
				base += "  http: {method: GET, url: 'https://example.invalid'}\n"
			}
			if _, err := admitW5Tools(t, base+"  "+bad+"\n"); err == nil {
				t.Fatal("invalid authored branch accepted")
			}
		})
	}
}

func TestW5ToolLiteralHTTPPresenceAndSerialization(t *testing.T) {
	for _, literal := range []string{"null", "false", "0", "''", "[]", "{}"} {
		entries, err := admitW5Tools(t, "worker:\n  handler_type: http\n  http: {method: POST, url: 'https://example.invalid', body: "+literal+"}\n")
		if err != nil {
			t.Fatal(err)
		}
		entry := entries["worker"]
		syntax, _ := entry.HTTP()
		if !syntax.BodyPresent {
			t.Fatalf("%s body lost", literal)
		}
		execution, _ := entry.HTTPExecution()
		request, err := execution.Prepare(nil, nil)
		wantBody := map[string]string{"null": "null", "false": "false", "0": "0", "''": `""`, "[]": "[]", "{}": "{}"}[literal]
		if err != nil || string(request.Body()) != wantBody {
			t.Fatalf("literal %s runtime request body=%q, want %q: %v", literal, request.Body(), wantBody, err)
		}
		body, err := yaml.Marshal(entries)
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := admitW5Tools(t, string(body))
		if err != nil {
			t.Fatal(err)
		}
		left, _ := entry.CanonicalHash()
		right, _ := loaded["worker"].CanonicalHash()
		if left != right {
			t.Fatal("readback changed literal body")
		}
		missing, err := admitW5Tools(t, "worker:\n  handler_type: http\n  http: {method: POST, url: 'https://example.invalid'}\n")
		if err != nil {
			t.Fatal(err)
		}
		absent, _ := missing["worker"].CanonicalHash()
		if absent == left {
			t.Fatal("literal body equals absent body")
		}
		absentExecution, _ := missing["worker"].HTTPExecution()
		absentRequest, err := absentExecution.Prepare(nil, nil)
		if err != nil || len(absentRequest.Body()) != 0 {
			t.Fatalf("omission acquired runtime body=%q: %v", absentRequest.Body(), err)
		}
	}
}

func TestW5ToolAdmissionDiskCatalogRetainedParity(t *testing.T) {
	for _, body := range []string{"worker: {}\n", "worker: {input_schema: {}, output_schema: {type: string}}\n", "worker: {input_schema: {type: string, minLength: 1.5}}\n", "worker: {<<: {parameters: null}}\n"} {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: parity\n")
		writeFixtureFile(t, filepath.Join(root, "tools.yaml"), body)
		disk, diskErr := loadOptionalToolDeclarations(filepath.Join(root, "tools.yaml"))
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		catalog, err := persisted.Decode()
		if err != nil {
			t.Fatal(err)
		}
		retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
		if err != nil {
			t.Fatal(err)
		}
		for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"artifact": artifact, "catalog": catalog, "retained": retained} {
			entries, err := loadOptionalToolDeclarationsFromSource(source, "tools.yaml")
			if (err == nil) != (diskErr == nil) {
				t.Fatalf("%s: %v / %v", name, err, diskErr)
			}
			if !bytes.Equal(artifact.LogicalBlob(), source.LogicalBlob()) || artifact.BundleHash() != source.BundleHash() {
				t.Fatal("authored identity changed")
			}
			if err == nil {
				a, _ := disk["worker"].CanonicalHash()
				b, _ := entries["worker"].CanonicalHash()
				if a != b {
					t.Fatal("typed model changed")
				}
			} else if !strings.Contains(err.Error(), "tools.yaml:") {
				t.Fatal("source diagnostic lost")
			}
		}
	}
}

func TestW5ModuleScopedResolutionAndSerialization(t *testing.T) {
	const schema = "{type: object, properties: {value: {type: integer}}}"
	body := fmt.Sprintf("worker:\n  handler_type: wasm\n  path: modules/worker.wasm\n  abi: core-json-v1\n  entry: compute\n  digest: sha256:%s\n  input_schema: %s\n  output_schema: %s\n  limits: {gas: 18446744073709551615, memory_pages: 17, output_bytes: 1024}\n", strings.Repeat("0", 64), schema, schema)
	entries, err := admitW5Tools(t, body)
	if err != nil {
		t.Fatal(err)
	}
	module := entries["worker"]
	if module.AgentExposable() {
		t.Fatal("module is agent-exposable")
	}
	pinned, _ := module.Module()
	if pinned.Limits.Gas != ^uint64(0) {
		t.Fatal("uint64 gas narrowed")
	}
	pinned.InputSchema["type"] = "string"
	unchanged, _ := module.Module()
	if unchanged.InputSchema["type"] != "object" {
		t.Fatal("module mutation escaped")
	}
	raw, err := yaml.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := admitW5Tools(t, string(raw))
	if err != nil {
		t.Fatal(err)
	}
	a, err := module.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	b, err := loaded["worker"].CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("module readback drift")
	}
	narrower, err := admitW5Tools(t, strings.ReplaceAll(body, "18446744073709551615", "18446744073709551614"))
	if err != nil {
		t.Fatal(err)
	}
	narrowerHash, err := narrower["worker"].CanonicalHash()
	if err != nil || narrowerHash == a {
		t.Fatalf("distinct exact uint64 limits collapsed: %v", err)
	}
	root := &FlowContractView{Paths: FlowContractPaths{FlowPath: "."}, Tools: entries}
	child := &FlowContractView{Paths: FlowContractPaths{FlowPath: "child"}, Tools: map[string]ToolSchemaEntry{"worker": MustToolSchemaEntry(WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject)))}}
	sibling := &FlowContractView{Paths: FlowContractPaths{FlowPath: "sibling"}, Tools: map[string]ToolSchemaEntry{"private": module}}
	root.Children = []FlowContractView{*child, *sibling}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: root}}
	nearer, ok := bundle.ToolEntryForFlow("child", "worker")
	if !ok {
		t.Fatal("nearer declaration lost")
	}
	if _, ok := nearer.Module(); ok {
		t.Fatal("wrong-kind declaration resurrected ancestor")
	}
	if _, ok := bundle.ToolEntryForFlow("child", "private"); ok {
		t.Fatal("sibling declaration leaked")
	}
	ancestor, ok := bundle.ToolEntryForFlow("sibling", "worker")
	if !ok || ancestor.AgentExposable() {
		t.Fatal("ancestor resolution lost")
	}
	if _, err := admitW5Tools(t, body+body); err == nil {
		t.Fatal("duplicate module id accepted")
	}
}

func TestW5ToolsGoverningSpecParses(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(repoRootForContractsTest(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := yamlsource.Load(body); err != nil {
		cause, _ := yamlsource.ParseCause(err)
		t.Fatalf("spec parse: %v; cause: %v", err, cause)
	}
}
