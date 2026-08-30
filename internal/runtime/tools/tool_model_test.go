package tools

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestAdmittedToolMutationCannotChangeDefinitionAuthorizationRateOrDispatch(t *testing.T) {
	properties := map[string]runtimecontracts.ToolInputSchema{
		"domain": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
	}
	headers := map[string]string{"X-Owner": "{{input.domain}}"}
	body := map[string]any{"domain": "{{input.domain}}"}
	response := map[string]any{"ok": "{{response.body.ok}}"}
	credentials := []string{"api_key"}
	entry := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolDescription("owned definition"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolPermission("external_api_access"),
		runtimecontracts.WithToolRateLimit("2/s", "250ms"),
		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(properties), runtimecontracts.ToolSchemaRequired("domain")),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
		),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST", URL: "https://owner.example.test/{{input.domain}}", Headers: headers, Body: body,
		}),
		runtimecontracts.WithToolResponseMapping(response),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}),
		runtimecontracts.WithToolCredentials(credentials...),
	)

	properties["hijacked"] = runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean)
	headers["X-Owner"] = "mutated"
	body["domain"] = "mutated"
	response["ok"] = false
	credentials[0] = "mutated"
	httpReadback, _ := entry.HTTP()
	httpReadback.URL = "https://mutated.invalid"
	httpReadback.Headers["X-Owner"] = "mutated-readback"
	mappingReadback, _ := entry.ResponseMapping()
	mappingReadback["ok"] = "mutated-readback"

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"owner_probe": entry}})
	definitions, err := toolDefinitionsForRuntime(source, nil)
	if err != nil {
		t.Fatalf("toolDefinitionsForRuntime: %v", err)
	}
	var definition *llm.ToolDefinition
	for index := range definitions {
		if definitions[index].Name == "owner_probe" {
			definition = &definitions[index]
			break
		}
	}
	if definition == nil || definition.Description != "owned definition" {
		t.Fatalf("definition = %#v, want immutable owner definition", definitions)
	}
	definitionSchema, ok := definition.Schema.(map[string]any)
	if !ok {
		t.Fatalf("definition schema type = %T, want map", definition.Schema)
	}
	if properties, ok := definitionSchema["properties"].(map[string]any); !ok || properties["domain"] == nil || properties["hijacked"] != nil {
		t.Fatalf("definition schema = %#v, want admitted domain only", definition.Schema)
	}
	execution, ok := executionToolFromAdmitted("owner_probe", entry)
	if !ok {
		t.Fatal("execution projection rejected admitted HTTP tool")
	}
	rate := execution.RateLimit()
	if execution.RequiredPermission() != "external_api_access" || execution.Handler() != runtimecontracts.ToolHandlerHTTP ||
		!rate.Enabled || rate.Limit != 2 || rate.Period.String() != "1s" || rate.MaxWait.String() != "250ms" {
		t.Fatalf("execution authority changed: permission=%q handler=%q rate=%#v", execution.RequiredPermission(), execution.Handler(), rate)
	}
	httpExecution, ok := execution.HTTPExecution()
	if !ok {
		t.Fatal("execution projection lost admitted HTTP plan")
	}
	request, err := httpExecution.Prepare(map[string]any{"domain": "example.com"}, map[string]any{"api_key": "secret"})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if request.URL() != "https://owner.example.test/example.com" || request.Headers().Get("X-Owner") != "example.com" ||
		string(request.Body()) != `{"domain":"example.com"}` || execution.Credentials()[0] != "api_key" {
		t.Fatalf("dispatch projection changed: url=%q headers=%v body=%s credentials=%v", request.URL(), request.Headers(), request.Body(), execution.Credentials())
	}
}

func TestExecutionToolsForActorUsesScopedDeclarationWithLiteralPublicName(t *testing.T) {
	tool := func(description string) runtimecontracts.ToolSchemaEntry {
		return runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolDescription(description),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin),
			runtimecontracts.WithToolSchemas(
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			),
		)
	}
	alpha := runtimecontracts.FlowContractView{
		Path:  "alpha",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "alpha"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"local-worker": {ID: "alpha-worker", Tools: []string{"shared-tool", "infra.ping"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"shared-tool": tool("alpha scoped tool"),
			"infra.ping": runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("mcp")),
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			),
		},
	}
	beta := runtimecontracts.FlowContractView{
		Path:  "beta",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "beta"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"local-worker": {ID: "public-worker", Tools: []string{"shared-tool", "infra.ping"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"shared-tool": tool("beta scoped tool"),
			"infra.ping": runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("mcp")),
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
			),
		},
	}
	refs := map[string]runtimecontracts.ContractURIRef{
		"alpha/local-worker": {Kind: "agent", FlowID: "alpha", LocalID: "local-worker", Full: "test://alpha/local-worker"},
		"beta/local-worker":  {Kind: "agent", FlowID: "beta", LocalID: "local-worker", Full: "test://beta/local-worker"},
	}
	byURI := map[string]runtimecontracts.ContractURIRef{}
	for _, ref := range refs {
		byURI[ref.Full] = ref
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{alpha, beta}},
			ByID: map[string]*runtimecontracts.FlowContractView{"alpha": &alpha, "beta": &beta},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{Agents: refs, ByURI: byURI},
	})
	entries, err := executionToolsForActor(source, models.AgentConfig{
		ID: "public-worker", FlowID: "beta", Tools: []string{"shared-tool"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := entries["shared-tool"]
	if !ok || entry.Description() != "beta scoped tool" {
		t.Fatalf("runtime scoped tool = %#v ok %v", entry, ok)
	}
	findings := RequiredMCPToolAvailabilityFindings(source, nil)
	if len(findings) != 2 || findings[0].AgentID != "alpha-worker" || findings[1].AgentID != "public-worker" {
		t.Fatalf("scoped required MCP findings = %#v", findings)
	}
}

func TestDirectAndDurableHTTPExecutionConsumeSameAdmittedToolSemantics(t *testing.T) {
	entry := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
		),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST",
			URL:    "https://provider.example.test/{{input.path}}?q={{input.query}}",
			Headers: map[string]string{
				"X-Query": "{{input.query}}",
			},
			Body: map[string]any{"query": "{{input.query}}"},
		}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}),
		runtimecontracts.WithToolResponseMapping(map[string]any{"value": "{{response.body.value}}"}),
	)
	direct, ok := executionToolFromAdmitted("parity_probe", entry)
	if !ok {
		t.Fatal("direct execution projection rejected admitted tool")
	}
	directHTTP, _ := direct.HTTPExecution()
	durableHTTP, _ := entry.HTTPExecution()
	input := map[string]any{"path": "a/b", "query": "x y"}
	directRequest, err := directHTTP.Prepare(input, nil)
	if err != nil {
		t.Fatalf("direct Prepare: %v", err)
	}
	durableRequest, err := durableHTTP.Prepare(input, nil)
	if err != nil {
		t.Fatalf("durable Prepare: %v", err)
	}
	if directRequest.Method() != durableRequest.Method() || directRequest.URL() != durableRequest.URL() ||
		!reflect.DeepEqual(directRequest.Headers(), durableRequest.Headers()) || !reflect.DeepEqual(directRequest.Body(), durableRequest.Body()) ||
		directRequest.Timeout() != durableRequest.Timeout() {
		t.Fatalf("prepared request drift: direct=%#v durable=%#v", directRequest, durableRequest)
	}
	responseEnv := map[string]any{"response": map[string]any{"status": 200, "body": map[string]any{"ok": true, "value": "same"}}}
	directSuccess, _ := direct.ResponseSuccessPolicy()
	durableSuccess, _ := entry.ResponseSuccessPolicy()
	if err := directSuccess.Evaluate(responseEnv); err != nil {
		t.Fatalf("direct success policy: %v", err)
	}
	if err := durableSuccess.Evaluate(responseEnv); err != nil {
		t.Fatalf("durable success policy: %v", err)
	}
	directMapping, _ := direct.CompiledResponseMapping()
	durableMapping, _ := entry.CompiledResponseMapping()
	directResult, err := directMapping.Render(responseEnv)
	if err != nil {
		t.Fatalf("direct response mapping: %v", err)
	}
	durableResult, err := durableMapping.Render(responseEnv)
	if err != nil {
		t.Fatalf("durable response mapping: %v", err)
	}
	if !reflect.DeepEqual(directResult, durableResult) {
		t.Fatalf("response projection drift: direct=%#v durable=%#v", directResult, durableResult)
	}
}

func TestExecutor_HTTPToolExecutesTemplateAndResponseMapping(t *testing.T) {
	t.Setenv("TEST_HTTP_API_KEY", "secret-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "Bearer secret-token"; r.Header.Get("Authorization") != want {
			t.Fatalf("Authorization = %q, want %q", r.Header.Get("Authorization"), want)
		}
		if got := r.URL.Query().Get("domain"); got != "example.com" {
			t.Fatalf("domain query = %q, want example.com", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"available": true,
			"provider":  "test",
		})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolDescription("Check domain availability"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"domain": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}), runtimecontracts.ToolSchemaRequired("domain")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "GET",
				URL:    server.URL + "?domain={{input.domain}}",
				Headers: map[string]string{
					"Authorization": "Bearer {{credentials.test_http_api_key}}",
				},
			}), runtimecontracts.WithToolResponseMapping(map[string]any{
				"available": "{{response.body.available}}",
				"status":    "{{response.status}}",
			}), runtimecontracts.WithToolCredentials([]string{"test_http_api_key"}...)),
		},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Tools:         []string{"check_domain"},
	})
	out, err := exec.Execute(ctx, "check_domain", map[string]any{"domain": "example.com"})
	if err != nil {
		t.Fatalf("Execute(check_domain): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got, ok := result["available"].(bool); !ok || !got {
		t.Fatalf("available = %#v, want true", result["available"])
	}
	if got, ok := result["status"].(int); !ok || got != 200 {
		t.Fatalf("status = %#v, want 200", result["status"])
	}
}

func TestExecutor_HTTPToolRejectsProviderOutputOutsideAdmittedSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":"not-a-boolean"}`)
	}))
	defer server.Close()

	output := runtimecontracts.MustToolInputSchema(
		runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"ok": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean),
		}),
		runtimecontracts.ToolSchemaRequired("ok"),
	)
	tool := admittedExecutionToolForTest(t, "schema_probe",
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), output),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodGet, URL: server.URL}),
	)
	exec := NewExecutorWithOptions(nil, ExecutorOptions{})
	_, err := exec.execHTTPRequestOnce(unmanagedToolTestContext(), http.MethodGet, server.URL, http.Header{}, nil, time.Second, tool, nil)
	failure, ok := runtimefailures.As(err)
	if err == nil || !ok || failure.Failure.Detail.Code != "provider_response_schema_invalid" {
		t.Fatalf("failure = %#v, want provider_response_schema_invalid", failure)
	}
}

func TestExecutor_HTTPResponseSuccessPolicyParityCases(t *testing.T) {
	t.Setenv("POLICY_SECRET", "provider-secret")
	tests := []struct {
		name        string
		status      int
		body        string
		policy      runtimecontracts.HTTPResponseSuccess
		credential  bool
		wantFailure bool
		forbidError string
	}{
		{name: "status 2xx", status: http.StatusNoContent, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}},
		{name: "status non-2xx", status: http.StatusMultipleChoices, body: `{}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}, wantFailure: true},
		{name: "boolean equality", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}},
		{name: "string equality", status: http.StatusOK, body: `{"state":"accepted"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}},
		{name: "numeric equality", status: http.StatusOK, body: `{"count":2}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.count", Equals: int64(2)}},
		{name: "provider failure", status: http.StatusOK, body: `{"ok":false}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}, wantFailure: true},
		{name: "unresolved path", status: http.StatusOK, body: `{"ok":true}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.missing", Equals: true}, wantFailure: true},
		{name: "secret redaction", status: http.StatusOK, body: `{"state":"provider-secret"}`, policy: runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.state", Equals: "accepted"}, credential: true, wantFailure: true, forbidError: "provider-secret"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.credential && r.Header.Get("X-Policy-Secret") != "provider-secret" {
					t.Fatalf("X-Policy-Secret = %q", r.Header.Get("X-Policy-Secret"))
				}
				if tc.body != "" {
					w.Header().Set("Content-Type", "application/json")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()

			tool := runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolDescription("exercise response success semantics"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: http.MethodPost,
				URL:    server.URL,
			}), runtimecontracts.WithToolSchemas(
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			), runtimecontracts.WithToolResponseSuccess(tc.policy))

			if tc.credential {
				httpSpec, ok := tool.HTTP()
				if !ok {
					t.Fatal("policy probe is missing HTTP execution semantics")
				}
				httpSpec.Headers = map[string]string{"X-Policy-Secret": "{{credentials.policy_secret}}"}
				var err error
				tool, err = tool.WithHTTP(httpSpec)
				if err != nil {
					t.Fatalf("replace policy probe headers: %v", err)
				}
				tool, err = tool.WithStaticCredentials("policy_secret")
				if err != nil {
					t.Fatalf("replace policy probe credentials: %v", err)
				}
			}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"policy_probe": tool}})
			exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
			ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"policy_probe"}})
			_, err := exec.Execute(ctx, "policy_probe", map[string]any{})
			if !tc.wantFailure {
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				return
			}
			requireToolFailure(t, err, runtimefailures.ClassConnectorFailure, "provider_response_rejected")
			if tc.forbidError != "" && strings.Contains(err.Error(), tc.forbidError) {
				t.Fatalf("Execute error leaked %q: %v", tc.forbidError, err)
			}
		})
	}
}

func TestExecutor_HTTPToolEncodesURLTemplateComponentsAndPreservesRawHeaderBody(t *testing.T) {
	query := `to:karpathy (agent OR "agentic")`
	var sawEscapedPath string
	var sawRawQuery string
	var sawHeader string
	var sawBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawEscapedPath = r.URL.EscapedPath()
		sawRawQuery = r.URL.RawQuery
		sawHeader = r.Header.Get("X-Search-Query")
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll body: %v", err)
		}
		if err := json.Unmarshal(rawBody, &sawBody); err != nil {
			t.Fatalf("Unmarshal body %s: %v", rawBody, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"x_search_tweets": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolDescription("Search X tweets"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"segment": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
				"query":   runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
				"cursor":  runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}), runtimecontracts.ToolSchemaRequired("segment", "query", "cursor")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "POST",
				URL:    server.URL + "/profiles/{{input.segment}}/search?q={{input.query}}&cursor={{input.cursor}}",
				Headers: map[string]string{
					"X-Search-Query": "raw {{input.query}}",
				},
				Body: map[string]any{
					"query": "{{input.query}}",
				},
			})),
		},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Tools:         []string{"x_search_tweets"},
	})
	if _, err := exec.Execute(ctx, "x_search_tweets", map[string]any{
		"segment": "team/a b",
		"query":   query,
		"cursor":  "page 1",
	}); err != nil {
		t.Fatalf("Execute(x_search_tweets): %v", err)
	}
	if want := "/profiles/team%2Fa%20b/search"; sawEscapedPath != want {
		t.Fatalf("escaped path = %q, want %q", sawEscapedPath, want)
	}
	if want := "q=to%3Akarpathy%20%28agent%20OR%20%22agentic%22%29&cursor=page%201"; sawRawQuery != want {
		t.Fatalf("raw query = %q, want %q", sawRawQuery, want)
	}
	if want := "raw " + query; sawHeader != want {
		t.Fatalf("header = %q, want %q", sawHeader, want)
	}
	if got := sawBody["query"]; got != query {
		t.Fatalf("body query = %#v, want %q", got, query)
	}
}

func TestResolveHTTPURLTemplatePreservesCompleteURL(t *testing.T) {
	want := "https://example.test/search?q=to%3Akarpathy%20%28agent%29"
	execution := mustHTTPExecution(t, "{{input.url}}")
	request, err := execution.Prepare(map[string]any{"url": want}, nil)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	got := request.URL()
	if got != want {
		t.Fatalf("resolved URL = %q, want %q", got, want)
	}
}

func TestResolveHTTPURLTemplatePreservesURLBaseAndAuthorityPlaceholders(t *testing.T) {
	credentials := map[string]any{"base_url": "https://api.example.com:8443"}
	input := map[string]any{"scheme": "https", "host": "api.example.com:8443", "query": "agentic orchestration"}
	for _, tt := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "base URL placeholder with fixed path",
			raw:  "{{credentials.base_url}}/v1/search?q={{input.query}}",
			want: "https://api.example.com:8443/v1/search?q=agentic%20orchestration",
		},
		{
			name: "scheme placeholder",
			raw:  "{{input.scheme}}://api.example.com/v1/search?q={{input.query}}",
			want: "https://api.example.com/v1/search?q=agentic%20orchestration",
		},
		{
			name: "authority placeholder",
			raw:  "https://{{input.host}}/v1/search?q={{input.query}}",
			want: "https://api.example.com:8443/v1/search?q=agentic%20orchestration",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, err := mustHTTPExecution(t, tt.raw).Prepare(input, credentials)
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			got := request.URL()
			if got != tt.want {
				t.Fatalf("resolved URL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecutor_CustomWebSearchEncodesHTTPURLTemplateComponents(t *testing.T) {
	query := `to:karpathy (agent OR "agentic")`
	var sawEscapedPath string
	var sawRawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawEscapedPath = r.URL.EscapedPath()
		sawRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"title": "hit", "url": "https://example.test/hit", "snippet": "body"},
			},
		})
	}))
	defer server.Close()

	exec := NewExecutorWithOptions(nil, ExecutorOptions{})
	httpExecution := mustHTTPExecution(t, server.URL+"/search/{{input.max_results}}?q={{input.query}}")
	results, err := exec.executeCustomWebSearch(unmanagedToolTestContext(), webSearchProviderConfig{
		HTTP:         httpExecution,
		HasHTTP:      true,
		ResponsePath: "results",
		FieldMapping: map[string]string{
			"title":   "title",
			"url":     "url",
			"snippet": "snippet",
		},
	}, query, 20, "")
	if err != nil {
		t.Fatalf("executeCustomWebSearch: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %#v, want one result", results)
	}
	if want := "/search/20"; sawEscapedPath != want {
		t.Fatalf("escaped path = %q, want %q", sawEscapedPath, want)
	}
	if want := "q=to%3Akarpathy%20%28agent%20OR%20%22agentic%22%29"; sawRawQuery != want {
		t.Fatalf("raw query = %q, want %q", sawRawQuery, want)
	}
}

func mustHTTPExecution(t *testing.T, rawURL string) runtimecontracts.ToolHTTPExecution {
	t.Helper()
	execution, err := runtimecontracts.AdmitToolHTTPExecution(runtimecontracts.HTTPToolSpec{Method: "GET", URL: rawURL})
	if err != nil {
		t.Fatalf("AdmitToolHTTPExecution: %v", err)
	}
	return execution
}

func TestExecutor_MCPToolExecutesDiscoveredServerTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			writeMCPResult(t, w, req["id"], map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
			})
		case "notifications/initialized":
			writeMCPResult(t, w, nil, map[string]any{})
		case "tools/list":
			writeMCPResult(t, w, req["id"], map[string]any{
				"tools": []map[string]any{{
					"name":        "ping",
					"description": "Ping the infra sidecar",
					"inputSchema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"target": map[string]any{"type": "string"},
						},
					},
				}},
			})
		case "tools/call":
			writeMCPResult(t, w, req["id"], map[string]any{
				"content":           []any{},
				"structuredContent": map[string]any{"ok": true},
			})
		default:
			t.Fatalf("unexpected mcp method %q", method)
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	ctx := models.WithActor(unmanagedToolTestContext(), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Tools:         []string{"infra.ping"},
	})
	out, err := exec.Execute(ctx, "infra.ping", map[string]any{"target": "svc"})
	if err != nil {
		t.Fatalf("Execute(infra.ping): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok || result["ok"] != true {
		t.Fatalf("result = %#v, want ok=true", out)
	}
}

func TestExecutor_ToolDefinitionsForActor_ExcludesContractMCPWithoutDiscovery(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {ID: "agent-1", Tools: []string{"infra.ping"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"infra.ping": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolDescription("Authored MCP tool should not create runtime availability"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("mcp")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
		},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{ExecutionMode: "live", ID: "agent-1", Tools: []string{"infra.ping"}})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	if containsToolName(names, "infra.ping") {
		t.Fatalf("did not expect authored handler_type mcp entry without discovery proof to be delivered, got %v", names)
	}
}

func TestExecutor_ToolDefinitionsForActor_UsesSharedActorRegistry(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"check_domain": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolDescription("Check domain availability"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"domain": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			})), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "GET",
				URL:    "https://example.test",
			})),
		},
	})

	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		WorkflowSource: source,
		ModelRuntimes:  staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})
	defs := exec.ToolDefinitionsForActor(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "agent-1",
		Tools:         []string{"check_domain"},
		NativeTools:   models.NativeToolConfig{FileIO: true},
	})

	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	if !containsToolName(names, "check_domain") {
		t.Fatalf("expected actor registry to include configured contract tool, got %v", names)
	}
	if !containsToolName(names, "read_file") || !containsToolName(names, "write_file") {
		t.Fatalf("expected actor registry to include enabled native file tools, got %v", names)
	}
}

func containsToolName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func TestToolAdmissionRejectsMalformedHTTPTool(t *testing.T) {
	if _, err := runtimecontracts.NewToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "GET"}),
	); err == nil || !strings.Contains(err.Error(), "http.url is required") {
		t.Fatalf("NewToolSchemaEntry error = %v, want missing URL rejection", err)
	}
}

func TestValidateToolImplementationsRejectsAuthoredPrivateChannelActivityNamespace(t *testing.T) {
	toolID := runtimecontracts.PrivateChannelActivityPrefix + "authored.send.gdeadbeef"
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			toolID: runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"})),
		},
	})
	_, err := ValidateToolImplementations(source)
	if err == nil || !strings.Contains(err.Error(), "reserved private channel activity namespace") {
		t.Fatalf("ValidateToolImplementations error = %v, want private namespace rejection", err)
	}
}

func TestValidateToolImplementationsWarnsForUnspecifiedNonExecutableTool(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"unimplemented_call": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
		},
	})

	warnings, err := ValidateToolImplementations(source)
	if err != nil {
		t.Fatalf("ValidateToolImplementations: %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Error(), "has no executable implementation") {
		t.Fatalf("warnings = %#v, want one unspecified implementation warning", warnings)
	}
}

func TestContractDefinitionsForSource_DoesNotExposeRemovedInfraBuiltins(t *testing.T) {
	defs, err := ContractDefinitionsForSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		switch def.Name {
		case "nginx_reload", "systemd_control", "certbot_execute":
			t.Fatalf("unexpected infra builtin still exposed: %s", def.Name)
		}
	}
}

func writeMCPResult(t *testing.T, w http.ResponseWriter, id any, result any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func toolsRepoRootForTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeToolFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
