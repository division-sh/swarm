package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

var providerSearchNames = []string{"brave", "serper", "tavily", "custom"}

func TestServedWebSearchProvidersPublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			denyProviderJourneyExternalNetwork(t)
			h := newChannelOnboardingE2EHarness(t, backend, false)
			urls := map[string]string{}
			calls := map[string]*atomic.Int32{}
			for _, name := range providerSearchNames {
				calls[name] = new(atomic.Int32)
				server := httptest.NewServer(searchProviderHandler(t, name, calls[name]))
				t.Cleanup(server.Close)
				urls[name] = server.URL
			}
			redirectExternalHosts(t, map[string]string{
				"api.search.brave.com": urls["brave"], "google.serper.dev": urls["serper"], "api.tavily.com": urls["tavily"],
			})
			h.opts.SourceRoot = writeProviderSearchJourneySource(t, urls["custom"])
			bundleHash := servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot)
			h.opts.TestLLMRuntime = nil
			llm := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "search-llm-key", searchJourneyReply)
			configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", llm.URL+"/v1")
			credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(context.Background(), "OPENAI_COMPATIBLE_API_KEY", "search-llm-key"); err != nil {
				t.Fatal(err)
			}
			for _, name := range providerSearchNames {
				key := "search_" + name + "_key"
				unsetEnvForTest(t, key)
				unsetEnvForTest(t, strings.ToUpper(key))
				if err := credentials.Set(context.Background(), key, name+"-key"); err != nil {
					t.Fatal(err)
				}
			}
			h.start(t)
			defer h.stop(t)
			var runID string
			for _, name := range providerSearchNames {
				t.Run(name, func(t *testing.T) {
					params := map[string]any{
						"bundle_hash": bundleHash,
						"event_name":  "search." + name + "_requested", "payload": map[string]any{"query": name + " query"},
						"idempotency_key": "search-journey-" + name,
					}
					if runID != "" {
						params["run_id"] = runID
					}
					published := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), params)
					if published.RunID == "" || (runID != "" && published.RunID != runID) {
						t.Fatalf("search publication changed its live run: before=%q after=%q", runID, published.RunID)
					}
					runID = published.RunID
					requireProviderSearchPublicResult(t, h.rpcEndpoint(), published.RunID, name)
					if calls[name].Load() != 1 {
						t.Fatalf("%s HTTP requests=%d, want one real search", name, calls[name].Load())
					}
				})
			}
			if got := len(llm.Requests()); got != 2*len(providerSearchNames) {
				t.Fatalf("search LLM requests=%d, want search and terminal output per provider", got)
			}
		})
	}
}

func searchProviderHandler(t *testing.T, name string, calls *atomic.Int32) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		query, count, path, method := request.URL.Query().Get("q"), request.URL.Query().Get("count"), "/search", http.MethodPost
		var body struct {
			Q          string `json:"q"`
			Query      string `json:"query"`
			APIKey     string `json:"api_key"`
			Num        int    `json:"num"`
			MaxResults int    `json:"max_results"`
		}
		if request.Method == http.MethodPost {
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode %s search: %v", name, err)
			}
		}
		validCredential := false
		var response any
		switch name {
		case "brave":
			path, method = "/res/v1/web/search", http.MethodGet
			validCredential = request.Header.Get("X-Subscription-Token") == "brave-key"
			response = map[string]any{"web": map[string]any{"results": []any{map[string]string{"title": name + " title", "url": "https://result.test/" + name, "description": name + " snippet"}}}}
		case "serper":
			query, count = body.Q, strconv.Itoa(body.Num)
			validCredential = request.Header.Get("X-API-KEY") == "serper-key"
			response = map[string]any{"organic": []any{map[string]string{"title": name + " title", "link": "https://result.test/" + name, "snippet": name + " snippet"}}}
		case "tavily":
			query, count = body.Query, strconv.Itoa(body.MaxResults)
			validCredential = body.APIKey == "tavily-key"
			response = map[string]any{"results": []any{map[string]string{"title": name + " title", "url": "https://result.test/" + name, "content": name + " snippet"}}}
		case "custom":
			path, method = "/custom", http.MethodGet
			validCredential = request.Header.Get("X-Search-Key") == "custom-key"
			response = map[string]any{"items": []any{map[string]string{"title": name + " title", "link": "https://result.test/" + name, "summary": name + " snippet"}}}
		}
		if request.URL.Path != path || request.Method != method || !validCredential || query != name+" query" || count != "2" {
			t.Errorf("%s search request path=%s method=%s credential_valid=%t query=%q count=%s", name, request.URL.Path, request.Method, validCredential, query, count)
			http.Error(w, "unexpected search request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("write %s search: %v", name, err)
		}
	})
}

func searchJourneyReply(request llmapi.Request) (llmapi.Reply, error) {
	var body struct {
		Model string `json:"model"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
		Messages []struct {
			Role       string `json:"role"`
			Content    string `json:"content"`
			ToolCallID string `json:"tool_call_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	name, searchResult, searchCallID := "", "", ""
	for _, message := range body.Messages {
		var payload struct {
			Query string `json:"query"`
		}
		if message.Role == "user" && json.Unmarshal([]byte(message.Content), &payload) == nil && payload.Query != "" {
			name, _, _ = strings.Cut(payload.Query, " ")
		}
		if message.Role == "tool" && strings.HasPrefix(message.ToolCallID, "search-") {
			searchResult, searchCallID = message.Content, message.ToolCallID
		}
	}
	if body.Model != "fixture-model" || !slices.Contains(providerSearchNames, name) {
		return llmapi.Reply{}, fmt.Errorf("search request lost model or scoped initial frame")
	}
	toolNames := []string{}
	for _, tool := range body.Tools {
		toolNames = append(toolNames, tool.Function.Name)
	}
	if !slices.Contains(toolNames, "web_search") || !slices.Contains(toolNames, "emit_search_"+name+"_completed") {
		return llmapi.Reply{}, fmt.Errorf("%s prompt lost the exact native search or authored output tool", name)
	}
	reply := llmapi.Reply{ID: fmt.Sprintf("search-response-%d", request.Number), Model: "gpt-compatible-mini", InputTokens: 5, OutputTokens: 3, TotalTokens: 8}
	if searchResult == "" {
		args, _ := json.Marshal(map[string]any{"query": name + " query", "max_results": 2})
		reply.Tools = []llmapi.ToolCall{{ID: "search-" + name, Name: "web_search", Arguments: args}}
		return reply, nil
	}
	if searchCallID != "search-"+name {
		return llmapi.Reply{}, fmt.Errorf("%s tool continuation changed its call identity", name)
	}
	var result struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal([]byte(searchResult), &result); err != nil || len(result.Results) != 1 || !reflect.DeepEqual(result.Results[0], providerSearchResult(name)) {
		return llmapi.Reply{}, fmt.Errorf("%s tool continuation lost exact normalized search result", name)
	}
	args, _ := json.Marshal(result.Results[0])
	reply.Tools = []llmapi.ToolCall{{ID: "output-" + name, Name: "emit_search_" + name + "_completed", Arguments: args}}
	return reply, nil
}

func providerSearchResult(name string) map[string]any {
	return map[string]any{"title": name + " title", "url": "https://result.test/" + name, "snippet": name + " snippet"}
}

func requireProviderSearchPublicResult(t *testing.T, endpoint, runID, name string) {
	t.Helper()
	eventName := name + "/search." + name + "_completed"
	observerID := identitytest.RootNode(t, "observer").Key()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": eventName}, "limit": 100}, &page)
		if len(page.Events) > 0 {
			if len(page.Events) != 1 || page.Events[0].EventID == "" || page.Events[0].RunID != runID || page.Events[0].EventName != eventName || !reflect.DeepEqual(page.Events[0].Payload, providerSearchResult(name)) {
				t.Fatalf("%s search public result differs from its exact normalized output: %+v", name, page)
			}
			if len(page.Events[0].Deliveries) > 1 || len(page.Events[0].DeadLetters) > 0 {
				t.Fatalf("%s search has unexpected output deliveries: %+v", name, page)
			}
			if len(page.Events[0].Deliveries) == 1 {
				delivery := page.Events[0].Deliveries[0]
				if delivery.SubscriberType != "node" || delivery.SubscriberID != observerID || delivery.Failure != nil || len(delivery.DeadLetters) != 0 {
					t.Fatalf("%s search output lost its exact observer: %+v", name, delivery)
				}
				if delivery.Status == "delivered" {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s search did not publish its normalized result through serve", name)
}

func writeProviderSearchJourneySource(t *testing.T, customURL string) string {
	t.Helper()
	root := t.TempDir()
	inputs, connections := []string{}, []map[string]any{}
	events, handlers := map[string]any{}, map[string]any{}
	write := func(path string, value any) {
		body, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), string(body))
	}
	manifest := map[string]any{"version": "1.0.0", "platform_version": "*"}
	write("manifest.yaml", manifest)
	for _, name := range providerSearchNames {
		input, output := "search."+name+"_requested", "search."+name+"_completed"
		inputs = append(inputs, input, output)
		events[input] = map[string]any{"query": "string"}
		connections = append(connections, map[string]any{"event": input, "from": ".", "to": name}, map[string]any{"event": output, "from": name, "to": "."})
		handlers[output] = map[string]any{"guard": map[string]any{"id": "observe_" + name, "check": true}}
		write(name+"/manifest.yaml", manifest)
		write(name+"/events.yaml", map[string]any{output: map[string]any{"title": "string", "url": "string", "snippet": "string"}})
		write(name+"/schema.yaml", map[string]any{"pins": map[string]any{"inputs": []string{input}, "outputs": []string{output}}})
		write(name+"/agents.yaml", map[string]any{"researcher": map[string]any{
			"role": "researcher", "model": "regular", "intent": map[string]any{"inline": "Search using web_search, then call emit_search_" + name + "_completed with title, url and snippet from the normalized result."},
			"native_tools": map[string]any{"web_search": true}, "subscriptions": []string{input}, "emit_events": []string{output},
		}})
		policy := map[string]any{"provider": name, "credentials_key": "search_" + name + "_key", "max_results_default": 2}
		if name == "custom" {
			policy["http"] = map[string]any{"method": "GET", "url": customURL + "/custom?q={{input.query}}&count={{input.max_results}}", "headers": map[string]string{"X-Search-Key": "{{credentials.search_custom_key}}"}}
			policy["response_path"] = "items"
			policy["field_mapping"] = map[string]string{"title": "title", "url": "link", "snippet": "summary"}
		}
		write(name+"/policy.yaml", map[string]any{"web_search_provider": policy})
	}
	write("events.yaml", events)
	write("schema.yaml", map[string]any{"pins": map[string]any{"inputs": inputs}, "connect": connections})
	write("nodes.yaml", map[string]any{"observer": map[string]any{"execution_type": "system_node", "event_handlers": handlers}})
	return root
}
