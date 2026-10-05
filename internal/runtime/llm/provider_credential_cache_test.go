package llm

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

var credentialCacheAPIAdapters = []struct {
	backend, file, header, path, response string
}{
	{llmselection.BackendAnthropic, "api_runtime.go", "x-api-key", "/v1/messages", `{"model":"test-model","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`},
	{llmselection.BackendOpenAICompatible, "openai_compatible_runtime.go", "authorization", "/v1/chat/completions", `{"model":"test-model","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`},
	{llmselection.BackendOpenAIResponses, "openai_responses_runtime.go", "authorization", "/v1/responses", `{"id":"resp_cache","model":"test-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`},
}

// The real file store retains source/precedence behavior; this read-only wrapper
// observes initialization without substituting a new credential resolver.
type credentialCacheReadProbe struct {
	runtimecredentials.Store
	gets, inspections atomic.Int32
	beforeGet         func(context.Context, int32) error
	beforeInspect     func(context.Context, int32) error
}

func (s *credentialCacheReadProbe) Get(ctx context.Context, key string) (string, bool, error) {
	count := s.gets.Add(1)
	if s.beforeGet != nil {
		if err := s.beforeGet(ctx, count); err != nil {
			return "", false, err
		}
	}
	return s.Store.Get(ctx, key)
}

func (s *credentialCacheReadProbe) Inspect(ctx context.Context, key string) (runtimecredentials.Metadata, error) {
	count := s.inspections.Add(1)
	if s.beforeInspect != nil {
		if err := s.beforeInspect(ctx, count); err != nil {
			return runtimecredentials.Metadata{}, err
		}
	}
	return s.Store.(runtimecredentials.Inspector).Inspect(ctx, key)
}

func credentialCacheRuntime(t *testing.T, backend, baseURL string, registry sessions.Registry, resolver ProviderCredentialResolver) (Runtime, *providerCredentialCache, func() string) {
	t.Helper()
	cfg := &config.Config{LLM: config.LLMConfig{
		Backend:          backend,
		Session:          config.LLMSessionConfig{LockTTL: time.Minute, RotateAfterTurns: 40, RotateOnParseFailures: 3},
		Models:           llmselection.ModelAliases{llmselection.ModelAliasRegular: {backend: "test-model"}},
		OpenAICompatible: config.OpenAICompatibleConfig{BaseURL: baseURL},
		OpenAIResponses:  config.OpenAIResponsesConfig{BaseURL: baseURL},
	}}
	// Complete normal typed-config admission before publishing the immutable
	// config to sessions. Credential resolution remains cold until the turn.
	if err := cfg.ValidateExtensions(); err != nil {
		t.Fatal(err)
	}
	switch backend {
	case llmselection.BackendAnthropic:
		r := NewAnthropicAPIRuntimeWithProviderCredentials(cfg, registry, "cache-test-owner", nil, nil, resolver)
		return r, &r.credentialCache, func() string { return r.apiURL }
	case llmselection.BackendOpenAICompatible:
		r := NewOpenAICompatibleRuntimeWithProviderCredentials(cfg, registry, "cache-test-owner", nil, nil, resolver)
		return r, &r.credentialCache, func() string { return r.baseURL }
	case llmselection.BackendOpenAIResponses:
		r := NewOpenAIResponsesRuntimeWithProviderCredentials(cfg, registry, "cache-test-owner", nil, nil, resolver)
		return r, &r.credentialCache, func() string { return r.baseURL }
	default:
		t.Fatalf("unexpected cache adapter %s", backend)
		return nil, nil, nil
	}
}

func requireCredentialCacheFailure(t *testing.T, err error, class runtimefailures.Class, code string) {
	t.Helper()
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != class || failure.Failure.Detail.Code != code {
		t.Fatalf("credential refusal lacks expected class/code: %v", err)
	}
}

func TestProviderCredentialCacheFailureCancellationRetryAndSuccessRetention(t *testing.T) {
	for _, adapter := range credentialCacheAPIAdapters {
		for _, failureMode := range []string{"read", "source", "canceled-before-read", "canceled-during-read", "missing-env-only"} {
			t.Run(adapter.backend+"/"+failureMode, func(t *testing.T) {
				profile := mustAdmissionProfile(t, adapter.backend)
				key := ProviderCredentialKey(profile)
				secret := "cache-success-fixture"
				stored := secret
				if failureMode == "missing-env-only" {
					stored = ""
				}
				probe := &credentialCacheReadProbe{Store: testProviderCredentialResolver(t, key, stored).Store}
				resolver := NewProviderCredentialResolverWithEnvLookup(probe, func(string) (string, bool) { return "ignored-env-fixture", true })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cause := errors.New("credential reader fault: confidential-cause-fixture")
				switch failureMode {
				case "read":
					probe.beforeGet = func(context.Context, int32) error { return cause }
				case "source":
					probe.beforeInspect = func(context.Context, int32) error { return cause }
				case "canceled-before-read":
					cancel()
				case "canceled-during-read":
					probe.beforeGet = func(context.Context, int32) error { cancel(); return nil }
				}
				_, cache, _ := credentialCacheRuntime(t, adapter.backend, "http://unused.test", sessions.NewInMemoryRegistry(time.Minute), resolver)
				err := cache.resolve(ctx, resolver, profile)
				if failureMode == "missing-env-only" {
					requireCredentialCacheFailure(t, err, runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing")
					var missing MissingProviderCredentialError
					if !errors.As(err, &missing) || !missing.EnvPresent || missing.Key != key {
						t.Fatal("missing credential lost typed env-only refusal")
					}
				} else {
					requireCredentialCacheFailure(t, err, runtimefailures.ClassDependencyUnavailable, "provider_credential_store_unavailable")
					if strings.HasPrefix(failureMode, "canceled") {
						if !errors.Is(err, context.Canceled) {
							t.Fatal("credential cancellation lost cause")
						}
					} else if !errors.Is(err, cause) {
						t.Fatal("credential store failure lost cause")
					}
				}
				if strings.Contains(err.Error(), "confidential-cause-fixture") || cache.snapshot() != "" {
					t.Fatal("failed resolution exposed or cached credential material")
				}
				if failureMode == "canceled-before-read" && probe.gets.Load() != 0 {
					t.Fatal("canceled first attempt read the credential store")
				}
				probe.beforeGet, probe.beforeInspect = nil, nil
				if err := probe.Store.Set(context.Background(), key, secret); err != nil {
					t.Fatal(err)
				}
				credential, err := resolver.Resolve(context.Background(), profile)
				if err != nil || credential.Source != runtimecredentials.SourceFile || !credential.EnvPresent || !credential.EnvShadowed || credential.Value != secret {
					t.Fatal("stored credential/source/env precedence was not preserved")
				}
				before := probe.gets.Load()
				if err := cache.resolve(context.Background(), resolver, profile); err != nil {
					t.Fatal(err)
				}
				if probe.gets.Load() != before+1 || cache.snapshot() != secret {
					t.Fatal("failed first attempt poisoned successful retry")
				}
				if err := probe.Store.Set(context.Background(), key, "replacement-fixture"); err != nil {
					t.Fatal(err)
				}
				before = probe.gets.Load()
				if err := cache.resolve(context.Background(), resolver, profile); err != nil || probe.gets.Load() != before || cache.snapshot() != secret {
					t.Fatal("successful cache was re-resolved or replaced")
				}
			})
		}
	}
}

func TestProviderCredentialCacheRuntimeAndProviderIsolation(t *testing.T) {
	for _, adapter := range credentialCacheAPIAdapters {
		profile := mustAdmissionProfile(t, adapter.backend)
		key := ProviderCredentialKey(profile)
		for _, value := range []string{"runtime-a-fixture", "runtime-b-fixture"} {
			resolver := testProviderCredentialResolver(t, key, value)
			_, cache, _ := credentialCacheRuntime(t, adapter.backend, "http://unused.test", sessions.NewInMemoryRegistry(time.Minute), resolver)
			if cache.snapshot() != "" {
				t.Fatal("new runtime inherited another runtime/provider credential")
			}
			if err := cache.resolve(context.Background(), resolver, profile); err != nil || cache.snapshot() != value {
				t.Fatal("runtime/provider cache leaked another credential")
			}
		}
	}
	profile := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	var cache providerCredentialCache
	err := cache.resolve(context.Background(), NewProviderCredentialResolverWithEnvLookup(nil, func(string) (string, bool) { return "", false }), profile)
	requireCredentialCacheFailure(t, err, runtimefailures.ClassDependencyUnavailable, "provider_credential_store_unavailable")
	if IsMissingProviderCredential(err) || cache.snapshot() != "" {
		t.Fatal("missing read port became a missing secret or successful cache")
	}
}

func TestProviderCredentialCacheOptionalProfileDoesNotInventCredential(t *testing.T) {
	var cache providerCredentialCache
	resolver := NewProviderCredentialResolverWithEnvLookup(nil, func(string) (string, bool) { return "ignored-env-fixture", true })
	optional := mustAdmissionProfile(t, llmselection.BackendMock)
	for attempt := 0; attempt < 2; attempt++ {
		if err := cache.resolve(context.Background(), resolver, optional); err != nil || cache.snapshot() != "" {
			t.Fatal("credentialless profile invented a cache entry or required a reader")
		}
	}
	required := mustAdmissionProfile(t, llmselection.BackendAnthropic)
	err := cache.resolve(context.Background(), resolver, required)
	requireCredentialCacheFailure(t, err, runtimefailures.ClassDependencyUnavailable, "provider_credential_store_unavailable")
}

func TestProviderCredentialCacheBaseURLConstructorCensus(t *testing.T) {
	for _, backend := range []string{llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses} {
		for _, raw := range []string{"", "localhost:8080", " https://example.test/v1/ "} {
			t.Run(backend+"/"+raw, func(t *testing.T) {
				profile := mustAdmissionProfile(t, backend)
				want, resolutionErr := llmselection.ResolveBaseURL(profile, raw)
				resolver := testProviderCredentialResolver(t, ProviderCredentialKey(profile), "constructor-fixture")
				runtime, _, endpoint := credentialCacheRuntime(t, backend, raw, sessions.NewInMemoryRegistry(time.Minute), resolver)
				if endpoint() != want {
					t.Fatal("constructor differs from canonical base-URL resolution")
				}
				if resolutionErr == nil {
					if endpoint() == "" {
						t.Fatal("admitted constructor leaves a reachable lazy base-URL write")
					}
					return
				}
				var requests atomic.Int32
				client := &http.Client{Transport: noInvocationRoundTripper{calls: &requests}}
				switch r := runtime.(type) {
				case *OpenAICompatibleRuntime:
					r.httpClient = client
				case *OpenAIResponsesRuntime:
					r.httpClient = client
				}
				contexts := []context.Context{withTestStatelessMemory(t, unmanagedLLMTestContext(), "url-agent-a", ""), withTestStatelessMemory(t, unmanagedLLMTestContext(), "url-agent-b", "")}
				for i, ctx := range contexts {
					actor, _ := runtimeactors.ActorFromContext(ctx)
					actor.Model = llmselection.ModelAliasRegular
					contexts[i] = runtimeactors.WithActor(ctx, actor)
				}
				sessionList := make([]*Session, 2)
				for i, ctx := range contexts {
					actor := []string{"url-agent-a", "url-agent-b"}[i]
					var err error
					sessionList[i], err = runtime.StartSession(ctx, actor, "system", nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				start, results := make(chan struct{}), make(chan error, 2)
				for i, ctx := range contexts {
					go func() {
						<-start
						var err error
						switch r := runtime.(type) {
						case *OpenAICompatibleRuntime:
							_, err = r.continueSession(ctx, sessionList[i], Message{Role: "user", Content: "hello"}, nil)
						case *OpenAIResponsesRuntime:
							_, err = r.continueSession(ctx, sessionList[i], Message{Role: "user", Content: "hello"}, nil)
						}
						results <- err
					}()
				}
				close(start)
				for range contexts {
					select {
					case err := <-results:
						if err == nil || err.Error() != resolutionErr.Error() {
							t.Fatalf("empty constructor did not return canonical URL error: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("base-URL refusal did not finish")
					}
				}
				if endpoint() != "" || requests.Load() != 0 {
					t.Fatal("invalid base-URL branch assigned an endpoint or reached HTTP")
				}
			})
		}
	}
}

func providerCredentialConsumerGuard(file string, source []byte, header string) error {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, source, 0)
	if err != nil {
		return err
	}
	var rawRead bool
	resolveCalls, headerSnapshots := 0, 0
	cacheMember := func(expr ast.Expr, member string) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != member {
			return false
		}
		cache, ok := selector.X.(*ast.SelectorExpr)
		if !ok || cache.Sel.Name != "credentialCache" {
			return false
		}
		receiver, ok := cache.X.(*ast.Ident)
		return ok && receiver.Name == "r"
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		if expr, ok := node.(ast.Expr); ok && cacheMember(expr, "value") {
			rawRead = true
		}
		return true
	})
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if function.Name.Name == "continueSession" && cacheMember(call.Fun, "resolve") {
				resolveCalls++
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if function.Name.Name != "sendRequest" || !ok || selector.Sel.Name != "Set" || len(call.Args) != 2 {
				return true
			}
			requestHeader, ok := selector.X.(*ast.SelectorExpr)
			if !ok || requestHeader.Sel.Name != "Header" {
				return true
			}
			request, ok := requestHeader.X.(*ast.Ident)
			if !ok || request.Name != "req" {
				return true
			}
			name, ok := call.Args[0].(*ast.BasicLit)
			if !ok || name.Value != `"`+header+`"` {
				return true
			}
			ast.Inspect(call.Args[1], func(node ast.Node) bool {
				if snapshot, ok := node.(*ast.CallExpr); ok && cacheMember(snapshot.Fun, "snapshot") {
					headerSnapshots++
				}
				return true
			})
			return true
		})
	}
	if rawRead {
		return errors.New(file + ": raw credential cache value access")
	}
	if resolveCalls != 1 || headerSnapshots != 1 {
		return errors.New(file + ": missing exact cache initialization/header snapshot consumer")
	}
	return nil
}

func TestProviderCredentialCacheFiniteConsumerGuardRejectsRawHeaderRead(t *testing.T) {
	for _, adapter := range credentialCacheAPIAdapters {
		t.Run(adapter.backend, func(t *testing.T) {
			source, err := os.ReadFile(adapter.file)
			if err != nil {
				t.Fatal(err)
			}
			if err := providerCredentialConsumerGuard(adapter.file, source, adapter.header); err != nil {
				t.Fatal(err)
			}
			negative := strings.Replace(string(source), "r.credentialCache.snapshot()", "r.credentialCache.value", 1)
			if negative == string(source) {
				t.Fatal("negative fixture did not restore a raw header read")
			}
			if err := providerCredentialConsumerGuard(adapter.file, []byte(negative), adapter.header); err == nil || !strings.Contains(err.Error(), "raw credential cache value") {
				t.Fatal("consumer guard accepted restored unsynchronized header read")
			}
			negative = strings.Replace(string(source), "r.credentialCache.resolve(", "r.credentials.Resolve(", 1)
			if negative == string(source) {
				t.Fatal("negative fixture did not remove cache initialization")
			}
			if err := providerCredentialConsumerGuard(adapter.file, []byte(negative), adapter.header); err == nil {
				t.Fatal("consumer guard accepted uncached initialization")
			}
		})
	}
}
