package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type providerSchemaEffectTools struct{}

func (providerSchemaEffectTools) Execute(context.Context, string, any) (any, error) {
	panic("schema proof must not execute tools")
}

func (providerSchemaEffectTools) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{Name: name, Visible: true, Callable: true})
	}
	return toolcapabilities.NewSet(caps)
}

type providerSchemaLocalTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (p providerSchemaLocalTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	endpoint := *request.URL
	endpoint.Scheme, endpoint.Host = p.target.Scheme, p.target.Host
	cloned.URL = &endpoint
	return p.base.RoundTrip(cloned)
}

func TestProviderSchemaEffectRefusalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := providerSchemaStoreFixture(t, backend)
			var sends atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sends.Add(1)
				_, _ = w.Write([]byte(`{"model":"test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`))
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			original := http.DefaultTransport
			http.DefaultTransport = providerSchemaLocalTransport{base: original, target: target}
			defer func() { http.DefaultTransport = original }()
			ctx := providerSchemaStoreContext(t, fixture)
			for _, schema := range []any{
				map[string]any{"type": "unsupported-provider-type"}, map[string]any{"type": " object "},
				map[string]any(nil), map[string]any{"properties": map[string]any(nil)},
				map[string]any{"type": []any{"object", "object"}}, map[string]any{"required": []any{"q", "q"}},
				map[string]any{"type": "string"},
			} {
				bad := []runtimellm.ToolDefinition{{Name: "inspect", Schema: schema}}
				conversation := providerSchemaStoreConversation(t, fixture, bad)
				response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)})
				want := `anthropic tool "inspect" input_schema must declare type "object"`
				if canonical := runtimellm.ValidateProviderToolDefinitions(bad); canonical != nil {
					want = canonical.Error()
				}
				if response != nil || err == nil || !strings.Contains(err.Error(), want) || sends.Load() != 0 {
					failure, _ := failures.As(err)
					for cause := error(failure); cause != nil; cause = errors.Unwrap(cause) {
						t.Logf("refusal cause: %T %v", cause, cause)
					}
					t.Fatalf("final schema gate not reached: schema=%v response=%v err=%v sends=%d want=%s", schema, response, err, sends.Load(), want)
				}
			}
			attempts, err := ReadExternalAttemptStorageForTest(ctx, fixture.store)
			if err != nil || len(attempts) != 0 {
				t.Fatalf("refusal created attempts: %+v err=%v", attempts, err)
			}
			effects, err := ReadManagedTurnEffectStorageForTest(ctx, fixture.store, fixture.authority.Target.RunID, fixture.agentID)
			if err != nil || effects != (ManagedTurnEffectStorage{}) {
				t.Fatalf("refusal created turn/effect facts: %+v err=%v", effects, err)
			}
			// A fresh runtime/conversation reconstructs the same admitted source after
			// the refusal. The selected lease must have been released exactly enough
			// to admit this valid successor, without inventing a new session identity.
			good := []runtimellm.ToolDefinition{{Name: "inspect", Schema: map[string]any{"type": "object", "properties": map[string]any{}}}}
			conversation := providerSchemaStoreConversation(t, fixture, good)
			response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: managedCompletionTestEvent(fixture.authority)})
			if err != nil || response == nil || response.Message.Content != "done" || sends.Load() != 1 || conversation.Session.ID != fixture.sessionID {
				t.Fatalf("valid reconstructed request failed: response=%v err=%v sends=%d session=%s", response, err, sends.Load(), conversation.Session.ID)
			}
			effects, err = ReadManagedTurnEffectStorageForTest(ctx, fixture.store, fixture.authority.Target.RunID, fixture.agentID)
			if err != nil || effects.Settled != 1 || effects.ResponseConsumed != 1 || effects.ProviderAttempts != 1 {
				t.Fatalf("valid request did not settle exactly once: %+v err=%v", effects, err)
			}
		})
	}
}

func TestProviderSchemaReconstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := providerSchemaStoreFixture(t, backend)
			input, err := contracts.AdmitToolInputSchemaMap(map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []any{"q"}})
			if err != nil {
				t.Fatal(err)
			}
			definition := runtimellm.ToolDefinition{Name: "inspect", Schema: input.Projection()}
			identity := runtimellm.ToolDefinitionIdentity(definition)
			var sends atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Tools []struct {
						Name   string         `json:"name"`
						Schema map[string]any `json:"input_schema"`
					} `json:"tools"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Tools) != 1 || runtimellm.ToolDefinitionIdentity(runtimellm.ToolDefinition{Name: request.Tools[0].Name, Schema: request.Tools[0].Schema}) != identity {
					t.Errorf("reconstruction changed request definition: %+v err=%v", request, err)
				}
				sends.Add(1)
				_, _ = w.Write([]byte(`{"model":"test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`))
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			original := http.DefaultTransport
			http.DefaultTransport = providerSchemaLocalTransport{base: original, target: target}
			defer func() { http.DefaultTransport = original }()
			ctx := providerSchemaStoreContext(t, fixture)
			for ordinal := 1; ordinal <= 2; ordinal++ {
				event := managedCompletionTestEvent(fixture.authority)
				if ordinal == 2 {
					event = managedCompletionTestEventWithIdentity(fixture.authority, uuid.NewString(), "completion.test.requested")
					claim := claimCompletionOriginEventForTest(t, ctx, fixture.store, fixture.authority, event)
					ctx = runtimecorrelation.WithInboundEvent(runtimedelivery.WithClaim(ctx, claim), event)
				}
				// Reconstruct from the immutable source projection, not the previous
				// runtime's mutable Session.Tools or in-process conversation state.
				conversation := providerSchemaStoreConversation(t, fixture, []runtimellm.ToolDefinition{{Name: definition.Name, Schema: input.Projection()}})
				response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event})
				if err != nil || response == nil || response.Message.Content != "done" || sends.Load() != int32(ordinal) || conversation.Session.ID != fixture.sessionID || conversation.TurnCount != ordinal {
					t.Fatalf("reconstructed turn %d: response=%v err=%v sends=%d session=%s count=%d", ordinal, response, err, sends.Load(), conversation.Session.ID, conversation.TurnCount)
				}
			}
			effects, err := ReadManagedTurnEffectStorageForTest(ctx, fixture.store, fixture.authority.Target.RunID, fixture.agentID)
			if err != nil || effects.Settled != 2 || effects.ResponseConsumed != 2 || effects.ProviderAttempts != 2 {
				t.Fatalf("reconstructed completion cardinality: %+v err=%v", effects, err)
			}
		})
	}
}

func providerSchemaStoreFixture(t *testing.T, backend string) completionSettlementFixture {
	t.Helper()
	var store completionSettlementTestStore
	var db *sql.DB
	if backend == "sqlite" {
		sqlite := newBootstrappedSQLiteRuntimeStoreForTest(t)
		store, db = sqlite, sqlite.backend.ConstructionHandle()
	} else {
		fixture := openPostgresAuthorActivityReceiptFixture(t)
		store, db = fixture.store.(completionSettlementTestStore), fixture.db
	}
	return newCompletionSettlementFixtureWithActor(t, store, db, backend == "sqlite", agentmemory.Plan{Enabled: true}, "global", executionposture.Live,
		runtimeactors.AgentConfig{ExecutionMode: "live", LLMBackend: "anthropic", ResolvedLLMBackend: "anthropic"})
}

func providerSchemaStoreContext(t *testing.T, fixture completionSettlementFixture) context.Context {
	t.Helper()
	ctx := managedExecutionStoreTestContext(t, runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal))
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: "provider-schema-store", BundleHash: "provider-schema-store"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		wait, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(wait); err != nil {
			t.Errorf("join schema runtime: %v", err)
		}
		if _, err := process.Join(wait); err != nil {
			t.Errorf("join schema process: %v", err)
		}
	})
	ctx = worklifetime.WithOccurrence(worklifetime.WithProcess(ctx, process), owner)
	ctx = runtimedelivery.WithClaim(ctx, fixture.origin)
	ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{ID: fixture.agentID, Identity: fixture.authority.Normal.Identity,
		Role: "worker", Type: "managed", ExecutionMode: "live", Model: "regular", LLMBackend: "anthropic",
		Memory: fixture.authority.Target.Memory, FlowID: "global", FlowPath: "global"})
	ctx = agentmemory.WithExecution(ctx, fixture.authority.Target.Memory, fixture.authority.Target.AgentIdentity)
	return runtimecorrelation.WithInboundEvent(ctx, managedCompletionTestEvent(fixture.authority))
}

func providerSchemaStoreConversation(t *testing.T, fixture completionSettlementFixture, tools []runtimellm.ToolDefinition) *runtimellm.Conversation {
	t.Helper()
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}}
	controller := liveTestCompletionController(fixture.store, fixture.store, fixture.store, publicationGroupSpendProjection{})
	runtime, err := (runtimellm.RuntimeFactory{Cfg: cfg, Sessions: fixture.store.(sessions.Registry),
		LiveSessions: fixture.store.(runtimellm.LiveSessionAcquirer), Conversations: fixture.store.(runtimellm.ConversationPersistence),
		LockOwner: fixture.leaseHolder, Credentials: mapCredentialStore{"ANTHROPIC_API_KEY": "local-key"}, CompletionController: controller}).Build()
	if err != nil {
		t.Fatal(err)
	}
	settlement := completionSettlementForTest(t, fixture.authority.Target, fixture, "anthropic_api", "", "")
	conversation := managedConsumerWithRuntime(t, fixture, settlement, runtime)
	conversation.Tools, conversation.Session.Tools = tools, tools
	conversation.SetToolExecutor(providerSchemaEffectTools{})
	return conversation
}
