package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

// Dispatch only: each existing harness retains its own immutable actor token
// and owns authorization/settlement. No harness or session is shared by actors.
type credentialCacheActorEffects map[string]*effecttest.Harness

func (s credentialCacheActorEffects) actor(id string) *effecttest.Harness {
	harness := s[id]
	if harness == nil {
		panic("credential cache fixture received an unknown actor")
	}
	return harness
}

func (s credentialCacheActorEffects) IsExternalEffectAuthorityCurrent(ctx context.Context, a runtimeeffects.Authority) (bool, error) {
	return s.actor(a.Normal.AgentID).IsExternalEffectAuthorityCurrent(ctx, a)
}
func (s credentialCacheActorEffects) AuthorizeExternalAttempt(ctx context.Context, a runtimeeffects.Authority, r runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	return s.actor(a.Normal.AgentID).AuthorizeExternalAttempt(ctx, a, r)
}
func (s credentialCacheActorEffects) MarkExternalAttemptLaunched(ctx context.Context, a runtimeeffects.Attempt, at time.Time) (runtimeeffects.ExternalAttemptLaunch, error) {
	return s.actor(a.Token.AgentID).MarkExternalAttemptLaunched(ctx, a, at)
}
func (s credentialCacheActorEffects) MarkExternalAttemptResponseObserved(ctx context.Context, a runtimeeffects.Attempt, evidence map[string]any, at time.Time) error {
	return s.actor(a.Token.AgentID).MarkExternalAttemptResponseObserved(ctx, a, evidence, at)
}
func (s credentialCacheActorEffects) SettleExternalAttempt(ctx context.Context, settlement runtimeeffects.Settlement) error {
	token, _ := runtimeeffects.LifecycleTokenFromContext(ctx)
	return s.actor(token.AgentID).SettleExternalAttempt(ctx, settlement)
}
func (s credentialCacheActorEffects) SettleCompletion(ctx context.Context, a runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	return s.actor(a.Token.AgentID).SettleCompletion(ctx, a, settlement)
}
func (s credentialCacheActorEffects) HeartbeatCompletionAttempt(ctx context.Context, a runtimeeffects.Attempt, at time.Time, lease time.Duration) error {
	return s.actor(a.Token.AgentID).HeartbeatCompletionAttempt(ctx, a, at, lease)
}
func (s credentialCacheActorEffects) RecoverCompletionContinuation(ctx context.Context, r runtimeeffects.CompletionContinuationRequest) (runtimeeffects.Attempt, bool, error) {
	return s.actor(r.Authority.Normal.AgentID).RecoverCompletionContinuation(ctx, r)
}
func (s credentialCacheActorEffects) ProjectCompletionConversation(ctx context.Context, a runtimeeffects.Attempt, p runtimeeffects.CompletionConversationProjection) error {
	return s.actor(a.Token.AgentID).ProjectCompletionConversation(ctx, a, p)
}
func (s credentialCacheActorEffects) ConsumeCompletionResponse(ctx context.Context, a runtimeeffects.Attempt, continuation *agentframe.ToolContinuation) error {
	return s.actor(a.Token.AgentID).ConsumeCompletionResponse(ctx, a, continuation)
}
func (s credentialCacheActorEffects) ProjectCommittedCompletionSpend(ctx context.Context, p runtimeeffects.CompletionSpendProjection) {
	token, _ := runtimeeffects.LifecycleTokenFromContext(ctx)
	s.actor(token.AgentID).ProjectCommittedCompletionSpend(ctx, p)
}

type credentialCacheTransport struct {
	base             http.RoundTripper
	local            *url.URL
	endpoint         func() string
	expectedEndpoint string
}

type credentialCacheContinuationAcquirer struct {
	LiveSessionAcquirer
	ready   chan string
	release <-chan struct{}
}

func (a credentialCacheContinuationAcquirer) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, owner string) (*sessions.Lease, ConversationRecord, error) {
	lease, record, err := a.LiveSessionAcquirer.AcquireLiveSession(ctx, identity, owner)
	if err != nil {
		return lease, record, err
	}
	if _, providerTurn := providerTurnAuthorityFromContext(ctx); providerTurn {
		a.ready <- identity.AgentID()
		select {
		case <-a.release:
		case <-ctx.Done():
			return lease, record, ctx.Err()
		}
	}
	return lease, record, nil
}

func (r credentialCacheTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if r.endpoint() != r.expectedEndpoint {
		return nil, errors.New("provider endpoint changed during concurrent requests")
	}
	// Anthropic's normal constructor retains its official URL. Redirect the test
	// transport, not the runtime field, to exercise immutable construction.
	cloned := request.Clone(request.Context())
	local := *request.URL
	local.Scheme, local.Host = r.local.Scheme, r.local.Host
	cloned.URL = &local
	return r.base.RoundTrip(cloned)
}

func TestProviderCredentialCacheColdConcurrentManagedSessionsAllAPIAdapters(t *testing.T) {
	for _, adapter := range credentialCacheAPIAdapters {
		t.Run(adapter.backend, func(t *testing.T) {
			profile := mustAdmissionProfile(t, adapter.backend)
			key, secret := ProviderCredentialKey(profile), "managed-cache-secret-fixture"
			resolverEntered, releaseResolver := make(chan struct{}), make(chan struct{})
			var resolverReleaseOnce sync.Once
			releaseCredential := func() { resolverReleaseOnce.Do(func() { close(releaseResolver) }) }
			probe := &credentialCacheReadProbe{Store: testProviderCredentialResolver(t, key, secret).Store}
			probe.beforeGet = func(ctx context.Context, count int32) error {
				if count != 1 {
					return nil
				}
				close(resolverEntered)
				select {
				case <-releaseResolver:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			resolver := NewProviderCredentialResolverWithEnvLookup(probe, func(string) (string, bool) { return "ignored-env-fixture", true })
			type observation struct{ headerOK, pathOK bool }
			observed := make(chan observation, 2)
			releaseHTTP := make(chan struct{})
			var httpReleaseOnce sync.Once
			releaseRequests := func() { httpReleaseOnce.Do(func() { close(releaseHTTP) }) }
			var requests, active, peak atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				current := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); old < current && !peak.CompareAndSwap(old, current); old = peak.Load() {
				}
				want := secret
				if adapter.header == "authorization" {
					want = "Bearer " + secret
				}
				// Send only boolean evidence; failures must never print auth material.
				observed <- observation{request.Header.Get(adapter.header) == want, request.URL.Path == adapter.path}
				select {
				case <-releaseHTTP:
				case <-request.Context().Done():
					return
				}
				w.Header().Set("content-type", "application/json")
				_, _ = w.Write([]byte(adapter.response))
			}))
			defer server.Close()
			defer releaseRequests()
			defer releaseCredential()
			registry := atomicLiveSessionTestRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute)}
			acquired, releaseAcquisition := make(chan string, 2), make(chan struct{})
			var acquisitionReleaseOnce sync.Once
			releaseSessions := func() { acquisitionReleaseOnce.Do(func() { close(releaseAcquisition) }) }
			defer releaseSessions()
			acquirer := credentialCacheContinuationAcquirer{LiveSessionAcquirer: registry, ready: acquired, release: releaseAcquisition}
			baseURL := server.URL + "/v1"
			if adapter.file == "openai_responses_runtime.go" {
				baseURL = ""
			}
			runtime, cache, endpoint := credentialCacheRuntime(t, adapter.backend, baseURL, registry, resolver)
			originalEndpoint := endpoint()
			if originalEndpoint == "" {
				t.Fatal("normal constructor left a reachable lazy endpoint initialization")
			}
			local, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: credentialCacheTransport{base: server.Client().Transport, local: local, endpoint: endpoint, expectedEndpoint: originalEndpoint}}
			effects := credentialCacheActorEffects{}
			conversations := make([]*Conversation, 2)
			contexts := make([]context.Context, 2)
			for i, actorID := range []string{"cache-agent-a", "cache-agent-b"} {
				harness := effecttest.New()
				contexts[i] = testManagedConversationContext(t, harness, actorID, "support/"+actorID, "support")
				actor, _ := runtimeactors.ActorFromContext(contexts[i])
				actor.Model = "regular"
				contexts[i] = runtimeactors.WithActor(contexts[i], actor)
				effects[actorID] = harness
				conversations[i] = newTestManagedConversation(t, actorID, "support/"+actorID, "support", nil, testMemory(), 3, runtime)
				conversations[i].SetToolExecutor(openAIToolExecutor{})
			}
			controller := liveTestCompletionController(effects, effects, effects, effects)
			switch r := runtime.(type) {
			case *AnthropicAPIRuntime:
				r.httpClient, r.completionController, r.liveSessions = client, controller, acquirer
			case *OpenAICompatibleRuntime:
				r.httpClient, r.completionController, r.liveSessions = client, controller, acquirer
			case *OpenAIResponsesRuntime:
				r.httpClient, r.completionController, r.liveSessions = client, controller, acquirer
			}
			for i, conversation := range conversations {
				ctx, cancel := context.WithCancel(contexts[i])
				defer cancel()
				contexts[i] = ctx
				if err := conversation.ensureSession(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if conversations[0].Session.ID == "" || conversations[0].Session.ID == conversations[1].Session.ID || conversations[0].Session.MemoryIdentity == conversations[1].Session.MemoryIdentity {
				t.Fatal("cold test did not construct two distinct managed sessions")
			}
			if cache.snapshot() != "" || probe.gets.Load() != 0 || requests.Load() != 0 {
				t.Fatal("managed sessions prewarmed the credential cache")
			}
			type outcome struct {
				response *Response
				err      error
			}
			ready, start, outcomes := make(chan struct{}, 2), make(chan struct{}), make(chan outcome, 2)
			for i, conversation := range conversations {
				go func() {
					ready <- struct{}{}
					<-start
					response, err := conversation.RunManaged(contexts[i], agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent(conversation.AgentID)})
					outcomes <- outcome{response, err}
				}()
			}
			for range conversations {
				<-ready
			}
			close(start)
			acquiredActors := map[string]bool{}
			for range conversations {
				select {
				case actor := <-acquired:
					acquiredActors[actor] = true
				case result := <-outcomes:
					t.Fatalf("managed admission failed before continued session acquire: %v", result.err)
				case <-time.After(5 * time.Second):
					t.Fatal("managed sessions did not overlap at continued acquire")
				}
			}
			if len(acquiredActors) != 2 || probe.gets.Load() != 0 {
				t.Fatal("continued-acquire barrier did not retain two cold managed sessions")
			}
			releaseSessions()
			select {
			case <-resolverEntered:
			case result := <-outcomes:
				t.Fatalf("managed admission failed before cold credential resolution: %v", result.err)
			case <-time.After(5 * time.Second):
				t.Fatal("cold managed calls did not resolve credentials")
			}
			if requests.Load() != 0 {
				t.Fatal("HTTP started before first credential resolution completed")
			}
			releaseCredential()
			for range conversations {
				select {
				case evidence := <-observed:
					if !evidence.headerOK || !evidence.pathOK {
						t.Fatal("provider request had incorrect stored credential or endpoint")
					}
				case result := <-outcomes:
					t.Fatalf("managed call completed before both HTTP requests overlapped: %v", result.err)
				case <-time.After(5 * time.Second):
					t.Fatal("shared cache serialized provider HTTP or failed managed admission")
				}
			}
			if peak.Load() != 2 || requests.Load() != 2 || probe.gets.Load() != 1 || probe.inspections.Load() != 1 {
				t.Fatalf("cold-cache overlap counts: peak=%d HTTP=%d credential_reads=%d source_reads=%d", peak.Load(), requests.Load(), probe.gets.Load(), probe.inspections.Load())
			}
			if endpoint() != originalEndpoint || cache.snapshot() != secret {
				t.Fatal("endpoint or successfully published credential changed across concurrent requests")
			}
			releaseRequests()
			for range conversations {
				select {
				case result := <-outcomes:
					if result.err != nil || result.response == nil || result.response.Message.Content != "done" {
						t.Fatalf("managed completion failed: %v", result.err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("managed provider completion did not settle")
				}
			}
			for _, conversation := range conversations {
				if conversation.Session.TurnCount != 1 {
					t.Fatal("managed session did not project exactly one completion")
				}
				adapterName := adapter.backend
				if adapter.file == "api_runtime.go" {
					adapterName = "anthropic_api"
				}
				settlements := effects[conversation.AgentID].CompletionSettlementsForAdapter(adapterName)
				if len(settlements) != 1 || settlements[0].AgentTurn == nil || !settlements[0].AgentTurn.ParseOK || !settlements[0].AgentTurn.Memory.Enabled {
					t.Fatal("managed session lost its exact completion evidence")
				}
				requireManagedSettlementProviderSelection(t, settlements)
			}
			if probe.gets.Load() != 1 || endpoint() != originalEndpoint {
				t.Fatal("settlement changed the runtime credential cache or endpoint")
			}
		})
	}
}
