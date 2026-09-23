package llm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestProviderStartSessionConsumesTypedReleaseOutcome(t *testing.T) {
	for _, provider := range []string{"anthropic", "cli", "compatible", "responses", "mock"} {
		for _, acknowledged := range []bool{false, true} {
			name := "unacknowledged"
			if acknowledged {
				name = "acknowledged"
			}
			t.Run(provider+"/"+name, func(t *testing.T) {
				fault := errors.New("release cleanup failed after commit")
				registry := &releaseOutcomeTestRegistry{InMemoryRegistry: sessions.NewInMemoryRegistry(time.Minute), acknowledged: acknowledged, failure: fault}
				publisher := &eventPublisherStub{}
				identity := testMemoryIdentity("agent-1", "support/instance-1")
				ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
				cfg := &config.Config{}
				var runtime sessionStarter
				switch provider {
				case "anthropic":
					runtime = NewAnthropicAPIRuntime(cfg, registry, "worker-1", nil, publisher)
				case "cli":
					runtime = NewClaudeCLIRuntime(cfg, registry, "worker-1", nil, nil, publisher)
				case "compatible":
					runtime = NewOpenAICompatibleRuntime(cfg, registry, "worker-1", nil, publisher)
				case "responses":
					runtime = NewOpenAIResponsesRuntime(cfg, registry, "worker-1", nil, publisher)
				case "mock":
					ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{
						ID: identity.AgentID(), ExecutionMode: "mock",
						Mock: mockperformance.Performance{Kind: "python", Source: []byte("def handle(input): return {}"), Digest: "test-module"},
					})
					runtime = NewMockRuntime(cfg, registry, "worker-1", nil, publisher, nil)
				}
				session, err := runtime.StartSession(ctx, identity.AgentID(), "system", nil)
				if registry.releases != 1 {
					t.Fatalf("typed release calls=%d, want 1", registry.releases)
				}
				if !acknowledged {
					if session != nil || !errors.Is(err, fault) || len(publisher.events) != 0 || len(publisher.runtimeLogs) != 0 {
						t.Fatalf("unacknowledged release started session=%+v err=%v events=%d logs=%d", session, err, len(publisher.events), len(publisher.runtimeLogs))
					}
					if _, err := registry.Acquire(ctx, identity, "worker-2"); err == nil {
						t.Fatal("unacknowledged release cleared the lease")
					}
					return
				}
				if err != nil || session == nil || session.MemoryIdentity != identity {
					t.Fatalf("acknowledged release lost session=%+v err=%v events=%d", session, err, len(publisher.events))
				}
				if len(publisher.runtimeLogs) == 0 || publisher.runtimeLogs[0].Action != "session_release_postcommit_failed" || publisher.runtimeLogs[0].Failure == nil {
					t.Fatalf("postcommit release error not diagnosed: %+v", publisher.runtimeLogs)
				}
				if _, err := registry.Acquire(ctx, identity, "worker-2"); err != nil {
					t.Fatalf("acknowledged release kept lease: %v", err)
				}
			})
		}
	}
}

func TestCompletedSessionReleaseRespectsAcknowledgementAndPriorError(t *testing.T) {
	providerFailure := errors.New("provider failed")
	cleanupFailure := errors.New("release cleanup failed after commit")
	for _, tc := range []struct {
		name         string
		acknowledged bool
		cancel       bool
		priorErr     error
		wantProvider bool
		wantCleanup  bool
		wantLog      bool
	}{
		{name: "acknowledged_completion", acknowledged: true, wantLog: true},
		{name: "acknowledged_canceled_completion", acknowledged: true, cancel: true, wantLog: true},
		{name: "acknowledged_provider_failure", acknowledged: true, priorErr: providerFailure, wantProvider: true, wantCleanup: true},
		{name: "unacknowledged_completion", wantCleanup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := &releaseOutcomeTestRegistry{InMemoryRegistry: sessions.NewInMemoryRegistry(time.Minute), acknowledged: tc.acknowledged, failure: cleanupFailure}
			identity := testMemoryIdentity("agent-1", "support/instance-1")
			base := context.Background()
			lease, err := registry.Acquire(base, identity, "worker-1")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			if tc.cancel {
				cancel()
			}
			publisher := &eventPublisherStub{}
			err = releaseCompletedSessionLease(ctx, registry, lease, identity.AgentID(), publisher, tc.priorErr)
			if errors.Is(err, providerFailure) != tc.wantProvider || errors.Is(err, cleanupFailure) != tc.wantCleanup {
				t.Fatalf("release error=%v, want provider=%v cleanup=%v", err, tc.wantProvider, tc.wantCleanup)
			}
			if tc.wantLog != (len(publisher.runtimeLogs) == 1 && publisher.runtimeLogs[0].Action == "session_release_postcommit_failed") {
				t.Fatalf("release diagnostic=%+v, want=%v", publisher.runtimeLogs, tc.wantLog)
			}
			if registry.releases != 1 {
				t.Fatalf("release calls=%d, want one", registry.releases)
			}
			_, reacquireErr := registry.Acquire(base, identity, "worker-2")
			if tc.acknowledged != (reacquireErr == nil) {
				t.Fatalf("lease acknowledgement=%v reacquire=%v", tc.acknowledged, reacquireErr)
			}
		})
	}
}
