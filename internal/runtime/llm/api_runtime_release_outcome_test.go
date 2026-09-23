package llm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type releaseOutcomeTestRegistry struct {
	*sessions.InMemoryRegistry
	acknowledged bool
	failure      error
	releases     int
}

func (r *releaseOutcomeTestRegistry) ReleaseOutcome(ctx context.Context, lease *sessions.Lease) (sessions.ReleaseResult, error) {
	r.releases++
	if !r.acknowledged {
		return sessions.ReleaseResult{}, r.failure
	}
	result, err := r.InMemoryRegistry.ReleaseOutcome(ctx, lease)
	return result, errors.Join(err, r.failure)
}

func TestAnthropicStartSessionConsumesAcknowledgedReleaseError(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		name := "unacknowledged"
		if acknowledged {
			name = "acknowledged"
		}
		t.Run(name, func(t *testing.T) {
			failure := errors.New("release postcommit handoff failed")
			registry := &releaseOutcomeTestRegistry{InMemoryRegistry: sessions.NewInMemoryRegistry(time.Minute), acknowledged: acknowledged, failure: failure}
			publisher := &eventPublisherStub{}
			identity := testMemoryIdentity("agent-1", "support/instance-1")
			ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
			runtime := NewAnthropicAPIRuntime(&config.Config{}, registry, "worker-1", nil, publisher)
			session, err := runtime.StartSession(ctx, identity.AgentID(), "system", nil)
			if registry.releases != 1 {
				t.Fatalf("release calls=%d, want one", registry.releases)
			}
			if acknowledged {
				if err != nil || session == nil || session.MemoryIdentity != identity {
					t.Fatalf("acknowledged start session=%+v err=%v events=%d", session, err, len(publisher.events))
				}
				var diagnosed bool
				for _, entry := range publisher.runtimeLogs {
					if entry.Action == "session_release_postcommit_failed" && entry.Failure != nil {
						detail, ok := entry.Detail.(map[string]any)
						if !ok || detail["postcommit_error"] != failure.Error() {
							continue
						}
						diagnosed = true
					}
				}
				if !diagnosed {
					t.Fatalf("postcommit error not diagnosed: %+v", publisher.runtimeLogs)
				}
				if _, err := registry.Acquire(ctx, identity, "worker-2"); err != nil {
					t.Fatalf("acknowledged release kept lease: %v", err)
				}
			} else {
				if session != nil || !errors.Is(err, failure) || len(publisher.events) != 0 || len(publisher.runtimeLogs) != 0 {
					t.Fatalf("unacknowledged start session=%+v err=%v events=%d logs=%d", session, err, len(publisher.events), len(publisher.runtimeLogs))
				}
				if _, err := registry.Acquire(ctx, identity, "worker-2"); err == nil {
					t.Fatalf("unacknowledged release unexpectedly freed lease: %v", err)
				}
			}
		})
	}
}
