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

type rotationOutcomeRegistry struct {
	sessions.Registry
	acquireErr, rotateErr, releaseErr error
	uncommitted                       bool
	cancel                            context.CancelFunc
	acquireCancel, releaseCancel      context.CancelFunc
	committed                         *sessions.Lease
	acquires, rotations, releases     int
}

func (r *rotationOutcomeRegistry) Acquire(ctx context.Context, id agentmemory.Identity, owner string) (*sessions.Lease, error) {
	r.acquires++
	lease, err := r.Registry.Acquire(ctx, id, owner)
	if err != nil {
		return lease, err
	}
	if r.acquireCancel != nil {
		r.acquireCancel()
	}
	return lease, r.acquireErr
}

func (r *rotationOutcomeRegistry) Rotate(ctx context.Context, id agentmemory.Identity, owner string, metadata sessions.RotationMetadata) (*sessions.Lease, error) {
	r.rotations++
	if r.uncommitted {
		return nil, r.rotateErr
	}
	lease, err := r.Registry.Rotate(ctx, id, owner, metadata)
	if err != nil {
		return lease, err
	}
	r.committed = lease
	if r.cancel != nil {
		r.cancel()
	}
	return lease, r.rotateErr
}

func (r *rotationOutcomeRegistry) Release(ctx context.Context, lease *sessions.Lease) error {
	r.releases++
	if r.releaseCancel != nil {
		r.releaseCancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// PostgreSQL release matches the exact active session, unlike the memory registry.
	if r.committed != nil && lease.SessionID != r.committed.SessionID {
		return errors.New("attempted predecessor release after committed rotation")
	}
	return errors.Join(r.Registry.Release(ctx, lease), r.releaseErr)
}

func TestSessionRotationRetainsAcknowledgedLeaseAndErrors(t *testing.T) {
	for _, method := range []string{"turn", "parse", "prepare"} {
		for _, committed := range []bool{false, true} {
			name := "uncommitted"
			if committed {
				name = "committed"
			}
			t.Run(method+"/"+name, func(t *testing.T) {
				failure, cleanup := errors.New("rotation failed"), errors.New("release failed after cleanup")
				registry := &rotationOutcomeRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute), rotateErr: failure, uncommitted: !committed}
				identity := testMemoryIdentity("agent-1", "support/instance-1")
				ctx, cancel := context.WithCancel(agentmemory.WithExecution(context.Background(), testMemory(), identity))
				defer cancel()
				lease, err := registry.Registry.Acquire(ctx, identity, "worker-1")
				if err != nil {
					t.Fatal(err)
				}
				if err := registry.Registry.Release(ctx, lease); err != nil {
					t.Fatal(err)
				}
				session := &Session{ID: lease.SessionID, ProviderSessionID: "provider-old", AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity, TurnCount: 1, ParseFailures: 1, Messages: []Message{{Role: "assistant", Content: "done"}}}
				var result *sessions.Lease
				switch method {
				case "turn":
					result, err = MaybeRotateAfterTurn(ctx, session, registry, "worker-1", 1, nil)
				case "parse":
					result, err = MaybeRotateAfterParseFailures(ctx, session, registry, "worker-1", 1, nil)
				case "prepare":
					registry.cancel, registry.releaseErr = cancel, cleanup
					err = prepareManagedSessionForTurn(ctx, session, registry, "worker-1", 1, nil)
				}
				if !errors.Is(err, failure) {
					t.Fatalf("lost rotation error: %v", err)
				}
				if registry.rotations != 1 {
					t.Fatalf("rotation attempts=%d", registry.rotations)
				}
				if committed {
					if session.ID != registry.committed.SessionID || session.ID == lease.SessionID || session.ProviderSessionID != "" || session.TurnCount != 0 || session.ParseFailures != 0 || len(session.Messages) != 1 || session.Messages[0].Role != "system" {
						t.Fatalf("committed rotation not adopted: %+v", session)
					}
					if method != "prepare" && result != registry.committed {
						t.Fatalf("committed lease discarded: %+v", result)
					}
				} else if result != nil || session.ID != lease.SessionID || session.ProviderSessionID != "provider-old" || session.TurnCount != 1 || session.ParseFailures != 1 {
					t.Fatalf("uncommitted rotation changed session: %+v, lease=%+v", session, result)
				}
				if method == "prepare" {
					if !errors.Is(err, cleanup) || registry.releases != 1 {
						t.Fatalf("cleanup lost or replayed: releases=%d err=%v", registry.releases, err)
					}
					current, err := registry.Registry.Acquire(context.Background(), identity, "worker-2")
					if err != nil {
						t.Fatalf("successor still leased: %v", err)
					}
					if current.SessionID != session.ID {
						t.Fatalf("released wrong session: %+v", current)
					}
				}
			})
		}
	}
}

func TestSessionPreparationAndProvidersReleaseAcknowledgedAcquireOnError(t *testing.T) {
	for _, name := range []string{"prepare", "anthropic", "cli", "compatible", "responses", "mock"} {
		t.Run(name, func(t *testing.T) {
			failure, cleanup := errors.New("acquire handoff failed"), errors.New("release handoff failed")
			registry := &rotationOutcomeRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute), acquireErr: failure, releaseErr: cleanup}
			identity := testMemoryIdentity("agent-1", "support/instance-1")
			ctx := agentmemory.WithExecution(context.Background(), testMemory(), identity)
			ctx = runtimeactors.WithActor(ctx, runtimeactors.AgentConfig{ID: identity.AgentID(), ExecutionMode: "mock", Mock: mockperformance.Performance{Kind: "python", Source: []byte("def handle(input): return {}"), Digest: "test-module"}})
			session := &Session{ID: "old", AgentID: identity.AgentID(), Memory: testMemory(), MemoryIdentity: identity, TurnCount: 1}
			cfg := &config.Config{}
			var err error
			switch name {
			case "prepare":
				err = prepareManagedSessionForTurn(ctx, session, registry, "worker-1", 1, nil)
			case "anthropic":
				_, err = NewAnthropicAPIRuntime(cfg, registry, "worker-1", nil, nil).continueSession(ctx, session, Message{}, nil)
			case "cli":
				_, err = NewClaudeCLIRuntime(cfg, registry, "worker-1", nil, nil, nil).continueSession(ctx, session, Message{}, nil)
			case "compatible":
				_, err = NewOpenAICompatibleRuntime(cfg, registry, "worker-1", nil, nil).continueSession(ctx, session, Message{}, nil)
			case "responses":
				_, err = NewOpenAIResponsesRuntime(cfg, registry, "worker-1", nil, nil).continueSession(ctx, session, Message{}, nil)
			case "mock":
				_, err = NewMockRuntime(cfg, registry, "worker-1", nil, nil, nil).continueSession(ctx, session, Message{}, nil)
			}
			if !errors.Is(err, failure) || !errors.Is(err, cleanup) {
				t.Fatalf("lost independent error: %v", err)
			}
			if registry.acquires != 1 || registry.rotations != 0 || registry.releases != 1 {
				t.Fatalf("unexpected operations: acquire=%d rotate=%d release=%d", registry.acquires, registry.rotations, registry.releases)
			}
			if _, err := registry.Registry.Acquire(ctx, identity, "worker-2"); err != nil {
				t.Fatalf("committed lease leaked: %v", err)
			}
		})
	}
}

func TestSessionStartReleasesCommittedAcquireOnErrorOrCancellation(t *testing.T) {
	for _, name := range []string{"anthropic", "cli", "compatible", "responses", "mock"} {
		for _, phase := range []string{"handoff_error", "canceled_after_acquire"} {
			t.Run(name+"/"+phase, func(t *testing.T) {
				failure, cleanup := errors.New("acquire handoff failed"), errors.New("release handoff failed")
				registry := &rotationOutcomeRegistry{Registry: sessions.NewInMemoryRegistry(time.Minute), releaseErr: cleanup}
				identity := testMemoryIdentity("agent-1", "support/instance-1")
				base := agentmemory.WithExecution(context.Background(), testMemory(), identity)
				base = runtimeactors.WithActor(base, runtimeactors.AgentConfig{ID: identity.AgentID(), ExecutionMode: "mock", Mock: mockperformance.Performance{Kind: "python", Source: []byte("def handle(input): return {}"), Digest: "test-module"}})
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				if phase == "handoff_error" {
					registry.acquireErr = failure
				} else {
					registry.acquireCancel = cancel
				}
				cfg, publisher := &config.Config{}, &eventPublisherStub{}
				var runtime sessionStarter
				switch name {
				case "anthropic":
					runtime = NewAnthropicAPIRuntime(cfg, registry, "worker-1", nil, publisher)
				case "cli":
					runtime = NewClaudeCLIRuntime(cfg, registry, "worker-1", nil, nil, publisher)
				case "compatible":
					runtime = NewOpenAICompatibleRuntime(cfg, registry, "worker-1", nil, publisher)
				case "responses":
					runtime = NewOpenAIResponsesRuntime(cfg, registry, "worker-1", nil, publisher)
				case "mock":
					runtime = NewMockRuntime(cfg, registry, "worker-1", nil, publisher, nil)
				}
				result, err := runtime.StartSession(ctx, identity.AgentID(), "system", nil)
				if result != nil || !errors.Is(err, cleanup) || phase == "handoff_error" && !errors.Is(err, failure) {
					t.Fatalf("start result=%+v err=%v", result, err)
				}
				if registry.acquires != 1 || registry.releases != 1 || len(publisher.events) != 0 || len(publisher.marks) != 0 {
					t.Fatalf("unexpected startup work: acquire=%d release=%d events=%d marks=%d", registry.acquires, registry.releases, len(publisher.events), len(publisher.marks))
				}
				if _, err := registry.Registry.Acquire(base, identity, "worker-2"); err != nil {
					t.Fatalf("committed startup lease leaked: %v", err)
				}
			})
		}
	}
}
