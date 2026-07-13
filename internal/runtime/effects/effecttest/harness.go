package effecttest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type Harness struct {
	mu                 sync.Mutex
	Token              runtimeeffects.LifecycleToken
	AuthorizeErr       error
	HeartbeatErr       error
	HeartbeatFailAfter int
	MarkErr            error
	SettleErr          error
	Heartbeats         map[string]int
	Attempts           map[string]runtimeeffects.Attempt
	States             map[string]runtimeeffects.State
	Completions        map[string]runtimeeffects.CompletionSettlement
}

func New() *Harness {
	return &Harness{
		Token:      runtimeeffects.LifecycleToken{RuntimeEpoch: 17, AgentID: "effect-test-agent", Generation: 4},
		Heartbeats: map[string]int{}, Attempts: map[string]runtimeeffects.Attempt{}, States: map[string]runtimeeffects.State{}, Completions: map[string]runtimeeffects.CompletionSettlement{},
	}
}

func (h *Harness) Context(identity string) context.Context {
	ctx := runtimeeffects.WithLifecycleToken(context.Background(), h.Token)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewCompletionController(h, h))
	return runtimeeffects.WithLogicalOperationIdentity(ctx, identity)
}

func (h *Harness) CompletionContext(identity string) context.Context {
	ctx := h.Context(identity)
	return runtimeeffects.WithUsageTarget(ctx, runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: "11111111-1111-4111-8111-111111111111",
		AgentID: h.Token.AgentID, SessionID: "22222222-2222-4222-8222-222222222222", RuntimeMode: "task",
	})
}

func (h *Harness) IsExternalEffectAuthorityCurrent(_ context.Context, authority runtimeeffects.Authority) (bool, error) {
	return authority.Normal == h.Token, nil
}

func (h *Harness) AuthorizeExternalAttempt(_ context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.AuthorizeErr != nil {
		return runtimeeffects.Attempt{}, h.AuthorizeErr
	}
	if authority.Normal != h.Token {
		return runtimeeffects.Attempt{}, fmt.Errorf("stale lifecycle token")
	}
	if _, exists := h.Attempts[req.AttemptID]; exists {
		return runtimeeffects.Attempt{}, fmt.Errorf("logical effect replay refused")
	}
	attempt := runtimeeffects.Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Token: authority.Normal, Authority: authority,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now,
	}
	h.Attempts[attempt.AttemptID] = attempt
	h.States[attempt.AttemptID] = runtimeeffects.StateAuthorized
	return attempt, nil
}

func (h *Harness) MarkExternalAttemptResponseObserved(_ context.Context, attempt runtimeeffects.Attempt, _ map[string]any, _ time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.States[attempt.AttemptID] != runtimeeffects.StateLaunched {
		return fmt.Errorf("attempt %s is not launched", attempt.AttemptID)
	}
	h.States[attempt.AttemptID] = runtimeeffects.StateResponseObserved
	return nil
}

func (h *Harness) MarkExternalAttemptLaunched(_ context.Context, attempt runtimeeffects.Attempt, _ time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.MarkErr != nil {
		return h.MarkErr
	}
	if h.States[attempt.AttemptID] != runtimeeffects.StateAuthorized {
		return fmt.Errorf("attempt %s is not authorized", attempt.AttemptID)
	}
	h.States[attempt.AttemptID] = runtimeeffects.StateLaunched
	return nil
}

func (h *Harness) HeartbeatCompletionAttempt(_ context.Context, attempt runtimeeffects.Attempt, _ time.Time, lease time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.HeartbeatErr != nil && (h.HeartbeatFailAfter <= 0 || h.Heartbeats[attempt.AttemptID] >= h.HeartbeatFailAfter) {
		return h.HeartbeatErr
	}
	if lease <= 0 {
		return fmt.Errorf("completion heartbeat lease must be positive")
	}
	state, ok := h.States[attempt.AttemptID]
	if !ok || (state != runtimeeffects.StateAuthorized && state != runtimeeffects.StateLaunched && state != runtimeeffects.StateResponseObserved) {
		return fmt.Errorf("attempt %s is not live", attempt.AttemptID)
	}
	h.Heartbeats[attempt.AttemptID]++
	return nil
}

func (h *Harness) SettleExternalAttempt(_ context.Context, settlement runtimeeffects.Settlement) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.SettleErr != nil {
		return h.SettleErr
	}
	if _, ok := h.States[settlement.AttemptID]; !ok {
		return fmt.Errorf("attempt %s is absent", settlement.AttemptID)
	}
	h.States[settlement.AttemptID] = settlement.State
	return nil
}

func (h *Harness) SettleCompletion(_ context.Context, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) (runtimeeffects.CompletionSettlementResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.SettleErr != nil {
		return runtimeeffects.CompletionSettlementResult{}, h.SettleErr
	}
	if _, ok := h.States[attempt.AttemptID]; !ok {
		return runtimeeffects.CompletionSettlementResult{}, fmt.Errorf("attempt %s is absent", attempt.AttemptID)
	}
	h.Completions[attempt.AttemptID] = settlement
	h.States[attempt.AttemptID] = settlement.Settlement.State
	return runtimeeffects.CompletionSettlementResult{
		Committed: true, SpendRecorded: true, AttemptID: attempt.AttemptID, EntityID: settlement.Spend.EntityID,
	}, nil
}

func (h *Harness) ProjectCommittedCompletionSpend(context.Context, runtimeeffects.CompletionSpendProjection) {
}

func (h *Harness) StateForAdapter(adapter string) (runtimeeffects.State, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for attemptID, attempt := range h.Attempts {
		if attempt.Adapter == adapter {
			return h.States[attemptID], true
		}
	}
	return "", false
}

func (h *Harness) RequireState(adapter string, want runtimeeffects.State) error {
	got, ok := h.StateForAdapter(adapter)
	if !ok {
		return fmt.Errorf("adapter %s has no attempt", adapter)
	}
	if got != want {
		return fmt.Errorf("adapter %s state = %s, want %s", adapter, got, want)
	}
	return nil
}

func (h *Harness) CompletionSettlementsForAdapter(adapter string) []runtimeeffects.CompletionSettlement {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]runtimeeffects.CompletionSettlement, 0)
	for attemptID, attempt := range h.Attempts {
		if attempt.Adapter == adapter {
			if settlement, ok := h.Completions[attemptID]; ok {
				out = append(out, settlement)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Now.Before(out[j].Now) })
	return out
}

func (h *Harness) HeartbeatsForAdapter(adapter string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	for attemptID, attempt := range h.Attempts {
		if attempt.Adapter == adapter {
			return h.Heartbeats[attemptID]
		}
	}
	return 0
}
