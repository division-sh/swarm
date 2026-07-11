package effecttest

import (
	"context"
	"fmt"
	"sync"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type Harness struct {
	mu           sync.Mutex
	Token        runtimeeffects.LifecycleToken
	AuthorizeErr error
	MarkErr      error
	SettleErr    error
	Attempts     map[string]runtimeeffects.Attempt
	States       map[string]runtimeeffects.State
}

func New() *Harness {
	return &Harness{
		Token:    runtimeeffects.LifecycleToken{RuntimeEpoch: 17, AgentID: "effect-test-agent", Generation: 4},
		Attempts: map[string]runtimeeffects.Attempt{}, States: map[string]runtimeeffects.State{},
	}
}

func (h *Harness) Context(identity string) context.Context {
	ctx := runtimeeffects.WithLifecycleToken(context.Background(), h.Token)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(h))
	return runtimeeffects.WithLogicalOperationIdentity(ctx, identity)
}

func (h *Harness) IsLifecycleTokenCurrent(_ context.Context, token runtimeeffects.LifecycleToken) (bool, error) {
	return token == h.Token, nil
}

func (h *Harness) AuthorizeExternalAttempt(_ context.Context, token runtimeeffects.LifecycleToken, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.AuthorizeErr != nil {
		return runtimeeffects.Attempt{}, h.AuthorizeErr
	}
	if token != h.Token {
		return runtimeeffects.Attempt{}, fmt.Errorf("stale lifecycle token")
	}
	if _, exists := h.Attempts[req.AttemptID]; exists {
		return runtimeeffects.Attempt{}, fmt.Errorf("logical effect replay refused")
	}
	attempt := runtimeeffects.Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Token: token,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now,
	}
	h.Attempts[attempt.AttemptID] = attempt
	h.States[attempt.AttemptID] = runtimeeffects.StateAuthorized
	return attempt, nil
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
