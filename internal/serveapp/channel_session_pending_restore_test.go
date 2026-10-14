//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

func startPendingSessionFixture(t *testing.T, backend, key string) (*channelonboarding.Service, *serveSessionBootstrap, channelonboarding.Operation, func()) {
	t.Helper()
	service, sessions := installedSessionBootstrapFixture(t, backend)
	adapter := sessions.(*serveSessionBootstrap)
	cut, _, release := installSessionConstructionCut(t, adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := service.Start(ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
		Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: key})
	requireSessionPendingResult(t, result, result.Operation, err)
	select {
	case <-cut.entered:
	case <-ctx.Done():
		t.Fatal("pending original construction never entered its cut", context.Cause(ctx))
	}
	return service, adapter, result.Operation, release
}

func TestServeSessionStartupAwaitsOriginalPendingPreparationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, canceled := range []bool{false, true} {
			name := "complete"
			if canceled {
				name = "cancel"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				requireStartupPendingSession(t, backend, canceled)
			})
		}
	}
}

func requireStartupPendingSession(t *testing.T, backend string, canceled bool) {
	t.Helper()
	service, adapter, op, release := startPendingSessionFixture(t, backend, "startup-pending")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.RestoreSessions(ctx) }()
	select {
	case err := <-done:
		release()
		t.Fatal("startup bypassed the owned preparation cut", err)
	case <-time.After(50 * time.Millisecond):
	}
	if canceled {
		cancel()
	} else {
		release()
	}
	select {
	case err := <-done:
		if canceled && !errors.Is(err, context.Canceled) || !canceled && err != nil {
			t.Fatal("startup lost its completed/canceled preparation outcome", err)
		}
	case <-time.After(30 * time.Second):
		release()
		t.Fatal("startup did not join its original preparation wait")
	}
	release()
	join, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	adapter.mu.Lock()
	attempt := adapter.connections[op.OperationID]
	adapter.mu.Unlock()
	select {
	case <-attempt.done:
	case <-join.Done():
		t.Fatal("startup lost the original private construction join", context.Cause(join))
	}
	if attempt.err != nil || attempt.connection == nil {
		t.Fatal("private construction did not finish under its retained owner", attempt.err)
	}
	attempt.mu.Lock()
	connected := attempt.connected
	attempt.mu.Unlock()
	retained, err := adapter.store.GetChannelOnboarding(join, op.OperationID)
	if err != nil || !reflect.DeepEqual(retained, op) || connected == canceled {
		t.Fatal("startup fabricated account/business evidence or automatic dial after cancellation", retained, err, connected)
	}
	if err := attempt.connection.Close(join); err != nil {
		t.Fatal("original startup connection did not close and join", err)
	}
}

func TestServeSessionContextRetirementJoinsPendingPreparationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, adapter, op, release := startPendingSessionFixture(t, backend, "context-retirement-pending")
			ctx := context.Background()
			store := adapter.store.(channelonboarding.TeardownStore)
			now := time.Now().UTC()
			teardown, err := store.ReserveChannelTeardown(ctx, channelonboarding.ReserveTeardownRequest{
				TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
				Kind: channelonboarding.TeardownContextRetirement, PrincipalID: op.PrincipalID,
				Scope: channelonboarding.TeardownScope{BundleHash: op.Coordinate.BundleHash, ContextPublicationGeneration: op.Coordinate.ContextPublicationGeneration}, RequestedAt: now})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.RetireChannelTeardownAuthority(ctx, channelonboarding.RetireTeardownAuthorityRequest{
				TeardownID: teardown.TeardownID, ExpectedRevision: teardown.Revision, Reason: "pending_context_retirement", Now: now}); err != nil {
				t.Fatal(err)
			}
			wait, stop := context.WithTimeout(ctx, 50*time.Millisecond)
			if err := adapter.RetireInactiveSessions(wait); !errors.Is(err, context.DeadlineExceeded) {
				stop()
				t.Fatal("context retirement lost the pending original construction", err)
			}
			stop()
			release()
			join, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := adapter.RetireInactiveSessions(join); err != nil {
				t.Fatal("retired original construction did not join", err)
			}
			adapter.mu.Lock()
			attempt := adapter.connections[op.OperationID]
			adapter.mu.Unlock()
			if attempt == nil || attempt.err == nil || attempt.connection != nil {
				t.Fatal("retired preparation opened a provider or lost refusal evidence", attempt)
			}
		})
	}
}
