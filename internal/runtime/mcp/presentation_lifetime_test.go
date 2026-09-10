package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channelactivation"
)

func TestTurnPresentationRevocationJoinsRequests(t *testing.T) {
	for _, mode := range []string{"unregister", "reset", "expiry", "parent_cancel", "parent_complete"} {
		t.Run(mode, func(t *testing.T) {
			publication, err := channelonboarding.NewChannelActivationPublication(nil)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := channelactivation.NewOwner(publication)
			if err != nil {
				t.Fatal(err)
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx, root, err := owner.AcquirePresentationForContext(parent, channelactivation.PresentationScope{Actor: "actor"})
			if err != nil {
				t.Fatal(err)
			}
			registry := NewTurnContextRegistry(nil)
			registry.PutTurnContextForTest("turn", TurnContext{Presentation: channelactivation.BindPresentation(ctx, time.Now().Add(time.Hour)), ExpiresAt: time.Now().Add(time.Hour)})
			resolved, ok := registry.ResolveTurnContext("turn")
			if !ok {
				t.Fatal("token missing")
			}
			request, release, err := resolved.Presentation.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan struct{})
			go func() {
				switch mode {
				case "unregister":
					registry.UnregisterTurnContext("turn")
				case "reset":
					registry.Reset()
				case "expiry":
					registry.PruneTurnContextsBefore(time.Now().Add(2 * time.Hour))
				case "parent_cancel":
					cancel()
				case "parent_complete":
					root.Release()
				}
				close(finished)
			}()
			select {
			case <-request.Done():
			case <-time.After(time.Second):
				t.Fatal("request was not cancelled")
			}
			if _, lateRelease, err := resolved.Presentation.Acquire(context.Background()); err == nil {
				lateRelease()
				t.Fatal("previously resolved token admitted after revocation")
			}
			if mode == "unregister" || mode == "reset" || mode == "parent_complete" {
				select {
				case <-finished:
					t.Fatal("revocation returned before request joined")
				default:
				}
			}
			// Registry access must not wait for an admitted request's drain.
			metadataDone := make(chan struct{})
			go func() { registry.ResolveTurnContext("other"); close(metadataDone) }()
			select {
			case <-metadataDone:
			case <-time.After(time.Second):
				t.Fatal("registry mutex held during drain")
			}
			release()
			release()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("revocation failed to join")
			}
			root.Release()
			registry.Reset()
			replaceCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := owner.ReplaceContext(replaceCtx, publication); err != nil {
				t.Fatalf("transport leaked a lease: %v", err)
			}
		})
	}
}
