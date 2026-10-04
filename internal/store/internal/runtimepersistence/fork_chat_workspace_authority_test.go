package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkChatWorkspaceAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newForkChatCompletionAuthorityFixture(t, backend == "sqlite")
			prepared := prepareForkChatCompletionGroup(t, fixture, "workspace", "admit target without provider launch")
			authority := forkChatCompletionAuthority(prepared, 1)
			owner, ok := fixture.store.(effects.ForkChatWorkspaceStore)
			if !ok {
				t.Fatal("selected store omits the existing fork authority's workspace projection")
			}
			if current, err := owner.IsForkChatWorkspaceAuthorityCurrent(testAuthorActivityContext(), authority); err != nil || !current {
				t.Fatalf("prepared workspace admission: current=%v err=%v", current, err)
			}
			if current, err := fixture.store.IsExternalEffectAuthorityCurrent(testAuthorActivityContext(), authority); err != nil || current {
				t.Fatalf("read-only workspace observation authorized execution: current=%v err=%v", current, err)
			}
			requireForkChatGroupRows(t, fixture, prepared.ForkTurnID, 0, 0)
			for _, test := range []struct {
				name   string
				change func(*effects.Authority)
			}{
				{"fork", func(a *effects.Authority) { a.ForkChat.ForkID = uuid.NewString() }},
				{"source", func(a *effects.Authority) { a.ForkChat.SourceRunID = uuid.NewString() }},
				{"bundle", func(a *effects.Authority) { a.ForkChat.BundleHash += "-foreign" }},
				{"actor", func(a *effects.Authority) { a.ForkChat.ActorTokenID = "other" }},
				{"occurrence", func(a *effects.Authority) { a.ForkChat.RequestOccurrenceID = uuid.NewString() }},
				{"request", func(a *effects.Authority) { a.ForkChat.RequestHash = "foreign" }},
				{"owner", func(a *effects.Authority) { a.ExecutionOwner = "other" }},
				{"fence", func(a *effects.Authority) { a.FenceGeneration++ }},
				{"turn", func(a *effects.Authority) { a.ID = uuid.NewString(); a.ForkChat.ForkTurnID = a.ID }},
				{"missing", func(a *effects.Authority) { *a = effects.Authority{} }},
			} {
				t.Run(test.name, func(t *testing.T) {
					foreign := authority
					test.change(&foreign)
					if current, err := owner.IsForkChatWorkspaceAuthorityCurrent(testAuthorActivityContext(), foreign); err != nil || current {
						t.Fatalf("foreign target admitted: current=%v err=%v", current, err)
					}
				})
			}
			ctx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "model")
			if current, err := owner.IsForkChatWorkspaceAuthorityCurrent(ctx, authority); err != nil || !current {
				t.Fatalf("executing workspace admission: current=%v err=%v", current, err)
			}
			settleForkChatPrelaunchFailure(t, ctx, handle, prepared, errors.New("pre-model refusal"), fixture.now.Add(time.Second))
			if err := fixture.store.FailOperatorConversationForkChat(testAuthorActivityContext(), runfork.ConversationForkChatFailureRequest{
				Prepared: prepared, Cause: errors.New("target refused"), Now: fixture.now.Add(2 * time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			if current, err := owner.IsForkChatWorkspaceAuthorityCurrent(testAuthorActivityContext(), authority); err != nil || current {
				t.Fatalf("terminal workspace admission: current=%v err=%v", current, err)
			}
		})
	}
}
