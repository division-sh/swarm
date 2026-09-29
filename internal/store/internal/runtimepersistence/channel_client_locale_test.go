package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

func TestChannelClientLocaleOwnsIndependentSelectedStoreRevision(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			begun := rig.start(t, channelonboarding.VerbConnect, "locale-token", false)
			rig.confirm(t, begun, "account-a")
			rig.complete(t, begun.Operation.OperationID)
			original := rig.parent(t, begun.Operation.OperationID)
			ctx := context.Background()
			request := channelonboarding.SetClientLocaleRequest{OperationID: original.OperationID,
				PrincipalID: original.PrincipalID, ExpectedRevision: original.ClientLocaleRevision, Language: "fr", Now: rig.now}
			changed, err := rig.fixture.store.SetChannelClientLocale(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if original.ClientLanguage != "" || changed.ClientLanguage != "fr" || changed.ClientLocaleRevision != original.ClientLocaleRevision+1 ||
				changed.Revision != original.Revision || changed.IdentityOperationID != original.IdentityOperationID ||
				changed.BindingRevision != original.BindingRevision || changed.ActivationRevision != original.ActivationRevision ||
				changed.ConfirmationOperationID != original.ConfirmationOperationID || !changed.Coordinate.Matches(original.Coordinate) {
				t.Fatalf("locale update changed connection/effect ownership: before=%#v after=%#v", original, changed)
			}
			replayed, err := rig.fixture.store.SetChannelClientLocale(ctx, request)
			if err != nil || replayed.ClientLocaleRevision != changed.ClientLocaleRevision {
				t.Fatalf("locale replay=%#v err=%v", replayed, err)
			}
			request.Language = "en"
			if _, err := rig.fixture.store.SetChannelClientLocale(ctx, request); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatalf("stale locale mutation=%v", err)
			}
			request.ExpectedRevision = changed.ClientLocaleRevision
			request.PrincipalID = uuid.NewString()
			if _, err := rig.fixture.store.SetChannelClientLocale(ctx, request); !errors.Is(err, channelonboarding.ErrNotFound) {
				t.Fatalf("foreign declaration=%v", err)
			}
			request.PrincipalID = original.PrincipalID
			request.ExpectedRevision = 0
			if _, err := rig.fixture.store.SetChannelClientLocale(ctx, request); !errors.Is(err, channelonboarding.ErrInvalidRequest) {
				t.Fatalf("unfenced declaration=%v", err)
			}
		})
	}
}
