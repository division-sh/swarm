//go:build linux || darwin

package sessionprovider

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

func TestWhatsAppStandingResponsibilityRequiresRealConfirmationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixtureWithTarget(t, backend, false, false, 0)
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || current {
				t.Fatal("pairing alone enabled standing retention", current, err)
			}
			identity := f.confirm(t)
			var err error
			f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision,
				Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, IdentityOperationID: identity.OperationID, Now: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || current {
				t.Fatal("uncheckpointed confirmation enabled retention", current, err)
			}
			f.operation, err = f.selected.ReconcileChannelOnboardingBinding(f.ctx, channelonboarding.ReconcileBindingRequest{
				OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision,
				ExpectedBindingRevision: f.binding.Revision, Now: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || !current || f.operation.Coordinate.TargetGeneration != 0 {
				t.Fatal("confirmed runless responsibility was not admitted", current, err)
			}
			for _, row := range []struct {
				name string
				edit func(*channelonboarding.Operation)
			}{
				{"revision", func(op *channelonboarding.Operation) { op.Revision++ }},
				{"principal", func(op *channelonboarding.Operation) { op.PrincipalID = uuid.NewString() }},
				{"identity_child", func(op *channelonboarding.Operation) { op.IdentityOperationID = uuid.NewString() }},
				{"binding", func(op *channelonboarding.Operation) { op.BindingRevision++ }},
				{"account", func(op *channelonboarding.Operation) { op.SessionAccount.AccountRef = "19999999999@s.whatsapp.net" }},
				{"account_admission", func(op *channelonboarding.Operation) { op.SessionAccount.AdmissionID = uuid.NewString() }},
				{"connection", func(op *channelonboarding.Operation) { op.SessionConnectionID = uuid.NewString() }},
				{"source", func(op *channelonboarding.Operation) { op.Coordinate.BundleHash += "foreign" }},
				{"inventory", func(op *channelonboarding.Operation) { op.Coordinate.PackInventoryGeneration += "foreign" }},
				{"selector", func(op *channelonboarding.Operation) { op.TargetSelector += "foreign" }},
				{"provider", func(op *channelonboarding.Operation) { op.Provider = "telegram" }},
				{"interface", func(op *channelonboarding.Operation) { op.Interface.ChannelManifestHash += "foreign" }},
			} {
				t.Run(row.name, func(t *testing.T) {
					foreign := f.operation
					row.edit(&foreign)
					if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, foreign); err != nil || current {
						t.Fatal("foreign responsibility enabled retention", current, err)
					}
				})
			}
			// No socket-health requirement: an enabled selected responsibility
			// survives disconnection without granting a new native admission.
			f.occurrence.fence()
			if err := f.occurrence.join(f.ctx); err != nil {
				t.Fatal(err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(context.Background(), f.operation); err != nil || !current {
				t.Fatal("disconnection erased confirmed standing responsibility", current, err)
			}
			if _, _, err := f.identities.Unbind(context.Background(), f.operation.Interface.Selector,
				f.binding.Revision, uuid.NewString(), uuid.NewString(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(context.Background(), f.operation); err != nil || current {
				t.Fatal("unbind retained native standing authority", current, err)
			}
		})
	}
}

func TestWhatsAppStandingResponsibilityRequiresCurrentActivationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || !current {
				t.Fatal("current activation lost confirmed responsibility", current, err)
			}
			if _, err := f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
				SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "standing_test", Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || current {
				t.Fatal("historical activation enabled standing responsibility", current, err)
			}
		})
	}
}
