//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
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
			structural := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}
			if err := channelonboarding.RequireSessionStandingAdmission(f.operation, structural); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("durable account record granted fresh native target admission", err)
			}
			native, current, err := structural.AdmitExecution(f.ctx, f.authority)
			if err != nil || !current {
				native.CloseExecution()
				t.Fatal("original SDK did not admit its confirmed native account", current, err)
			}
			if err := channelonboarding.RequireSessionStandingAdmission(f.operation, native); err != nil {
				native.CloseExecution()
				t.Fatal("fresh target guard lost genuine held native authority", err)
			}
			foreign := f.operation
			foreign.Revision++
			if err := channelonboarding.RequireSessionStandingAdmission(foreign, native); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				native.CloseExecution()
				t.Fatal("held native authority adopted another parent revision", err)
			}
			f.occurrence.fence()
			if err := channelonboarding.RequireSessionStandingAdmission(f.operation, native); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				native.CloseExecution()
				t.Fatal("fenced occurrence retained fresh promotion authority through an unreleased handle", err)
			}
			if current, err := f.selected.SessionStandingBindingCurrent(context.Background(), f.operation); err != nil || !current {
				native.CloseExecution()
				t.Fatal("execution fencing erased confirmed retention", current, err)
			}
			native.CloseExecution()
			if err := channelonboarding.RequireSessionStandingAdmission(f.operation, native); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("released SDK admission still promoted a native target", err)
			}
			canceled, stop := context.WithCancel(f.ctx)
			stop()
			if current, err := f.selected.SessionStandingBindingCurrent(canceled, f.operation); current || !errors.Is(err, context.Canceled) {
				t.Fatal("canceled selected observation granted standing responsibility", current, err)
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
