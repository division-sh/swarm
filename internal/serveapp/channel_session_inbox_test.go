//go:build linux || darwin

package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

func TestServeSessionInboxTransportConsumesOwnedHealthBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			activation := channelonboarding.ConnectedChannelActivation{OperationID: op.OperationID, PrincipalID: op.PrincipalID,
				Posture: op.Posture, Provider: op.Provider, Interface: op.Interface, Revision: op.ActivationRevision,
				BindingRevision: op.BindingRevision, SessionAccount: op.SessionAccount, Coordinate: op.Coordinate}
			// This exercises transport qualification with real SDK ownership.
			// It does not manufacture a principal-authorized Inbox request/activation.
			dispatcher := &serveChannelDeliveryDispatcher{activations: f.adapter.store, sessions: f.adapter}
			entry := channeldelivery.ResolvedInboxEntry{Kind: channeldelivery.InboxEntryTextReply}
			provider, disposition, err := dispatcher.admitInboxEntryTransport(f.ctx, activation, entry)
			if err != nil || disposition != channeldelivery.InboxEntryAccepted || provider.RequireExecutable() != nil {
				provider.CloseExecution()
				t.Fatal("native Inbox fabricated a webhook requirement or lost its SDK owner", disposition, err)
			}
			provider.CloseExecution()
			entry.Kind = channeldelivery.InboxEntryNative
			provider, disposition, err = dispatcher.admitInboxEntryTransport(f.ctx, activation, entry)
			provider.CloseExecution()
			if err != nil || disposition != channeldelivery.InboxEntryRejected {
				t.Fatal("menu-less channel accepted a fabricated native menu entry", disposition, err)
			}
			entry.Kind = channeldelivery.InboxEntryTextReply
			changed := activation
			changed.SessionAccount.AdmissionID = uuid.NewString()
			provider, disposition, err = dispatcher.admitInboxEntryTransport(f.ctx, changed, entry)
			provider.CloseExecution()
			if err == nil || disposition != channeldelivery.InboxEntryUnavailable {
				t.Fatal("Inbox adopted contradictory activation authority", disposition, err)
			}
			dispatcher.sessions = nil
			provider, disposition, err = dispatcher.admitInboxEntryTransport(f.ctx, activation, entry)
			provider.CloseExecution()
			if err != nil || disposition != channeldelivery.InboxEntryUnavailable {
				t.Fatal("uninstalled native owner qualified Inbox", disposition, err)
			}
			dispatcher.sessions = f.adapter
			if err := f.adapter.connection(op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			provider, disposition, err = dispatcher.admitInboxEntryTransport(f.ctx, activation, entry)
			provider.CloseExecution()
			if err != nil || disposition != channeldelivery.InboxEntryUnavailable {
				t.Fatal("retired native ownership still authorized Inbox entry", disposition, err)
			}
		})
	}
}
