//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type pairingCommitCancelStore struct {
	channelonboarding.Store
	cancel context.CancelFunc
	before bool
}

func (s pairingCommitCancelStore) AdvanceChannelOnboarding(ctx context.Context, req channelonboarding.AdvanceRequest) (channelonboarding.Operation, error) {
	if s.before {
		s.cancel()
	}
	op, err := s.Store.AdvanceChannelOnboarding(ctx, req)
	if err == nil && !s.before {
		s.cancel()
	}
	return op, err
}

func TestWhatsAppPairingBeforeCommitCancellationPreservesReservationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "pairing-before-commit"})
			if err != nil {
				t.Fatal(err)
			}
			pairedBootstrapStateFixture(t, f, started.Operation)
			f.connection, err = OpenRuntimeBootstrap(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
				OperationID: started.Operation.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(f.ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal(err)
			}
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			f.connection.store = pairingCommitCancelStore{Store: f.store, cancel: cancel, before: true}
			if _, paired, err := f.connection.CheckpointPairing(caller, started.Operation.Revision); err == nil || paired {
				t.Fatal("pre-commit cancellation adopted the SDK account", paired, err)
			}
			f.connection.store = f.store
			retained, err := f.store.GetChannelOnboarding(f.ctx, started.Operation.OperationID)
			if err != nil || retained.Phase != started.Operation.Phase || retained.Revision != started.Operation.Revision ||
				retained.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || f.connection.sessionAccount() != retained.SessionAccount {
				t.Fatal("pre-commit cancellation changed reservation or account", retained, err)
			}
			resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: retained.OperationID})
			if err != nil || resumed.IdentityOperation == nil || resumed.Operation.SessionConnectionID != retained.SessionConnectionID {
				t.Fatal("fresh retry could not checkpoint the original pairing", resumed, err)
			}
		})
	}
}

func TestWhatsAppPairingCommitCancellationRemainsRecoverableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "reviewer-pairing-commit"})
			if err != nil {
				t.Fatal(err)
			}
			accountRef := pairedBootstrapStateFixture(t, f, started.Operation)
			f.connection, err = OpenRuntimeBootstrap(f.ctx, RuntimeConnectionOptions{Directory: f.directory, OperationID: started.Operation.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(f.ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal(err)
			}
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			f.connection.store = pairingCommitCancelStore{Store: f.store, cancel: cancel}
			_, _, err = f.connection.CheckpointPairing(caller, started.Operation.Revision)
			if !errors.Is(err, context.Canceled) {
				t.Fatal("canceled checkpoint did not report cancellation")
			}
			f.connection.store = f.store
			retained, err := f.store.GetChannelOnboarding(f.ctx, started.Operation.OperationID)
			if err != nil || retained.Phase != channelonboarding.PhaseAwaitingExternalIdentity || retained.SessionAccount.AccountRef != accountRef {
				t.Fatal("pairing did not commit before cancellation", retained, err)
			}
			if err := f.connection.CheckBootstrap(f.ctx); err != nil {
				t.Errorf("committed original pairing is stranded by stale account projection: %v", err)
			}
			resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: retained.OperationID})
			if err != nil || resumed.IdentityOperation == nil {
				t.Fatalf("fresh request cannot resume committed pairing into the human claim: %v", err)
			}
			account := retained.SessionAccount
			for n := 0; n < 3; n++ {
				repeated, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: retained.OperationID})
				if err != nil || repeated.IdentityOperation == nil || repeated.IdentityOperation.OperationID != resumed.IdentityOperation.OperationID ||
					repeated.Operation.SessionAccount != account || repeated.Binding != nil || repeated.Operation.ActivationRevision != 0 {
					t.Fatal("commit recovery reminted authority or bypassed human confirmation", repeated, err)
				}
			}
			if err := f.connection.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			closedView, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: retained.OperationID})
			if err != nil || closedView.IdentityOperation == nil || closedView.IdentityOperation.OperationID != resumed.IdentityOperation.OperationID ||
				closedView.Operation.Revision != resumed.Operation.Revision || closedView.Binding != nil {
				t.Fatal("closed pending-identity readback changed durable authority", closedView, err)
			}
			if _, err := f.connection.AdmitSessionAccount(f.ctx, account); err == nil {
				t.Fatal("closed connection issued native account authority")
			}
			if err := f.connection.CheckBootstrap(f.ctx); err == nil {
				t.Fatal("closed pending-identity readback became successful bootstrap")
			}
			f.connection, err = OpenRuntimeConnection(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
				OperationID: retained.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			f.peer = newSDKPeer(t)
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(f.ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal(err)
			}
			restarted, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: retained.OperationID})
			if err != nil || restarted.IdentityOperation == nil || restarted.IdentityOperation.OperationID != resumed.IdentityOperation.OperationID ||
				restarted.Operation.SessionAccount != account {
				t.Fatal("paired restart adopted another account or identity", restarted, err)
			}
			if _, err := f.owner.RetireAndWait(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.connection.AdmitSessionAccount(context.Background(), account); err == nil {
				t.Fatal("retired runtime retained native account authority")
			}
		})
	}
}
