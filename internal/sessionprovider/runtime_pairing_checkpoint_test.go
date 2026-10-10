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

// Only the SDK's persisted paired device and authenticated transport are fixture
// inputs. Onboarding and native account admission use the actual selected owners.
func pairedBootstrapStateFixture(t *testing.T, f *runtimeBootstrapFixture, op channelonboarding.Operation) string {
	t.Helper()
	if err := f.connection.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := openSessionState(f.ctx, f.directory, op.SessionConnectionID, "")
	if err != nil {
		t.Fatal(err)
	}
	device := newSDKDeviceFixture(t, state.database)
	account := device.ID.ToNonAD().String()
	if err := state.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.connection = nil
	f.peer = newSDKPeer(t)
	return account
}

func TestWhatsAppSessionIdentityUsesNativeAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "paired-identity"})
			if err != nil {
				t.Fatal(err)
			}
			accountRef := pairedBootstrapStateFixture(t, f, started.Operation)
			account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: started.Operation.SessionConnectionID,
				AccountRef: accountRef, AdmissionID: uuid.NewString(), Revision: 1}
			op, err := f.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: started.Operation.OperationID,
				ExpectedRevision: started.Operation.Revision, Phase: channelonboarding.PhaseAwaitingExternalIdentity,
				SessionAccount: &account, Now: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			f.connection, err = OpenRuntimeConnection(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
				OperationID: op.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(f.ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal(err)
			}
			resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: op.OperationID})
			if err != nil || resumed.IdentityOperation == nil || resumed.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
				t.Fatalf("paired native admission did not reach the existing claim ceremony: %+v, %v", resumed, err)
			}
			if !resumed.IdentityOperation.ProviderAuthority.SameProvenance(operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}) ||
				resumed.Binding != nil || resumed.Operation.Coordinate.TargetGeneration != 0 || resumed.Operation.ActivationRevision != 0 {
				t.Fatal("pairing invented a credential, operator binding or executable activation")
			}
			repeated, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: op.OperationID})
			if err != nil || repeated.IdentityOperation == nil || repeated.IdentityOperation.OperationID != resumed.IdentityOperation.OperationID ||
				repeated.Operation.SessionAccount != account {
				t.Fatal("same paired responsibility reminted account or identity", repeated, err)
			}
		})
	}
}

func TestWhatsAppReservedPairingCheckpointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
				Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "checkpoint-pairing"})
			if err != nil {
				t.Fatal(err)
			}
			if op, paired, err := f.connection.CheckpointPairing(f.ctx, started.Operation.Revision); err != nil || paired || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
				t.Fatal("unpaired SDK observation created account provenance", op, paired, err)
			}
			accountRef := pairedBootstrapStateFixture(t, f, started.Operation)
			f.connection, err = OpenRuntimeBootstrap(f.ctx, RuntimeConnectionOptions{Directory: f.directory,
				OperationID: started.Operation.OperationID, Store: f.store, Plan: f.candidate.Plan})
			if err != nil {
				t.Fatal(err)
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(f.ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal("retained SDK device did not authenticate", err)
			}
			if err := f.connection.CheckBootstrap(f.ctx); err != nil {
				t.Fatal("healthy original bootstrap was not reusable", err)
			}
			resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: started.Operation.OperationID})
			if err != nil || resumed.IdentityOperation == nil || resumed.IdentityOperation.State != operatorchannel.StateAwaitingClaim ||
				resumed.Operation.SessionAccount.AccountRef != accountRef || resumed.Operation.SessionAccount.ConnectionID != started.Operation.SessionConnectionID ||
				resumed.Operation.ActivationRevision != 0 || resumed.Operation.Coordinate.TargetGeneration != 0 || resumed.Binding != nil {
				t.Fatal("reserved pairing did not freeze the SDK account before the human claim", resumed, err)
			}
			account := resumed.Operation.SessionAccount
			for index := 0; index < 3; index++ {
				op, paired, err := f.connection.CheckpointPairing(f.ctx, resumed.Operation.Revision)
				if err != nil || !paired || op.SessionAccount != account || op.Revision != resumed.Operation.Revision {
					t.Fatal("checkpoint replay reminted account provenance", op, paired, err)
				}
			}
			if _, _, err := f.connection.CheckpointPairing(f.ctx, started.Operation.Revision); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("stale pairing revision advanced", err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, _, err := f.connection.CheckpointPairing(canceled, resumed.Operation.Revision); err == nil {
				t.Fatal("canceled caller checkpointed pairing")
			}
			if err := f.connection.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.connection.CheckBootstrap(f.ctx); err == nil {
				t.Fatal("completed close reported bootstrap success")
			}
			if _, err := f.connection.AdmitSessionAccount(f.ctx, account); err == nil {
				t.Fatal("closed bootstrap supplied native authority")
			}
		})
	}
}
