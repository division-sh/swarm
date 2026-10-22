//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
)

// Only the SDK's persisted paired device and authenticated transport are fixture
// inputs. Onboarding and native account admission use the actual selected owners.
func pairedBootstrapStateFixture(t *testing.T, f *runtimeBootstrapFixture, op channelonboarding.Operation, expected ...types.JID) string {
	t.Helper()
	if err := f.connection.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := openSessionState(f.ctx, f.directory, op.SessionConnectionID, "")
	if err != nil {
		t.Fatal(err)
	}
	device := newSDKDeviceFixture(t, state.database, expected...)
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
		t.Run(backend+"/deployment_invocation", func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			ctx := f.peer.ctx // No runtime occurrence or source fact is carried.
			process := worklifetime.NewProcess()
			operations := f.store.(interface {
				DeploymentChannelOperations() channelonboarding.OperationStore
			}).DeploymentChannelOperations()
			op, err := operations.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{
				DeploymentRegistration: true, OperationID: uuid.NewString(), PrincipalID: f.principal.ID,
				RequestKeyHash: "deployment-checkpoint-key", RequestHash: "deployment-checkpoint-request",
				Verb: channelonboarding.VerbConnect, Provider: "whatsapp", Interface: f.candidate.Interface,
				Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAccountLink,
				RequestedAt: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider} {
				op, err = operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
					ExpectedRevision: op.Revision, Phase: phase, Now: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
			}
			opts := DeploymentConnectionOptions{Directory: f.directory, OperationID: op.OperationID, Principal: f.principal,
				Process: process, Operations: operations, Store: f.store, Plan: f.candidate.Plan}
			f.connection, err = OpenDeploymentConnection(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := f.connection.Close(context.Background()); err != nil {
					t.Error(err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if _, err := OpenDeploymentConnection(ctx, opts); err == nil {
				t.Fatal("a second invocation obtained the same private device")
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.connection.PairingReadback(ctx, f.principal); err != nil {
				t.Fatal("deployment QR readback demanded runtime authority", err)
			}
			if _, paired, err := f.connection.CheckpointPairing(ctx, op.Revision); err != nil || paired {
				t.Fatal("unpaired invocation created an account", paired, err)
			}
			if _, err := f.connection.ChannelExecution(ctx); err == nil {
				t.Fatal("deployment pairing granted runtime execution")
			}
			account := pairedBootstrapStateFixture(t, f, op, types.NewJID("15550100123", types.DefaultUserServer))
			ctx = f.peer.ctx
			f.connection, err = OpenDeploymentConnection(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			f.peer.attach(t, f.connection.state.currentOccurrence().client)
			if err := f.connection.Connect(ctx); err != nil || !f.connection.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
				t.Fatal("retained deployment device did not authenticate", err)
			}
			checkpoint, paired, err := f.connection.CheckpointPairing(ctx, op.Revision)
			if err != nil || !paired || checkpoint.AccountReference != "+"+strings.TrimSuffix(account, "@s.whatsapp.net") ||
				checkpoint.Phase != channelonboarding.PhaseAwaitingExternalIdentity || checkpoint.ActivationRevision != 0 ||
				checkpoint.AccountConfirmed || checkpoint.Coordinate != (channelonboarding.ChannelRuntimeContextCoordinate{}) {
				t.Fatal("deployment checkpoint lost account identity or manufactured activation", paired, err)
			}
			candidate := f.candidate
			candidate.DeploymentRegistration, candidate.Ceremony = true, channelonboarding.CeremonyAccountLink
			candidate.Coordinate, candidate.Target = channelonboarding.ChannelRuntimeContextCoordinate{}, channelonboarding.CandidateTarget{}
			candidate.ConfirmationOperation = ""
			catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
			if err != nil {
				t.Fatal(err)
			}
			service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Deployment: true, Store: f.store,
				Operations: operations, Identities: f.channels, Credentials: f.writer, Sessions: f,
				Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil }})
			if err != nil {
				t.Fatal(err)
			}
			preview, err := service.Get(ctx, checkpoint.OperationID)
			if err != nil || preview.Operation.AccountConfirmed || preview.Operation.Revision != checkpoint.Revision {
				t.Fatal("readback approved the retained preview", err)
			}
			registered, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: checkpoint.OperationID})
			if err != nil || registered.Operation.Phase != channelonboarding.PhaseSucceeded || !registered.Operation.AccountConfirmed ||
				registered.Readiness != nil || registered.Binding != nil || registered.Operation.ActivationRevision != 0 {
				t.Fatal("registration required or fabricated runtime activation", err)
			}
			replay, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: checkpoint.OperationID})
			if err != nil || replay.Operation.Revision != registered.Operation.Revision {
				t.Fatal("registered replay restarted the ceremony", err)
			}
		})
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
