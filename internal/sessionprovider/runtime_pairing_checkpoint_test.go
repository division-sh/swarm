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
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var errRegistrationCompletion = errors.New("controlled logout checkpoint failure")

type registrationCompletionFailure struct {
	channelonboarding.OperationStore
	fail       bool
	failUnlink bool
}

func (s *registrationCompletionFailure) AdvanceChannelOnboarding(ctx context.Context, req channelonboarding.AdvanceRequest) (channelonboarding.Operation, error) {
	if req.CompleteDeploymentLogout && s.fail {
		s.fail = false
		return channelonboarding.Operation{}, errRegistrationCompletion
	}
	if req.FailureCode == "account_logout_unlinked" && s.failUnlink {
		s.failUnlink = false
		return channelonboarding.Operation{}, errRegistrationCompletion
	}
	return s.OperationStore.AdvanceChannelOnboarding(ctx, req)
}

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
				resumed.Binding != nil || resumed.Operation.ActivationRevision != 0 {
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
		for _, mode := range []string{"comms", "hitl", "reject_comms", "reject_hitl"} {
			hitl, rejectPreview := strings.HasSuffix(mode, "hitl"), strings.HasPrefix(mode, "reject_")
			label := backend + "/deployment_invocation"
			if mode != "comms" {
				label += "_" + mode
			}
			t.Run(label, func(t *testing.T) {
				f := newRuntimeBootstrapFixture(t, backend)
				ctx := startupownership.CarryMutationAuthority(f.peer.ctx, f.ctx) // No runtime occurrence or source fact is carried.
				process, found := worklifetime.ProcessFromContext(f.ctx)
				if !found {
					t.Fatal("deployment fixture lost its original process work owner")
				}
				ctx = worklifetime.WithProcess(ctx, process)
				operations := f.store.(interface {
					DeploymentChannelOperations() channelonboarding.OperationStore
				}).DeploymentChannelOperations()
				completion := &registrationCompletionFailure{OperationStore: operations}
				operations = completion
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
					Process: process, Operations: operations, Store: f.store, Plan: f.candidate.Plan, Trigger: f.trigger}
				request, requestDone := context.WithCancel(ctx)
				f.connection, err = OpenDeploymentConnection(request, opts)
				requestDone()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					defer cancel()
					if err := f.connection.Close(closeCtx); err != nil {
						t.Error(err)
					}
				})
				if _, err := OpenDeploymentConnection(ctx, opts); err == nil {
					t.Fatal("a second invocation obtained the same private device")
				}
				candidate := f.candidate
				candidate.DeploymentRegistration, candidate.Ceremony = true, channelonboarding.CeremonyAccountLink
				candidate.Coordinate, candidate.Target = channelonboarding.ChannelRuntimeContextCoordinate{}, channelonboarding.CandidateTarget{}
				candidate.ConfirmationOperation = ""
				catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
				if err != nil {
					t.Fatal(err)
				}
				var operator *channelonboarding.AccountReference
				if hitl {
					operator = &channelonboarding.AccountReference{Provider: "whatsapp", Account: "+15550100123"}
				}
				service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Deployment: true, OperatorAccount: operator, Store: f.store,
					Operations: operations, Identities: f.channels, Credentials: f.writer, Sessions: f,
					Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil }})
				if err != nil {
					t.Fatal(err)
				}
				f.peer.attach(t, f.connection.state.currentOccurrence().client)
				if err := f.connection.Connect(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := f.connection.PairingReadback(ctx, f.principal); err != nil {
					t.Fatal("deployment QR readback demanded runtime authority", err)
				}
				initial, err := service.Get(ctx, op.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				if pending, err := initial.SessionPairingPending(time.Now()); err != nil || !pending {
					t.Fatal("initial deployment QR progress was not the original non-executable projection", pending, err)
				}
				if _, paired, err := f.connection.CheckpointPairing(ctx, op.Revision); err != nil || paired {
					t.Fatal("unpaired invocation created an account", paired, err)
				}
				if _, err := f.connection.ChannelExecution(ctx); err == nil {
					t.Fatal("deployment pairing granted runtime execution")
				}
				account := pairedBootstrapStateFixture(t, f, op, types.NewJID("15550100123", types.DefaultUserServer))
				ctx = worklifetime.WithProcess(startupownership.CarryMutationAuthority(f.peer.ctx, ctx), process)
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
				preview, err := service.Get(ctx, checkpoint.OperationID)
				if err != nil || preview.Operation.AccountConfirmed || preview.Operation.Revision != checkpoint.Revision {
					t.Fatal("readback approved the retained preview", err)
				}
				if _, err := f.connection.AdmitSessionAccount(ctx, checkpoint.SessionAccount); err == nil {
					t.Fatal("unconfirmed account granted operator authority")
				}
				logout := func(op channelonboarding.Operation) channelonboarding.Result {
					t.Helper()
					reconcile := completion.fail || completion.failUnlink
					retainedCode := "account_logout_unlinked"
					if completion.failUnlink {
						retainedCode = "account_logout_launched"
					}
					type outcome struct {
						result channelonboarding.Result
						err    error
					}
					done := make(chan outcome, 1)
					go func() {
						result, err := service.LogoutDeployment(ctx, op.OperationID, op.Revision)
						done <- outcome{result, err}
					}()
					unlink := f.peer.next(t)
					if unlink.node.Tag != "iq" || unlink.node.Attrs["xmlns"] != "md" {
						t.Fatal("deployment logout did not use the original unlink transport", unlink.node)
					}
					f.peer.acknowledge(t, unlink)
					select {
					case result := <-done:
						if reconcile {
							if !errors.Is(result.err, errRegistrationCompletion) {
								t.Fatal("logout missed the exact controlled completion failure", result.err)
							}
							retained, readErr := operations.GetChannelOnboarding(ctx, op.OperationID)
							if readErr != nil || retained.FailureCode != retainedCode || retained.LogoutCompleted {
								t.Fatal("unlink was not retained for exact local reconciliation", retained, readErr)
							}
							accounts, readErr := service.DeploymentAccounts(ctx, "whatsapp")
							if readErr != nil || len(accounts) != 1 || accounts[0].Operation.OperationID != retained.OperationID {
								t.Fatal("incomplete logout disappeared from public account selection", readErr)
							}
							result.result, result.err = service.LogoutDeployment(ctx, retained.OperationID, retained.Revision)
						}
						if result.err != nil {
							t.Fatal("deployment logout failed", result.err)
						}
						return result.result
					case <-ctx.Done():
						t.Fatal("deployment logout did not join", context.Cause(ctx))
						return channelonboarding.Result{}
					}
				}
				if rejectPreview {
					loggedOut := logout(preview.Operation)
					if !loggedOut.Operation.LogoutCompleted || loggedOut.Operation.AccountConfirmed ||
						loggedOut.Operation.IdentityOperationID != "" || loggedOut.Operation.ActivationRevision != 0 {
						t.Fatal("rejected account gained approval or could not be unlinked")
					}
					return
				}
				registered, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: checkpoint.OperationID})
				if hitl {
					if err != nil || !registered.Operation.AccountConfirmed || registered.IdentityOperation == nil ||
						registered.IdentityOperation.State != operatorchannel.StateAwaitingClaim ||
						registered.IdentityOperation.ProviderAuthority.Scope != authority.DeploymentParent ||
						registered.Operation.ActivationRevision != 0 || registered.Readiness != nil || registered.Binding != nil {
						t.Fatal("deployment operator ceremony required runtime authority or admitted an unverified operator", err)
					}
					resumed, resumeErr := service.ReverifyDeployment(ctx, registered.Operation.OperationID)
					if resumeErr != nil || resumed.IdentityOperation == nil || resumed.IdentityOperation.OperationID != registered.IdentityOperation.OperationID ||
						resumed.Operation.SessionAccount != registered.Operation.SessionAccount || !resumed.Operation.AccountConfirmed {
						t.Fatal("pending operator enrollment could not resume its exact retained account", resumeErr)
					}
					expired, expireErr := f.channels.ExpireOperation(ctx, resumed.IdentityOperation.OperationID, resumed.IdentityOperation.Revision, resumed.IdentityOperation.ExpiresAt)
					if expireErr != nil || expired.State != operatorchannel.StateExpired {
						t.Fatal("genuine operator expiry did not settle", expireErr)
					}
					registered, err = service.ReverifyDeployment(ctx, resumed.Operation.OperationID)
					if err != nil || registered.IdentityOperation == nil || registered.IdentityOperation.OperationID == expired.OperationID ||
						registered.IdentityOperation.State != operatorchannel.StateAwaitingClaim || registered.Operation.SessionAccount != resumed.Operation.SessionAccount || !registered.Operation.AccountConfirmed {
						t.Fatal("terminal operator enrollment stranded or replaced its approved account", err)
					}
					current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, f.store,
						channelonboarding.AdmissionResponsibility{DeploymentRegistration: true, OperationID: registered.Operation.OperationID,
							OperationRevision: registered.Operation.Revision, Provider: "whatsapp", SessionAccount: registered.Operation.SessionAccount,
							Credentials: registered.Operation.CredentialAdmissions}, true)
					if err != nil || !current {
						t.Fatal("deployment claim demanded a runtime source", current, err)
					}
					device, err := f.connection.state.device(ctx)
					if err != nil {
						t.Fatal(err)
					}
					message := encryptedMessageFixture(t, device, types.NewJID("15550100124", types.DefaultUserServer),
						&waE2E.Message{Conversation: proto.String(registered.IdentityOperation.Challenge)})
					f.peer.mu.Lock()
					socket := f.peer.peers[0]
					f.peer.mu.Unlock()
					if err := socket.send(ctx, message); err != nil {
						t.Fatal(err)
					}
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					var claimed operatorchannel.Operation
					for {
						claimed, err = f.channels.GetOperation(ctx, registered.IdentityOperation.OperationID)
						if err != nil {
							t.Fatal(err)
						}
						if claimed.State == operatorchannel.StateAwaitingConfirmation {
							break
						}
						select {
						case <-tick.C:
						case <-ctx.Done():
							t.Fatal("native deployment claim did not settle", context.Cause(ctx))
						}
					}
					if claimed.ExternalAccountRef != "15550100124@s.whatsapp.net" {
						t.Fatal("deployment claim changed its authenticated sender")
					}
					confirmed, binding, err := service.ConfirmIdentity(ctx, claimed.OperationID, claimed.Revision, true, time.Now().UTC())
					if err != nil || confirmed.State != operatorchannel.StateBound || binding.ExternalAccountRef != claimed.ExternalAccountRef ||
						binding.ProviderAuthority.Scope != authority.DeploymentParent {
						t.Fatal("explicit deployment claimant confirmation lost its account scope", err)
					}
					finished, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: registered.Operation.OperationID})
					if err != nil || finished.Operation.Phase != channelonboarding.PhaseSucceeded || finished.Binding == nil ||
						finished.Operation.ActivationRevision != 0 || finished.Readiness != nil {
						t.Fatal("verified deployment ceremony required runtime activation", err)
					}
					completion.fail = true
					loggedOut := logout(finished.Operation)
					if !loggedOut.Operation.LogoutCompleted || loggedOut.Operation.Phase != channelonboarding.PhaseRetired {
						t.Fatal("deployment HITL logout required runtime authority or failed unlink")
					}
					return
				}
				if err != nil || registered.Operation.Phase != channelonboarding.PhaseSucceeded || !registered.Operation.AccountConfirmed ||
					registered.Readiness != nil || registered.Binding != nil || registered.Operation.ActivationRevision != 0 {
					t.Fatal("registration required or fabricated runtime activation", err)
				}
				replay, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: checkpoint.OperationID})
				if err != nil || replay.Operation.Revision != registered.Operation.Revision {
					t.Fatal("registered replay restarted the ceremony", err)
				}
				admitted, err := f.connection.AdmitSessionAccount(ctx, registered.Operation.SessionAccount)
				if err != nil || admitted.Scope() != authority.DeploymentParent {
					t.Fatal("confirmed invocation lacked exact deployment authority", err)
				}
				admitted.Close()
				wrongScope := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: registered.Operation.SessionAccount}
				if _, _, err := wrongScope.AdmitExecution(ctx, f.connection); err == nil {
					t.Fatal("deployment possession granted runtime authority")
				}
				completion.failUnlink = true
				loggedOut := logout(registered.Operation)
				if !loggedOut.Operation.LogoutCompleted || loggedOut.Operation.Phase != channelonboarding.PhaseRetired {
					t.Fatal("deployment account logout required an operator or runtime authority")
				}
			})
		}
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
				resumed.Operation.ActivationRevision != 0 || resumed.Binding != nil {
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
