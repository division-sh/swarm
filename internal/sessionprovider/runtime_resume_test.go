//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	credentials "github.com/division-sh/swarm/internal/runtime/credentials"
	effects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func startPairedRuntimeBootstrapFixture(t *testing.T, f *runtimeBootstrapFixture) channelonboarding.Result {
	t.Helper()
	started, err := f.service.Start(f.ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect,
		Selection: channelonboarding.CandidateSelection{Provider: "whatsapp"}, IdempotencyKey: "restore-paired-session"})
	if err != nil {
		t.Fatal(err)
	}
	accountRef := pairedBootstrapStateFixture(t, f, started.Operation)
	if err := f.BootstrapSession(f.ctx, started.Operation, f.candidate); err != nil {
		t.Fatal(err)
	}
	if !f.connection.state.currentOccurrence().client.WaitForConnection(5 * time.Second) {
		t.Fatal("paired fixture did not finish SDK authentication")
	}
	resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: started.Operation.OperationID})
	if err != nil || resumed.IdentityOperation == nil || resumed.IdentityOperation.State != operatorchannel.StateAwaitingClaim ||
		resumed.Operation.SessionAccount.AccountRef != accountRef || resumed.Operation.ActivationRevision != 0 {
		t.Fatal("paired setup did not reach the actual claim owner", resumed, err)
	}
	return resumed
}

func TestWhatsAppPublicResumeRestoresOriginalPairedResponsibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			original := startPairedRuntimeBootstrapFixture(t, f)
			retired := f.connection
			if err := retired.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.connection = nil
			f.peer = newSDKPeer(t)
			resumed, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: original.Operation.OperationID})
			if err != nil || f.connection == nil || f.connection == retired {
				t.Fatal("public resume did not restore the retained SDK responsibility", err)
			}
			if err := f.connection.CheckBootstrap(f.ctx); err != nil {
				t.Fatal("resume returned without a current connection", err)
			}
			if resumed.Operation.SessionConnectionID != original.Operation.SessionConnectionID ||
				resumed.Operation.SessionAccount != original.Operation.SessionAccount ||
				resumed.IdentityOperation == nil || resumed.IdentityOperation.OperationID != original.IdentityOperation.OperationID ||
				resumed.Operation.ActivationRevision != 0 || resumed.Operation.Coordinate.TargetGeneration != 0 || resumed.Binding != nil {
				t.Fatal("resume replaced identity or manufactured execution", resumed)
			}
			connection := f.connection
			if _, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: original.Operation.OperationID}); err != nil || f.connection != connection {
				t.Fatal("healthy resume reconnected or replaced its owner", err)
			}
		})
	}
}

func TestWhatsAppConnectionSurvivesTargetAdmissionWithoutGrantingBusinessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			original := startPairedRuntimeBootstrapFixture(t, f)
			oldAdmission, err := f.connection.AdmitSessionAccount(f.ctx, original.Operation.SessionAccount)
			if err != nil {
				t.Fatal(err)
			}
			defer oldAdmission.Close()
			coordinate := original.Operation.Coordinate
			coordinate.TargetGeneration = 1
			current, err := f.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: original.Operation.OperationID, ExpectedRevision: original.Operation.Revision,
				Phase: original.Operation.Phase, RebindCoordinate: &coordinate, Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.connection.CheckBootstrap(f.ctx); err != nil {
				t.Fatal("business target progression invalidated the original bot connection", err)
			}
			if id, revision := oldAdmission.Parent(); id != original.Operation.OperationID || revision != original.Operation.Revision || revision == current.Revision {
				t.Fatal("held possession fact adopted a newer selected parent", id, revision)
			}
			admission, err := f.connection.AdmitSessionAccount(f.ctx, original.Operation.SessionAccount)
			if err != nil {
				t.Fatal("target progression replaced physical account authority", err)
			}
			if _, revision := admission.Parent(); revision != current.Revision {
				t.Fatal("fresh native admission did not retain the exact current parent", revision)
			}
			admission.Close()
			if current.SessionAccount != original.Operation.SessionAccount || current.ActivationRevision != 0 ||
				current.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
				t.Fatal("target coordinate admitted human or business authority", current)
			}
			toolID, tool, err := f.candidate.Plan.ConnectorOperation("deliver")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.connection.ExecuteChannelWrite(f.ctx, uuid.NewString(), toolID, tool,
				map[string]any{"destination": "customer@s.whatsapp.net", "text": "unauthorized"}, nil, effects.AuthorityChannelDelivery); err == nil {
				t.Fatal("connection continuity granted business delivery without binding/activation")
			}
		})
	}
}

func TestWhatsAppPublicRecoveryRestoresOriginalPairedResponsibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newRuntimeBootstrapFixture(t, backend)
			original := startPairedRuntimeBootstrapFixture(t, f)
			if err := f.connection.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.connection = nil
			f.peer = newSDKPeer(t)
			if err := f.service.Recover(f.ctx); err != nil || f.connection == nil {
				t.Fatal("existing recovery owner did not restore the session", err)
			}
			connection := f.connection
			if err := f.service.Recover(f.ctx); err != nil || f.connection != connection {
				t.Fatal("recovery repeated the SDK connection", err)
			}
			current, err := f.store.GetChannelOnboarding(f.ctx, original.Operation.OperationID)
			if err != nil || current.SessionAccount != original.Operation.SessionAccount || current.IdentityOperationID != original.Operation.IdentityOperationID ||
				current.Revision != original.Operation.Revision || current.ActivationRevision != 0 || current.Coordinate.TargetGeneration != 0 {
				t.Fatal("recovery rewrote exact identity or enabled business", current, err)
			}
			caller, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := f.service.Recover(caller); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled recovery acquired work", err)
			}
		})
	}
}

func TestWhatsAppSucceededRecoveryRestoresOnlyCurrentActivationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			a := newActiveInputFixture(t, backend)
			a.activate(t) // genuine SDK input, human confirmation and selected activation.
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration,
				channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
				var err error
				a.operation, err = a.selected.AdvanceChannelOnboarding(a.ctx, channelonboarding.AdvanceRequest{
					OperationID: a.operation.OperationID, ExpectedRevision: a.operation.Revision, Phase: phase, Now: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := a.state.close(a.ctx); err != nil {
				t.Fatal(err)
			}
			file, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			current, err := credentials.NewSnapshotOwner(file)
			if err != nil {
				t.Fatal(err)
			}
			candidate := channelonboarding.Candidate{Provider: a.operation.Provider, Interface: a.operation.Interface,
				Coordinate: a.operation.Coordinate, Plan: a.channel, Posture: a.operation.Posture, Ceremony: a.operation.Ceremony,
				ConfirmationOperation: "deliver", ConnectionHealth: "provider_connection",
				Target: channelonboarding.CandidateTarget{Selector: a.operation.TargetSelector, FlowPath: ".", Provider: "whatsapp", Alias: "whatsapp",
					ServiceID: flowidentity.StandingServiceID("."), Generation: a.operation.Coordinate.TargetGeneration,
					PublicationSequence: a.standing.PublicationSequence, AdmissionGeneration: a.catalog.Generation()}}
			f := &runtimeBootstrapFixture{t: t, ctx: a.ctx, store: a.selected, peer: newSDKPeer(t), owner: a.workOwner,
				directory: a.basePath, candidate: candidate, credentials: current}
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			f.channels, err = operatorchannel.NewService(a.selected, proofs, f, []operatorchannel.InterfaceIdentity{candidate.Interface}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.channels.Bootstrap(f.ctx, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			writer, err := channelonboarding.NewCredentialWriter(file)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
			if err != nil {
				t.Fatal(err)
			}
			f.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{Store: a.selected, SourceArtifacts: a.selected.(sourceartifact.Reader),
				Identities: f.channels, Credentials: writer, Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil },
				Sessions: f, Activations: f, Confirmation: f, Readiness: f})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.service.Recover(f.ctx); err != nil || f.connection == nil {
				t.Fatal("successful current slot lost SDK restoration", err)
			}
			if err := f.connection.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			f.connection = nil
			if _, err := a.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
				SlotKey: a.activation.SlotKey, ExpectedActivationRevision: a.activation.Revision, Reason: "retire recovery proof", Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if err := f.service.Recover(f.ctx); err != nil || f.connection != nil {
				t.Fatal("retired successful slot reopened provider state", err)
			}
			if _, err := f.service.Retry(f.ctx, channelonboarding.RetryInput{OperationID: a.operation.OperationID}); !errors.Is(err, channelonboarding.ErrRevisionConflict) || f.connection != nil {
				t.Fatal("public resume reopened retired successful history", err)
			}
		})
	}
}
