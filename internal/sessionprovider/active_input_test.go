//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type activeInputStore interface {
	sessionInputSelectedStore
	pipeline.StandingServicePersistence
}

type activeInputFixture struct {
	ctx        context.Context
	claimEvent capturedEvent
	claim      operatorchannel.InboundClaim
	selected   activeInputStore
	state      *sessionState
	sender     *waStore.Device
	occurrence *clientOccurrence
	peer       *sdkPeer
	spool      *captureStore
	owner      *sessionInputOwner
	authority  *sessionAuthorityOwner
	identities *operatorchannel.Service
	channel    packs.SatisfactionPlan
	trigger    providertriggers.InboundAdmissionPlan
	operation  channelonboarding.Operation
	activation channelonboarding.ConnectedChannelActivation
	binding    operatorchannel.Binding
	standing   pipeline.StandingServiceReconciliation
	source     captureSource
	mu         sync.Mutex
	scope      captureScope
	handled    chan capturedEvent
}

// The peer supplies encrypted network frames; SDK decryption/capture, selected
// claim/confirmation/activation and standing admission are real. This is not a
// physical pairing, provider confirmation delivery, or served journey proof.
func newActiveInputFixture(t *testing.T, backend string) *activeInputFixture {
	t.Helper()
	f := &activeInputFixture{handled: make(chan capturedEvent, 1)}
	if backend == "sqlite" {
		f.selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected = storetest.AdmitPostgresRuntimeStore(t, db)
	}
	ctx, now := context.Background(), time.Now().UTC().Truncate(time.Microsecond)
	source := sourceartifactfixture.Require(t, ctx, f.selected)
	manifest := sessionInputManifestFixture(t)
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	f.trigger, err = catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "whatsapp", Provider: "whatsapp"})
	if err != nil {
		t.Fatal(err)
	}
	f.channel = activeInputChannelFixture(t, catalog)
	identity, err := f.channel.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	principal, err := f.selected.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	base, connectionID := t.TempDir(), uuid.NewString()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	f.state = sessionStateFixture(t, base, connectionID, "")
	device := newSDKDeviceFixture(t, f.state.container)
	_, senderStore := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "sender.db"))
	f.sender = newSDKDeviceFixture(t, senderStore)
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: connectionID,
		AccountRef: device.ID.ToNonAD().String(), AdmissionID: uuid.NewString(), Revision: 1}
	generation, err := f.channel.Generation()
	if err != nil {
		t.Fatal(err)
	}
	f.source = captureSource{Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: source.BundleHash(), BundleIdentity: "active-native-input", PackInventoryGeneration: "sha256:active-native-input",
		RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1}, CatalogGeneration: catalog.Generation()}
	id := uuid.NewString()
	f.operation, err = f.selected.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{OperationID: id,
		RequestKeyHash: id, RequestHash: id, PrincipalID: principal.ID, Verb: channelonboarding.VerbConnect, Provider: "whatsapp",
		Interface: identity, Coordinate: f.source.Coordinate, TargetSelector: "ingress:.:whatsapp",
		Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge, RequestedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity} {
		req := channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: phase, Now: now}
		if phase == channelonboarding.PhaseAwaitingExternalIdentity {
			req.SessionAccount = &account
		}
		f.operation, err = f.selected.AdvanceChannelOnboarding(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.peer = newSDKPeer(t)
	f.ctx = authoractivity.WithScope(correlation.WithSourceArtifactFact(f.peer.ctx, source), authoractivity.BundleScope(f.source.Coordinate.RuntimeInstanceID, source.BundleHash()))
	f.occurrence, err = f.state.newOccurrence(f.ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, f.spool = openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), connectionID)
	f.setScope(channelonboarding.SessionInputOnboarding)
	if _, err := f.occurrence.bindCallbacks(func(ctx context.Context, raw any) error {
		message, ok := raw.(*waEvents.Message)
		if !ok {
			return nil
		}
		f.mu.Lock()
		scope := f.scope
		f.mu.Unlock()
		event, err := captureSDKMessage(scope, f.source, f.occurrence.occurrenceID, message, time.Now().UTC().Truncate(time.Microsecond))
		if err != nil {
			return err
		}
		if err := f.spool.capture(ctx, event); err != nil {
			return err
		}
		f.handled <- event
		return nil
	}, f.spool.recordFailure); err != nil {
		t.Fatal(err)
	}
	f.peer.attach(t, f.occurrence.client)
	if err := f.occurrence.connect(); err != nil || !f.occurrence.client.WaitForConnection(5*time.Second) {
		t.Fatal("native connection", err)
	}
	reader, err := newSessionInputReader(f.state, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	f.owner, err = newSessionInputOwner(f.selected, reader, f.operation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	f.authority, err = newSessionAuthorityOwner(f.state, f.selected, f.operation, nil)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.identities, err = operatorchannel.NewService(f.selected, proofs, f.authority, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.identities.Bootstrap(ctx, now); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *activeInputFixture) setScope(kind channelonboarding.SessionInputScope) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scope = captureScope{Kind: kind, Session: f.operation.SessionAccount, Source: f.operation.Coordinate.DurableIdentity(),
		OnboardingOperation: f.operation.OperationID, OperationRevision: f.operation.Revision, TargetSelector: f.operation.TargetSelector,
		PrincipalID: f.operation.PrincipalID, BindingRevision: f.operation.BindingRevision}
	if kind == channelonboarding.SessionInputBusiness {
		f.scope.OperationRevision = f.activation.OperationRevision
		f.scope.ActivationRevision = f.activation.Revision
		f.scope.PublicationBinding = inbound.BindingGeneration{ServiceID: f.standing.ServiceID, RunID: f.standing.RunID, Generation: f.standing.Generation}
	}
}

func (f *activeInputFixture) receive(t *testing.T, text string) (capturedEvent, input.Admission) {
	t.Helper()
	device, err := f.state.device(f.peer.ctx)
	if err != nil {
		t.Fatal(err)
	}
	from := types.NewJID("100000000003", types.DefaultUserServer)
	from.Device = 1
	message := encryptedMessageFromFixture(t, device, f.sender, from, &waE2E.Message{Conversation: proto.String(text)})
	message.Attrs["id"] = uuid.NewString()
	f.peer.mu.Lock()
	socket := f.peer.peers[0]
	f.peer.mu.Unlock()
	if err := socket.send(f.peer.ctx, message); err != nil {
		t.Fatal(err)
	}
	var event capturedEvent
	select {
	case event = <-f.handled:
	case <-f.peer.ctx.Done():
		t.Fatal("no native capture")
	}
	admitted, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
		OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admitted.Close)
	return event, admitted
}

func (f *activeInputFixture) activate(t *testing.T) {
	t.Helper()
	ctx, now := f.ctx, time.Now().UTC().Truncate(time.Microsecond)
	authority := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}
	identity, err := f.identities.Begin(ctx, f.operation.Interface.Selector, operatorchannel.OperationConnect, 0,
		uuid.NewString(), uuid.NewString(), f.operation.OperationID, authority, false, now)
	if err != nil {
		t.Fatal(err)
	}
	f.operation, err = f.selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
		ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase, IdentityOperationID: identity.OperationID, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	f.setScope(channelonboarding.SessionInputOnboarding)
	event, admitted := f.receive(t, identity.Challenge)
	f.claimEvent = event
	sealed, claim, err := prepareSessionClaim(ctx, admitted, f.trigger, f.channel)
	if err != nil {
		t.Fatal(err)
	}
	f.claim = claim
	settlement, err := f.selected.SettleSessionChannelClaim(ctx, sealed)
	if err != nil || !settlement.Consumed || settlement.Disposition != operatorchannel.DispositionConsumedBinding {
		t.Fatalf("native claim: %+v %v", settlement, err)
	}
	receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(ctx, claim.PublicationID)
	if err != nil || !found || !receipt.Matches(claim) {
		t.Fatalf("exact claim receipt: %+v %t %v", receipt, found, err)
	}
	_, f.binding, err = f.identities.Confirm(ctx, settlement.Operation.OperationID, settlement.Operation.Revision, true, now)
	if err != nil || f.binding.Status != operatorchannel.BindingCurrent {
		t.Fatalf("real operator confirmation: %+v %v", f.binding, err)
	}
	admitted.Close()
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
		f.operation, err = f.selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
			ExpectedRevision: f.operation.Revision, Phase: phase, IdentityOperationID: identity.OperationID, BindingRevision: f.binding.Revision, Now: now})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.operation, f.activation, err = f.selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, ActivationID: uuid.NewString(),
		BindingRevision: f.binding.Revision, ConversationRef: f.binding.ConversationRef, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	source := sourceartifactfixture.Require(t, ctx, f.selected)
	f.standing, err = f.selected.ReconcileStandingService(ctx, pipeline.StandingServiceCandidate{BindingEnabled: f.activation.Status == channelonboarding.ActivationCurrent && f.binding.Status == operatorchannel.BindingCurrent,
		ServiceID: flowidentity.StandingServiceID("."), FlowPath: ".", Source: source})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.selected.PublishStandingService(ctx, f.standing.ServiceID, f.standing.RunID, f.standing.Generation); err != nil {
		t.Fatal(err)
	}
	f.setScope(channelonboarding.SessionInputBusiness)
}

func TestWhatsAppActiveBusinessInputUsesRealBindingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			event, admitted := f.receive(t, "business text")
			request, err := f.trigger.AdmitSessionInput(f.peer.ctx, admitted)
			if err != nil {
				t.Fatal(err)
			}
			delivery, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, ".")
			if err != nil || len(delivery.Events) != 2 || delivery.Events[1].Payload["text"] != "business text" {
				t.Fatalf("genuine business seal: %+v %v", delivery, err)
			}
			if !admitted.ReceivedAt().Equal(event.ReceivedAt) {
				t.Fatal("changed receipt time")
			}
			admitted.Close()
			if _, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, "."); err == nil {
				t.Fatal("released input retained business authority")
			}
		})
	}
}

func TestWhatsAppSealedBusinessInputRejectsUnbindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			_, admitted := f.receive(t, "business before unbind")
			request, err := f.trigger.AdmitSessionInput(f.ctx, admitted)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, "."); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, "."); err == nil {
				t.Fatal("unbound operator binding retained native business authority")
			}
		})
	}
}

func TestWhatsAppActiveBusinessSealTemporalFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			_, admitted := f.receive(t, "frozen business")
			request, err := f.trigger.AdmitSessionInput(f.ctx, admitted)
			if err != nil {
				t.Fatal(err)
			}
			delivery, seal, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, ".")
			if err != nil {
				t.Fatal(err)
			}
			route, err := events.NewExternalIngressRoutingSource(".", events.RoutingSourceAuthorityProviderAdmissionPlan)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(delivery.Events[0].Payload)
			if err != nil {
				t.Fatal(err)
			}
			output := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), delivery.Events[0].Name, "inbound-gateway", "", body, 0,
				f.standing.RunID, events.EventEnvelope{}, route, admitted.ReceivedAt())
			if err := seal.ValidateOutput(f.operation.Coordinate.BundleHash, "whatsapp", 0, 2, output, provideroutput.KindRaw, provideroutput.Authorization{}); err != nil {
				t.Fatal(err)
			}
			wrongRun := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), delivery.Events[0].Name, "inbound-gateway", "", body, 0,
				uuid.NewString(), events.EventEnvelope{}, route, admitted.ReceivedAt())
			if err := seal.ValidateOutput(f.operation.Coordinate.BundleHash, "whatsapp", 0, 2, wrongRun, provideroutput.KindRaw, provideroutput.Authorization{}); err == nil {
				t.Fatal("seal accepted another run")
			}
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
				f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
					ExpectedRevision: f.operation.Revision, Phase: phase, Now: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, "."); err != nil {
					t.Fatalf("completion progress replaced frozen activation: %v", err)
				}
			}
			if _, err := f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{SlotKey: f.activation.SlotKey,
				ExpectedActivationRevision: f.activation.Revision, Reason: "native temporal fence", Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.trigger.ProjectPublication(request, f.operation.Coordinate.BundleHash, "."); err == nil {
				t.Fatal("retired activation retained a business seal")
			}
			if err := seal.ValidateOutput(f.operation.Coordinate.BundleHash, "whatsapp", 0, 2, output, provideroutput.KindRaw, provideroutput.Authorization{}); err == nil {
				t.Fatal("old seal survived activation retirement")
			}
		})
	}
}

func TestWhatsAppNativeClaimReceiptRecoveryAfterRetirementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			if err := f.state.retireOccurrence(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: f.claimEvent.Scope.Session.ConnectionID, OccurrenceID: f.claimEvent.OccurrenceID,
				Conversation: f.claimEvent.Conversation, EventID: f.claimEvent.EventID, Kind: f.claimEvent.Kind}); err == nil {
				t.Fatal("dead onboarding capture regained execution")
			}
			for _, stage := range []string{"original", "reopen", "new_occurrence", "changed_body"} {
				t.Run(stage, func(t *testing.T) {
					event := f.claimEvent
					if stage == "new_occurrence" {
						event.OccurrenceID = uuid.NewString()
						event.ReceivedAt = event.ReceivedAt.Add(time.Second)
					}
					if stage == "changed_body" {
						event.Body = []byte(`{"kind":"message","text":"different"}`)
					}
					path := filepath.Join(t.TempDir(), "capture.db")
					db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
					if err := spool.capture(f.ctx, event); err != nil {
						t.Fatal(err)
					}
					if stage == "reopen" {
						if err := db.Close(); err != nil {
							t.Fatal(err)
						}
						_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
					}
					settled, err := spool.reconcileSessionClaim(f.ctx, event, f.selected)
					if stage == "changed_body" {
						if !errors.Is(err, operatorchannel.ErrConflict) || settled {
							t.Fatalf("changed history accepted: %t %v", settled, err)
						}
						rows, err := spool.pending(f.ctx)
						if err != nil || len(rows) != 1 {
							t.Fatalf("failed reconciliation lost capture: %d %v", len(rows), err)
						}
					} else if err != nil || !settled {
						t.Fatalf("committed history needed live SDK/replanning: %t %v", settled, err)
					}
					receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(f.ctx, f.claim.PublicationID)
					if err != nil || !found || !receipt.Matches(f.claim) || !receipt.RecordedAt.Equal(f.claimEvent.ReceivedAt) {
						t.Fatalf("original receipt changed: %+v %t %v", receipt, found, err)
					}
				})
			}
		})
	}
}

func TestWhatsAppUncommittedOnboardingCaptureDoesNotAdoptAdvancementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			event, admitted := f.receive(t, "original pending input")
			admitted.Close()
			if settled, err := f.spool.reconcileSessionClaim(f.ctx, event, f.selected); err != nil || settled {
				t.Fatalf("uncommitted input fabricated history: %t %v", settled, err)
			}
			op, err := f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
				ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase, Now: time.Now().UTC()})
			if err != nil || op.Revision <= f.operation.Revision {
				t.Fatal("did not advance responsibility", err)
			}
			if value, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID, OccurrenceID: event.OccurrenceID,
				Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind}); err == nil {
				value.Close()
				t.Fatal("original pending capture adopted a new operation revision")
			}
			rows, err := f.spool.pending(f.ctx)
			if err != nil || len(rows) != 1 || !rows[0].sameCapture(event) {
				t.Fatalf("refusal lost original pending evidence: %+v %v", rows, err)
			}
		})
	}
}

func TestWhatsAppSessionAuthorityIsOwnedAndExactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			retained := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}
			foreignOperation := f.operation
			foreignOperation.Provider, foreignOperation.SessionAccount.Provider = "discord", "discord"
			if _, err := newSessionAuthorityOwner(f.state, f.selected, foreignOperation, nil); err == nil {
				t.Fatal("WhatsApp SDK issued another provider's account authority")
			}
			admitted, current, err := retained.AdmitExecution(f.ctx, f.authority)
			if err != nil || !current || admitted.RequireExecutable() != nil {
				t.Fatalf("native account authority: %t %v", current, err)
			}
			defer admitted.CloseExecution()
			for i, edit := range []func(*operatorchannel.SessionAccountAdmission){
				func(a *operatorchannel.SessionAccountAdmission) { a.ConnectionID = uuid.NewString() },
				func(a *operatorchannel.SessionAccountAdmission) { a.AccountRef = "other@s.whatsapp.net" },
				func(a *operatorchannel.SessionAccountAdmission) { a.AdmissionID = uuid.NewString() },
				func(a *operatorchannel.SessionAccountAdmission) { a.Revision++ },
			} {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					wrong := retained
					edit(&wrong.Session)
					if value, current, err := wrong.AdmitExecution(f.ctx, f.authority); err == nil || current {
						value.CloseExecution()
						t.Fatal("replacement account acquired native authority")
					}
				})
			}
			copy := admitted
			admitted.CloseExecution()
			if copy.RequireExecutable() == nil {
				t.Fatal("copied admission survived shared release")
			}
			if retained.RequireExecutable() == nil {
				t.Fatal("retained account metadata is executable")
			}
		})
	}
}

func TestWhatsAppNativeIdentityBeginRejectsStaleParentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			retained := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}
			admitted, current, err := retained.AdmitExecution(f.ctx, f.authority)
			if err != nil || !current {
				t.Fatal(err)
			}
			defer admitted.CloseExecution()
			if _, err := f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
				ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase, Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			if _, err := f.selected.BeginChannelBinding(f.ctx, operatorchannel.BeginRequest{OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect,
				PrincipalID: f.operation.PrincipalID, Interface: f.operation.Interface, RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
				OnboardingOperationID: f.operation.OperationID, ProviderAuthority: admitted, RequestedAt: now, ExpiresAt: now.Add(time.Minute)}); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
				t.Fatalf("stale native admission began identity: %v", err)
			}
			rows, err := f.selected.ListOperatorChannelOperations(f.ctx, f.operation.PrincipalID)
			if err != nil || len(rows) != 0 {
				t.Fatalf("stale refusal mutated identity: %+v %v", rows, err)
			}
		})
	}
}

func activeInputChannelFixture(t *testing.T, catalog *providertriggers.CatalogSnapshot) packs.SatisfactionPlan {
	t.Helper()
	body := []byte(`provider: whatsapp
transport: session
capabilities: {card_render: true, reply_to_reference: true, actions_as_buttons: false, actions_as_text: true, edit: false, acknowledgment: false, inbox_listing: true}
onboarding:
  ceremony: authenticated_text_challenge
  confirmation: deliver
  connection_health: provider_connection
  learned_destination: {destination: conversation_reference}
opaque_types:
  destination: {type: string, minLength: 1}
  delivery_reference: {type: string, minLength: 1}
  external_account_reference: {type: string, minLength: 1}
  conversation_reference: {type: string, minLength: 1}
operations:
  deliver:
    tool: whatsapp.fixture_send
    input: {destination: context.destination, text: input.presentation.text}
    output: {delivery_reference: result.id}
events:
  text:
    event: inbound.whatsapp.message
    fields:
      text: event.text
      external_account_reference: event.external_account_reference
      conversation_reference: event.conversation_reference
      conversation_scope: event.conversation_scope
      provider_message_reference: event.provider_message_reference
      reply_to_message_reference: event.reply_to_message_reference
      entry_invocation: event.entry_invocation
`)
	manifest, err := packs.ParseChannelManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	pack := packs.LoadedChannelPack{Envelope: packs.Envelope{ID: "provider.whatsapp.hitl_channel", Version: "0.1.0", Type: packs.TypeChannel,
		ManifestHash: packs.ManifestHash(body), Implements: []string{operatorchannel.InterfaceHITLChannelV2}, Provenance: packs.Provenance{Source: "external"},
		Requires: packs.Requires{Packs: map[string]string{packs.TypeTrigger: "provider.whatsapp.input", packs.TypeConnector: "provider.whatsapp.fixture_connector"}}},
		Manifest: manifest, Source: packs.MustPackSource("test", "active-native-input")}
	text := contracts.MustToolInputSchema(contracts.ToolSchemaString, contracts.ToolSchemaMinLength(1))
	presentationText := contracts.MustToolInputSchema(contracts.ToolSchemaString, contracts.ToolSchemaMinLength(1), contracts.ToolSchemaMaxLength(2000))
	object := func(properties map[string]contracts.ToolInputSchema, required ...string) contracts.ToolInputSchema {
		return contracts.MustToolInputSchema(contracts.ToolSchemaObject, contracts.ToolSchemaProperties(properties), contracts.ToolSchemaRequired(required...))
	}
	connector := packs.ConnectorPackDescriptor{Identity: packs.MustPackIdentity("provider.whatsapp.fixture_connector", "0.1.0", packs.ManifestHash([]byte("fixture-connector")), packs.TypeConnector, packs.MustPackSource("test", "active-native-input")),
		Provider: "whatsapp", Tools: map[string]contracts.ToolSchemaEntry{"whatsapp.fixture_send": contracts.MustToolSchemaEntry(
			contracts.WithToolCategory(contracts.ToolCategoryProviderConnector.String()), contracts.WithToolHandler(contracts.ToolHandlerHTTP),
			contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
			contracts.WithToolSchemas(object(map[string]contracts.ToolInputSchema{"destination": text, "text": presentationText}, "destination", "text"), object(map[string]contracts.ToolInputSchema{"id": text}, "id")),
			contracts.WithToolHTTP(contracts.HTTPToolSpec{Method: "POST", URL: "http://fixture.invalid/never-called", Body: map[string]any{"text": "{{input.text}}"}}),
			contracts.WithToolResponseSuccess(contracts.HTTPResponseSuccess{Kind: "http_status_2xx"}), contracts.WithToolResponseMapping(map[string]any{"id": "{{response.body.id}}"}))}}
	snapshot, err := yamlsource.LoadFile("../../platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := packs.CompileChannel(registry, pack, catalog.PackDescriptors(), []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
