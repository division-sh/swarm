//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	nativeinput "github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type sessionInputSelectedStore interface {
	channelonboarding.Store
	operatorchannel.Store
	sourceartifactfixture.Writer
}

func TestWhatsAppOwnedSessionInputBootstrapBothStores(t *testing.T) {
	runOwnedSessionInputBootstrapBothStores(t, 0, channelonboarding.PhaseAwaitingExternalIdentity, 0)
}

func TestReviewerOnboardingPositiveTargetCannotPublishBothStores(t *testing.T) {
	runOwnedSessionInputBootstrapBothStores(t, 1, channelonboarding.PhaseAwaitingExternalIdentity, 0)
}

func TestOnboardingInputNeverBusinessAcrossPrebindingPhasesBothStores(t *testing.T) {
	for _, target := range []uint64{0, 1} {
		for _, row := range []struct {
			phase   channelonboarding.Phase
			binding int64
		}{
			{channelonboarding.PhaseActivatingProvider, 0},
			{channelonboarding.PhaseAwaitingOperatorConfirmation, 0},
			{channelonboarding.PhaseAwaitingOperatorConfirmation, 1},
			{channelonboarding.PhasePublishingActivation, 1},
		} {
			if target == 0 && row.phase == channelonboarding.PhasePublishingActivation {
				continue
			}
			t.Run(fmt.Sprintf("target_%d/%s/binding_%d", target, row.phase, row.binding), func(t *testing.T) {
				runOwnedSessionInputBootstrapBothStores(t, target, row.phase, row.binding)
			})
		}
	}
}

func runOwnedSessionInputBootstrapBothStores(t *testing.T, targetGeneration uint64, phase channelonboarding.Phase, bindingRevision int64) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected sessionInputSelectedStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			ctx, now := context.Background(), time.Now().UTC()
			source := sourceartifactfixture.Require(t, ctx, selected)
			principal, err := selected.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			base, connectionID := t.TempDir(), uuid.NewString()
			if err := os.Chmod(base, 0o700); err != nil {
				t.Fatal(err)
			}
			state := sessionStateFixture(t, base, connectionID, "")
			device := newSDKDeviceFixture(t, state.container)
			account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: connectionID,
				AccountRef: device.ID.ToNonAD().String(), AdmissionID: uuid.NewString(), Revision: 1}
			generation, err := plangeneration.FromCanonicalValue(map[string]string{"native": "session-input"})
			if err != nil {
				t.Fatal(err)
			}
			coordinate := channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: source.BundleHash(),
				BundleIdentity: "owned-session-input", PackInventoryGeneration: "sha256:session-input", RuntimeInstanceID: uuid.NewString(),
				ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: targetGeneration}
			id := uuid.NewString()
			op, err := selected.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{OperationID: id,
				RequestKeyHash: id, RequestHash: id, PrincipalID: principal.ID, Verb: channelonboarding.VerbConnect,
				Provider: "whatsapp", Interface: operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2,
					ChannelPackID: "provider.whatsapp.hitl_channel", ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:input", SemanticGeneration: "sha256:input"},
				Coordinate: coordinate, TargetSelector: "ingress:reception:whatsapp", Posture: channelonboarding.ActivationSessionConnection,
				Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge, RequestedAt: now})
			if err != nil {
				t.Fatal(err)
			}
			for _, nextPhase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity} {
				if phase == channelonboarding.PhaseActivatingProvider && nextPhase == channelonboarding.PhaseAwaitingExternalIdentity {
					break
				}
				request := channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: nextPhase, Now: now}
				if nextPhase == channelonboarding.PhaseAwaitingExternalIdentity || phase == channelonboarding.PhaseActivatingProvider && nextPhase == phase {
					request.SessionAccount = &account
				}
				op, err = selected.AdvanceChannelOnboarding(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == channelonboarding.PhaseAwaitingOperatorConfirmation || phase == channelonboarding.PhasePublishingActivation {
				op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
					ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, BindingRevision: bindingRevision, Now: now})
				if err != nil {
					t.Fatal(err)
				}
				if phase == channelonboarding.PhasePublishingActivation {
					op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
						ExpectedRevision: op.Revision, Phase: phase, BindingRevision: bindingRevision, Now: now})
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			manifest := sessionInputManifestFixture(t)
			catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
				Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "reception", Provider: "whatsapp"})
			if err != nil {
				t.Fatal(err)
			}
			originalSource := captureSource{Coordinate: coordinate, CatalogGeneration: catalog.Generation()}
			peer := newSDKPeer(t)
			occurrence, err := state.newOccurrence(peer.ctx, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			_, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), connectionID)
			scope := captureScope{Kind: channelonboarding.SessionInputOnboarding, Session: account, Source: coordinate.DurableIdentity(),
				OnboardingOperation: op.OperationID, OperationRevision: op.Revision, TargetSelector: op.TargetSelector, PrincipalID: principal.ID, BindingRevision: op.BindingRevision}
			handled := make(chan capturedEvent, 1)
			if _, err := occurrence.bindCallbacks(func(ctx context.Context, raw any) error {
				message, ok := raw.(*waEvents.Message)
				if !ok {
					return nil
				}
				event, err := captureSDKMessage(scope, originalSource, occurrence.occurrenceID, message, time.Now().UTC().Truncate(time.Microsecond))
				if err != nil {
					return err
				}
				if err := spool.capture(ctx, event); err != nil {
					return err
				}
				handled <- event
				return nil
			}, spool.recordFailure); err != nil {
				t.Fatal(err)
			}
			peer.attach(t, occurrence.client)
			if err := occurrence.connect(); err != nil || !occurrence.client.WaitForConnection(5*time.Second) {
				t.Fatal("native SDK authentication failed", err)
			}
			from := types.NewJID("100000000003", types.DefaultUserServer)
			from.Device = 1
			message := encryptedMessageFixture(t, device, from, &waE2E.Message{Conversation: proto.String("open inbox")})
			peer.mu.Lock()
			socket := peer.peers[0]
			peer.mu.Unlock()
			if err := socket.send(peer.ctx, message); err != nil {
				t.Fatal(err)
			}
			var event capturedEvent
			select {
			case event = <-handled:
			case <-peer.ctx.Done():
				t.Fatal("SDK did not decrypt/capture input")
			}
			reader, err := newSessionInputReader(state, spool)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := newSessionInputOwner(selected, reader, op.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			reference := SessionInputReference{ConnectionID: connectionID, OccurrenceID: occurrence.occurrenceID,
				Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind}
			input, err := owner.admit(peer.ctx, reference)
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			admitted, err := plan.AdmitSessionInput(peer.ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			if !input.ReceivedAt().Equal(event.ReceivedAt) {
				t.Fatal("native input substituted the original capture receipt time")
			}
			delivery, err := plan.ProjectDelivery(admitted)
			if err != nil || len(delivery.Events) != 2 || delivery.Events[1].Payload["text"] != "open inbox" {
				t.Fatalf("owned native input projection: %+v %v", delivery, err)
			}
			if _, _, err := plan.ProjectPublication(admitted, source.BundleHash(), "reception"); err == nil {
				t.Fatal("runless operator input acquired business publication")
			}
			body := input.Body()
			body[0] = 'x'
			if _, err := plan.ProjectDelivery(admitted); err != nil {
				t.Fatalf("readback mutated owner input: %v", err)
			}
			var forged nativeinput.Admission
			if err := json.Unmarshal([]byte(`{"account":"claimed","body":"claimed","authorized":true}`), &forged); err != nil {
				t.Fatal(err)
			}
			if _, err := plan.AdmitSessionInput(peer.ctx, forged); err == nil {
				t.Fatal("serialized descriptor minted session admission")
			}
			for _, edit := range []func(*SessionInputReference){
				func(r *SessionInputReference) { r.ConnectionID = uuid.NewString() },
				func(r *SessionInputReference) { r.OccurrenceID = uuid.NewString() },
				func(r *SessionInputReference) { r.EventID = "missing" },
			} {
				changed := reference
				edit(&changed)
				if value, err := owner.admit(peer.ctx, changed); err == nil {
					value.Close()
					t.Fatal("foreign native input admitted")
				}
			}
			for _, row := range []struct {
				name string
				edit func(*capturedEvent)
			}{
				{"account", func(e *capturedEvent) { e.Scope.Session.AccountRef = "100000000099@s.whatsapp.net" }},
				{"admission", func(e *capturedEvent) { e.Scope.Session.AdmissionID = uuid.NewString() }},
				{"account_revision", func(e *capturedEvent) { e.Scope.Session.Revision++ }},
				{"operation", func(e *capturedEvent) { e.Scope.OnboardingOperation = uuid.NewString() }},
				{"operation_revision", func(e *capturedEvent) { e.Scope.OperationRevision++ }},
				{"principal", func(e *capturedEvent) { e.Scope.PrincipalID = uuid.NewString() }},
				{"source", func(e *capturedEvent) { e.Scope.Source.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) }},
				{"inventory", func(e *capturedEvent) { e.Scope.Source.PackInventoryGeneration = "sha256:foreign" }},
				{"target", func(e *capturedEvent) { e.Scope.TargetSelector = "ingress:other:whatsapp" }},
				{"business", func(e *capturedEvent) {
					e.Scope.Kind = channelonboarding.SessionInputBusiness
					e.Scope.PublicationBinding = captureFixture(t).Scope.PublicationBinding
					e.Scope.ActivationRevision, e.Scope.BindingRevision = 1, 1
				}},
			} {
				t.Run(row.name, func(t *testing.T) {
					foreign := event
					foreign.EventID = "FOREIGN_" + row.name
					row.edit(&foreign)
					foreign.Source.Coordinate.BundleHash = foreign.Scope.Source.BundleHash
					foreign.Source.Coordinate.BundleIdentity = foreign.Scope.Source.BundleIdentity
					foreign.Source.Coordinate.PackInventoryGeneration = foreign.Scope.Source.PackInventoryGeneration
					foreign.Source.Coordinate.PlanGeneration = foreign.Scope.Source.PlanGeneration
					if err := spool.capture(ctx, foreign); err != nil {
						t.Fatal(err)
					}
					ref := reference
					ref.EventID = foreign.EventID
					if value, err := owner.admit(peer.ctx, ref); err == nil {
						value.Close()
						t.Fatal("structural capture evidence acquired native input authority")
					}
				})
			}
			foreignGeneration, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
				Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "2.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := input.Validate(peer.ctx, "whatsapp", foreignGeneration.Generation()); err == nil {
				t.Fatal("native input crossed its frozen catalog generation")
			}
			canceled, cancel := context.WithCancel(peer.ctx)
			cancelStore := &sessionInputCancelLookupStore{sessionInputSelectedStore: selected, cancel: cancel}
			cancelOwner, err := newSessionInputOwner(cancelStore, reader, op.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if value, err := cancelOwner.admit(canceled, reference); err == nil {
				value.Close()
				t.Fatal("lookup cancellation disclosed executable native input")
			}
			cancel()
			input.Close()
			if _, err := plan.ProjectDelivery(admitted); err == nil {
				t.Fatal("released native input remained executable")
			}
			if phase != channelonboarding.PhaseAwaitingExternalIdentity {
				return
			}
			pendingInput, err := owner.admit(peer.ctx, reference)
			if err != nil {
				t.Fatal(err)
			}
			defer pendingInput.Close()
			pendingAdmission, err := plan.AdmitSessionInput(peer.ctx, pendingInput)
			if err != nil {
				t.Fatal(err)
			}
			op, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := plan.ProjectDelivery(pendingAdmission); err == nil {
				t.Fatal("planned input remained executable after responsibility advancement")
			}
			if value, err := owner.admit(peer.ctx, reference); err == nil {
				value.Close()
				t.Fatal("old capture adopted a later onboarding revision")
			}
		})
	}
}

type sessionInputCancelLookupStore struct {
	sessionInputSelectedStore
	cancel context.CancelFunc
}

func (s *sessionInputCancelLookupStore) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	op, err := s.sessionInputSelectedStore.GetChannelOnboarding(ctx, id)
	s.cancel()
	return op, err
}

func sessionInputManifestFixture(t *testing.T) providertriggers.Manifest {
	t.Helper()
	body := string(incomingNormalizationFixture(t).SourceBytes())
	body = strings.Replace(body, "secret: {required: true}\nsignature: {type: token_equality, header: X-Fixture-Signature}\n", "transport: session\nack: {mode: durable_before_dispatch}\n", 1)
	body = strings.ReplaceAll(body, "      provider_timestamp_ms:\n", `      entry_invocation:
        from: entry_invocation
        optional: true
        schema:
          type: object
          additionalProperties: false
          required: [reference]
          properties:
            reference: {type: string, minLength: 1, maxLength: 32}
            address: {type: string, minLength: 5, maxLength: 32}
      provider_timestamp_ms:
`)
	manifest, err := providertriggers.ParseManifest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}
