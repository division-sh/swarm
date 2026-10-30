//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func sendRuntimeIncomingFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id, text string) {
	t.Helper()
	sendRuntimeIncomingAtFixture(t, f, c, id, text, time.Now())
}

func sendRuntimeIncomingAtFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id, text string, at time.Time) {
	sendRuntimeIncomingContentFixture(t, f, c, id, &waE2E.Message{Conversation: proto.String(text)}, at)
}

func sendRuntimeIncomingContentFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id string, content *waE2E.Message, at time.Time) {
	t.Helper()
	device, err := c.state.device(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	from := types.NewJID("100000000003", types.DefaultUserServer)
	from.Device = 1
	message := encryptedMessageFromFixture(t, device, f.sender, from, content)
	message.Attrs["id"], message.Attrs["t"] = id, at.Unix()
	f.peer.mu.Lock()
	socket := f.peer.peers[len(f.peer.peers)-1]
	f.peer.mu.Unlock()
	if err := socket.send(f.peer.ctx, message); err != nil {
		t.Fatal(err)
	}
}

func waitRuntimeIncomingReceipt(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id string) {
	t.Helper()
	for {
		select {
		case frame := <-f.peer.protocol:
			if frame.node.Tag == "receipt" && frame.node.AttrGetter().String("id") == id {
				return
			}
		case <-c.state.currentOccurrence().callbacks.drained:
			t.Fatal("runtime incoming publication failed", c.state.currentOccurrence().callbacks.currentFailure())
		case <-f.peer.ctx.Done():
			t.Fatal("runtime incoming publication never acknowledged")
		}
	}
}

func TestWhatsAppRuntimeIncomingPublishesBeforeAcknowledgementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/registered", func(t *testing.T) {
			f := newActiveInputFixtureWithTarget(t, backend, false, false, 1, true)
			var err error
			f.standing, err = f.selected.ReconcileFlowRun(f.ctx, runlifecycle.LiveRunScope{FlowPath: "."}, sourceartifactfixture.FactFor(f.rooted.Artifact()))
			if err != nil {
				t.Fatal(err)
			}
			publication := inboundpublication.BindingGeneration{RunID: f.standing.RunID}
			incoming := RuntimeIncomingOptions{Alias: "whatsapp", Trigger: f.trigger, Bus: f.publicationBus(t), Posture: executionposture.Live}
			var held atomic.Bool
			var changedRun, changedSource atomic.Bool
			var releases atomic.Int64
			selector := func(ctx context.Context, op channelonboarding.Operation) (RegisteredBusinessBinding, error) {
				if op.OperationID != f.operation.OperationID || op.SessionAccount != f.operation.SessionAccount || !op.AccountConfirmed {
					return RegisteredBusinessBinding{}, channelonboarding.ErrRevisionConflict
				}
				if !held.Load() {
					return RegisteredBusinessBinding{}, ErrAccountUnclaimed
				}
				coordinate, selectedPublication := f.source.Coordinate, publication
				if changedRun.Load() {
					selectedPublication.RunID = uuid.NewString()
				}
				if changedSource.Load() {
					coordinate.BundleHash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				}
				return RegisteredBusinessBinding{Context: f.ctx, Incoming: incoming, Coordinate: coordinate,
					TargetSelector: "ingress:.:whatsapp", Publication: selectedPublication,
					Current: func(context.Context) error {
						if !held.Load() {
							return channelonboarding.ErrRevisionConflict
						}
						return f.ctx.Err()
					}, Release: func() error { releases.Add(1); return nil }}, nil
			}
			if err := f.state.close(f.ctx); err != nil {
				t.Fatal(err)
			}
			principal, err := f.selected.EnsureOperatorPrincipal(f.ctx, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			process := worklifetime.NewProcess()
			operations := f.selected.(interface {
				DeploymentChannelOperations() channelonboarding.OperationStore
			}).DeploymentChannelOperations()
			open := func() *RuntimeConnection {
				c, err := OpenDeploymentConnection(f.ctx, DeploymentConnectionOptions{Directory: f.basePath, Store: f.selected,
					Operations: operations, OperationID: f.operation.OperationID, Principal: principal, Process: process,
					Plan: f.channel, Trigger: f.trigger, RegisteredBusiness: selector})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := c.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				return c
			}
			t.Cleanup(func() {
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			})
			c := open()
			connectRuntimeConnectionFixture(t, f, c)
			unclaimed, at := uuid.NewString(), time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, unclaimed, "no current holder", at)
			waitRuntimeIncomingReceipt(t, f, c, unclaimed)
			if count, err := c.UnclaimedCount(f.ctx); err != nil || count != 1 {
				t.Fatal("unheld account did not retain exactly one terminal capture", count, err)
			}
			cards, _, err := f.selected.(decisioncard.Store).ListDecisionCards(f.ctx, decisioncard.ListOptions{Limit: 1})
			if err != nil || len(cards) != 0 {
				t.Fatal("unclaimed account traffic produced an operator Inbox card", len(cards), err)
			}
			held.Store(true)
			id, businessAt := uuid.NewString(), time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, id, "registered account customer input", businessAt)
			waitRuntimeIncomingReceipt(t, f, c, id)
			f.scope = captureScope{Kind: channelonboarding.SessionInputBusiness, Session: f.operation.SessionAccount,
				Source: f.source.Coordinate.DurableIdentity(), OnboardingOperation: f.operation.OperationID, OperationRevision: f.operation.Revision,
				PrincipalID: f.operation.PrincipalID, TargetSelector: "ingress:.:whatsapp", AccountOnly: true,
				DeploymentRegistration: true, RegisteredBusiness: true, PublicationBinding: publication}
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, runtimeIncomingIdentityFixture(t, f, id))
			if err != nil || !found || record.OutputCount != 2 {
				t.Fatal("registered account acknowledged before its exact current runtime publication", found, err)
			}
			original, err := publicationCaptureProvenance(record.Request)
			if err != nil || original.Scope != f.scope || original.Scope.ActivationID != "" || original.Scope.ActivationRevision != 0 {
				t.Fatal("registration was adopted as a learned runtime activation", err)
			}
			sendRuntimeIncomingAtFixture(t, f, c, unclaimed, "no current holder", at)
			waitRuntimeIncomingReceipt(t, f, c, unclaimed)
			if count, err := c.UnclaimedCount(f.ctx); err != nil || count != 1 {
				t.Fatal("old unclaimed delivery was reinterpreted after holder construction", count, err)
			}
			sendRuntimeIncomingAtFixture(t, f, c, id, "registered account customer input", businessAt)
			waitRuntimeIncomingReceipt(t, f, c, id)
			repeated, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, runtimeIncomingIdentityFixture(t, f, id))
			if err != nil || !found || repeated.Request.RequestFingerprint != record.Request.RequestFingerprint || repeated.OutputCount != record.OutputCount {
				t.Fatal("registered account replay changed its original runtime publication", found, err)
			}
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, true); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = storetest.SetSessionPublicationInsertFault(context.Background(), f.selected, false) })
			recoveryID := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, recoveryID, "retained registered customer input")
			waitRuntimeIncomingFailure(t, f, c, recoveryID)
			fixture, spool := openCaptureFixture(t, filepath.Join(c.state.directory.path, "provider.db"), f.operation.SessionAccount.ConnectionID)
			pending, err := spool.pendingPublications(f.ctx)
			if err != nil || len(pending) != 1 || pending[0].request == nil || pending[0].event.EventID != recoveryID {
				t.Fatal("registered publication failure lost exact retained request", len(pending), err)
			}
			frozen := bytes.Clone(pending[0].requestBytes)
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, false); err != nil {
				t.Fatal(err)
			}
			c = open()
			connectRuntimeConnectionFixture(t, f, c)
			for _, changed := range []*atomic.Bool{&changedRun, &changedSource} {
				changed.Store(true)
				before := releases.Load()
				if err := c.ReconcileIncoming(f.ctx); !errors.Is(err, errCaptureScopeChanged) || releases.Load() != before+1 {
					t.Fatal("registered recovery adopted changed source/run or leaked its use", err)
				}
				changed.Store(false)
			}
			before := releases.Load()
			if err := c.ReconcileIncoming(f.ctx); err != nil || releases.Load() != before+1 {
				t.Fatal("registered recovery failed to reacquire and release the exact original holder", err)
			}
			recovered, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, runtimeIncomingIdentityFixture(t, f, recoveryID))
			actual, encodeErr := publicationRequestBytes(recovered.Request)
			if err != nil || !found || encodeErr != nil || !bytes.Equal(actual, frozen) {
				t.Fatal("registered recovery lost or rewrote its retained publication", found, err, encodeErr)
			}
			if pending, err := c.captures.pending(f.ctx); err != nil || len(pending) != 0 {
				t.Fatal("registered recovery did not retire completed capture", len(pending), err)
			}
		})
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			device, err := c.state.device(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			selfID := uuid.NewString()
			self := encryptedMessageFromFixture(t, device, f.sender, device.ID.ToNonAD(),
				&waE2E.Message{Conversation: proto.String("paired phone echo, not customer input")})
			self.Attrs["id"] = selfID
			f.peer.mu.Lock()
			socket := f.peer.peers[len(f.peer.peers)-1]
			f.peer.mu.Unlock()
			if err := socket.send(f.peer.ctx, self); err != nil {
				t.Fatal(err)
			}
			waitRuntimeIncomingReceipt(t, f, c, selfID)
			if err := c.CheckBootstrap(f.ctx); err != nil {
				t.Fatal("paired phone echo fenced its own session", err)
			}
			if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, runtimeIncomingIdentityFixture(t, f, selfID)); err != nil || found {
				t.Fatal("paired phone echo became customer business input", found, err)
			}
			id := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, id, "installed native business text")
			waitRuntimeIncomingReceipt(t, f, c, id)
			identity := runtimeIncomingIdentityFixture(t, f, id)
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(context.Background(), identity)
			if err != nil || !found || record.OutputCount != 2 {
				t.Fatal("SDK acknowledgment preceded complete native publication", found, err)
			}
			original, err := publicationCaptureProvenance(record.Request)
			if err != nil || original.Scope != f.scope || original.OccurrenceID != c.state.currentOccurrence().occurrenceID {
				t.Fatal("runtime incoming publication changed its original admitted scope", err)
			}
			var payload map[string]any
			if err := json.Unmarshal(record.Events[1].Event.Payload(), &payload); err != nil || payload["text"] != "installed native business text" {
				t.Fatal("runtime incoming publication changed SDK text", payload, err)
			}
			rows, err := c.captures.pending(f.ctx)
			if err != nil || len(rows) != 0 {
				t.Fatal("runtime publication retained completed capture", len(rows), err)
			}
			assertRuntimeIncomingSetupReceipt(t, f)
			spoofed := incomingMessageFixture()
			spoofed.Info.IsFromMe = true
			guard := c.state.currentOccurrence().callbacks
			if guard.receive(spoofed) || !errors.Is(guard.currentFailure(), errIncomingMalformed) || c.CheckBootstrap(f.ctx) == nil {
				t.Fatal("a foreign sender bypassed refusal by claiming a self echo")
			}
		})
	}
}

func TestWhatsAppRuntimeIncomingOperatorIntentAckAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			id, at := uuid.NewString(), time.Now()
			content := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("Open inbox"),
				ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("summary-delivery"), QuotedMessage: &waE2E.Message{Conversation: proto.String("summary")}}}}
			sendRuntimeIncomingContentFixture(t, f, c, id, content, at)
			waitRuntimeIncomingReceipt(t, f, c, id)
			identity := runtimeIncomingIdentityFixture(t, f, id)
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
			if err != nil || !found || record.OutputCount != 0 || len(record.Events) != 0 {
				t.Fatal("SDK operator acknowledgment preceded its zero-business-event commit", found, record, err)
			}
			if pending, err := c.captures.pending(f.ctx); err != nil || len(pending) != 0 {
				t.Fatal("committed operator capture was not retired through its receipt", pending, err)
			}
			if err := c.Close(f.ctx); err != nil {
				t.Fatal(err)
			}

			f.peer = newSDKPeer(t)
			reopened := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, reopened)
			sendRuntimeIncomingContentFixture(t, f, reopened, id, content, at)
			waitRuntimeIncomingReceipt(t, f, reopened, id)
			repeated, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
			if err != nil || !found || repeated.Request.RequestFingerprint != record.Request.RequestFingerprint ||
				repeated.OutputCount != 0 {
				t.Fatal("redelivery adopted the successor sequence or changed operator evidence", repeated, err)
			}
			texts, err := f.selected.(channeldelivery.Store).ListPendingChannelTexts(f.ctx, "", 10)
			if err != nil || len(texts) != 1 || texts[0].Fact.ReplyToReference != "summary-delivery" || texts[0].Fact.Text != "Open inbox" {
				t.Fatal("restart duplicated or changed the operator intent", texts, err)
			}
			if pending, err := reopened.captures.pending(f.ctx); err != nil || len(pending) != 0 {
				t.Fatal("historical operator redelivery did not reconcile before staging", pending, err)
			}
		})
	}
}

func assertRuntimeIncomingSetupReceipt(t *testing.T, f *activeInputFixture) {
	t.Helper()
	receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(f.ctx, f.claim.PublicationID)
	fingerprint, fingerprintErr := f.claimEvent.PublicationFingerprint()
	if err != nil || !found || !receipt.Matches(f.claim) || fingerprintErr != nil || receipt.NativeCaptureFingerprint != fingerprint {
		t.Fatal("incoming reconciliation lost historical claim evidence", found, err, fingerprintErr)
	}
}

func runtimeIncomingIdentityFixture(t *testing.T, f *activeInputFixture, id string) inboundpublication.Identity {
	t.Helper()
	event := capturedEvent{Scope: f.scope, Source: f.source, OccurrenceID: uuid.NewString(),
		Conversation: types.NewJID("100000000003", types.DefaultUserServer).String(), EventID: id,
		Kind: "message", Body: []byte(`{}`), ReceivedAt: time.Now().UTC().Truncate(time.Microsecond)}
	event.Source.DeploymentRegistration = event.Scope.DeploymentRegistration
	identity, err := event.PublicationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func openRuntimeIncomingFixture(t *testing.T, f *activeInputFixture) *RuntimeConnection {
	t.Helper()
	if err := f.state.close(f.ctx); err != nil {
		t.Fatal(err)
	}
	c, err := OpenRuntimeConnection(f.ctx, RuntimeConnectionOptions{Directory: f.basePath,
		Store: f.selected, OperationID: f.operation.OperationID, Plan: f.channel,
		Incoming: &RuntimeIncomingOptions{Alias: "whatsapp", Trigger: f.trigger, Bus: f.publicationBus(t), Posture: executionposture.Live}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}

func TestWhatsAppRuntimeIncomingRefusesIncompleteOwnersBeforePossession(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	f.activate(t)
	foreign := newActiveInputFixture(t, "sqlite")
	foreign.activate(t)
	for _, cell := range []string{"bus", "trigger", "alias", "posture", "selected_owners", "foreign_bus"} {
		t.Run(cell, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "must-not-open")
			incoming := RuntimeIncomingOptions{Alias: "whatsapp", Trigger: f.trigger, Bus: f.publicationBus(t), Posture: executionposture.Live}
			opts := RuntimeConnectionOptions{Directory: root, Store: f.selected, OperationID: f.operation.OperationID,
				Plan: f.channel, Incoming: &incoming}
			switch cell {
			case "bus":
				incoming.Bus = nil
			case "trigger":
				incoming.Trigger = providertriggers.InboundAdmissionPlan{}
			case "alias":
				incoming.Alias = "invalid/alias"
			case "posture":
				incoming.Posture = ""
			case "selected_owners":
				opts.Store = struct{ channelonboarding.Store }{f.selected}
			case "foreign_bus":
				incoming.Bus = foreign.publicationBus(t)
			}
			c, err := OpenRuntimeConnection(f.ctx, opts)
			if c != nil || err == nil {
				if c != nil {
					_ = c.Close(context.Background())
				}
				t.Fatal("incomplete incoming owners reached private possession", err)
			}
			if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid incoming installation created provider state", err)
			}
		})
	}
}

func waitRuntimeIncomingFailure(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id string) {
	t.Helper()
	guard := c.state.currentOccurrence().callbacks
	select {
	case <-guard.drained:
	case <-f.peer.ctx.Done():
		t.Fatal("invalid incoming authority did not fail closed")
	}
	if guard.currentFailure() == nil {
		t.Fatal("invalid incoming authority lost failure evidence")
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case frame := <-f.peer.protocol:
			if frame.node.Tag == "receipt" && frame.node.AttrGetter().String("id") == id {
				t.Fatal("failed incoming handoff acknowledged the provider message")
			}
		default:
			return
		}
	}
}

func TestWhatsAppRuntimeIncomingAuthorityFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []string{"onboarding", "unbind", "retired_activation", "runtime_fence"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				if cell != "onboarding" {
					f.activate(t)
				}
				c := openRuntimeIncomingFixture(t, f)
				observed := make(chan error, 1)
				if cell == "onboarding" {
					guard := c.state.currentOccurrence().callbacks
					handle := guard.handle
					guard.handle = func(ctx context.Context, raw any) error {
						err := handle(ctx, raw)
						if _, message := raw.(*events.Message); message {
							observed <- err
						}
						return err
					}
				}
				connectRuntimeConnectionFixture(t, f, c)
				now := time.Now().UTC().Truncate(time.Microsecond)
				var err error
				switch cell {
				case "unbind":
					_, _, err = f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), now)
				case "retired_activation":
					_, err = f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "incoming fence proof", Now: now})
				case "runtime_fence":
					err = f.workOwner.Fence()
				}
				if err != nil {
					t.Fatal(err)
				}
				id := uuid.NewString()
				sendRuntimeIncomingFixture(t, f, c, id, "refused business message")
				if cell == "onboarding" {
					select {
					case err := <-observed:
						if !runtimefailures.OnlyBranches(err, func(branch error) bool { return branch == errIncomingAdmissionPending }) {
							t.Fatal("authenticated unapproved input was not classified as pending", err)
						}
					case <-f.peer.ctx.Done():
						t.Fatal("pending incoming input was not observed")
					}
					if err := c.CheckBootstrap(f.ctx); err != nil || c.state.currentOccurrence().callbacks.currentFailure() != nil {
						t.Fatal("pending approval fenced its original SDK transport", err)
					}
				} else {
					waitRuntimeIncomingFailure(t, f, c, id)
				}
				fixture, spool := openCaptureFixture(t, filepath.Join(c.state.directory.path, "provider.db"), f.operation.SessionAccount.ConnectionID)
				defer fixture.Close()
				rows, err := spool.pending(context.Background())
				want := 1
				if cell == "onboarding" {
					want = 0
				}
				if err != nil || len(rows) != want {
					t.Fatal("invalid runtime authority was frozen as a business capture", len(rows), err)
				}
				if cell != "onboarding" {
					_, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(context.Background(), runtimeIncomingIdentityFixture(t, f, id))
					if err != nil || found {
						t.Fatal("invalid runtime authority persisted a business publication", found, err)
					}
				}
			})
		}
	}
}

func TestWhatsAppRuntimeIncomingRecoversOriginalRequestBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"captured", "staged", "committed"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				event, admitted := f.receive(t, "unfinished native message")
				var frozen []byte
				if phase != "captured" {
					h := f.businessHandoff(t)
					prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", h.bus, h.store, h.posture, f.channel)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
						t.Fatal(err)
					}
					frozen, err = publicationRequestBytes(prepared.command.Request)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "committed" {
						if result, err := prepared.commitAndDispatch(); err != nil || !result.Acknowledged {
							t.Fatal("original commit", err)
						}
					} else if err := h.bus.AbandonInboundDeliveryPlan(f.ctx, prepared.plan); err != nil {
						t.Fatal(err)
					}
				}
				admitted.Close()
				f.restartBusinessConnection(t)
				f.restartBusinessConnection(t)
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				if err := c.ReconcileIncoming(f.ctx); err != nil {
					t.Fatal("installed runtime did not resume its retained incoming work", err)
				}
				identity, _ := event.PublicationIdentity()
				record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
				if err != nil || !found || record.OutputCount != 2 {
					t.Fatal("recovery lost the complete original publication", found, err)
				}
				original, err := publicationCaptureProvenance(record.Request)
				if err != nil || !original.SameCapture(event) {
					t.Fatal("runtime recovery rewrote original capture authority", err)
				}
				if phase != "captured" {
					actual, err := publicationRequestBytes(record.Request)
					if err != nil || !bytes.Equal(actual, frozen) {
						t.Fatal("runtime recovery rewrote immutable request bytes", err)
					}
				}
				if err := c.ReconcileIncoming(f.ctx); err != nil {
					t.Fatal("repeated reconciliation", err)
				}
				rows, err := c.captures.pending(f.ctx)
				if err != nil || len(rows) != 0 {
					t.Fatal("reconciliation retained completed capture", len(rows), err)
				}
				assertRuntimeIncomingSetupReceipt(t, f)
			})
		}
	}
}

func TestWhatsAppRuntimeIncomingRollbackRetainsEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, true); err != nil {
				t.Fatal(err)
			}
			faultInstalled := true
			t.Cleanup(func() {
				if faultInstalled {
					if err := storetest.SetSessionPublicationInsertFault(context.Background(), f.selected, false); err != nil {
						t.Error(err)
					}
				}
			})
			id := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, id, "rollback retained message")
			waitRuntimeIncomingFailure(t, f, c, id)
			fixture, spool := openCaptureFixture(t, filepath.Join(c.state.directory.path, "provider.db"), f.operation.SessionAccount.ConnectionID)
			rows, err := spool.pendingPublications(context.Background())
			if err != nil || len(rows) != 1 || rows[0].event.EventID != id || rows[0].request == nil {
				t.Fatal("failed installed publication discarded capture/request evidence", len(rows), err)
			}
			frozen := bytes.Clone(rows[0].requestBytes)
			event := rows[0].event
			assertRuntimeIncomingSetupReceipt(t, f)
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, false); err != nil {
				t.Fatal(err)
			}
			faultInstalled = false
			f.restartBusinessConnection(t)
			next := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, next)
			if err := next.ReconcileIncoming(f.ctx); err != nil {
				t.Fatal("failed incoming publication was not resumable", err)
			}
			identity, _ := event.PublicationIdentity()
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
			actual, encodeErr := publicationRequestBytes(record.Request)
			if err != nil || !found || encodeErr != nil || !bytes.Equal(actual, frozen) {
				t.Fatal("rollback recovery changed original request", found, err, encodeErr)
			}
		})
	}
}
