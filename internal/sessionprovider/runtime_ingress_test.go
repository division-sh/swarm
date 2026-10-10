//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
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
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
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
		for _, cell := range []string{"onboarding", "unbind", "retired_activation", "suspended_run", "runtime_fence"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				if cell != "onboarding" {
					f.activate(t)
				}
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				now := time.Now().UTC().Truncate(time.Microsecond)
				var err error
				switch cell {
				case "unbind":
					_, _, err = f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), now)
				case "retired_activation":
					_, err = f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "incoming fence proof", Now: now})
				case "suspended_run":
					_, err = f.selected.SuspendStandingService(f.ctx, pipeline.StandingServiceOperation{ServiceID: f.standing.ServiceID,
						Actor: "test", Reason: "incoming fence proof", ExecutionPosture: executionposture.Live, Expected: &f.standing})
				case "runtime_fence":
					err = f.workOwner.Fence()
				}
				if err != nil {
					t.Fatal(err)
				}
				id := uuid.NewString()
				sendRuntimeIncomingFixture(t, f, c, id, "refused business message")
				waitRuntimeIncomingFailure(t, f, c, id)
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
					prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", h.bus, h.store, h.posture)
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
