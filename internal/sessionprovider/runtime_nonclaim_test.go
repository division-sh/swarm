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
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestWhatsAppOrdinarySetupTextCannotPoisonClaimRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			id := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, id, "hello")
			// Accept either current refusal or a future owned ignored receipt;
			// neither may create a human binding or prevent a valid claim.
		initial:
			for {
				select {
				case frame := <-f.peer.protocol:
					if frame.node.Tag == "receipt" && frame.node.AttrGetter().String("id") == id {
						break initial
					}
				case <-c.state.currentOccurrence().callbacks.drained:
					break initial
				case <-f.peer.ctx.Done():
					t.Fatal("ordinary input has no completed disposition")
				}
			}
			bindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
			if err != nil || len(bindings) != 0 {
				t.Fatal("ordinary input created authority", bindings, err)
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			// A fresh genuine occurrence must not stay poisoned by ordinary text.
			c = openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			if err := c.ReconcileIncoming(f.ctx); err != nil {
				t.Errorf("ordinary setup text poisons fresh occurrence recovery: %v", err)
			}
			validID, at := uuid.NewString(), time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, validID, identity.Challenge, at)
			waitRuntimeIncomingReceipt(t, f, c, validID)
			event := runtimeClaimEventFixture(t, f, c, validID, identity.Challenge, at)
			providerID, err := event.PublicationProviderEventID()
			if err != nil {
				t.Fatal(err)
			}
			receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(f.ctx, operatorchannel.SessionClaimReceiptID(f.operation.OperationID, providerID))
			if err != nil || !found || receipt.OperationID != identity.OperationID {
				t.Errorf("ordinary text blocked later valid claim receipt: found=%v err=%v", found, err)
			}
		})
	}
}

type nonClaimObservationFailureStore struct {
	channelonboarding.Store
	failure error
}

func (s *nonClaimObservationFailureStore) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	if s.failure != nil {
		return channelonboarding.Operation{}, s.failure
	}
	return s.Store.GetChannelOnboarding(ctx, id)
}

func TestWhatsAppNonClaimDispositionPreservesObservationErrorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			beginRuntimeClaimFixture(t, f)
			observer := &nonClaimObservationFailureStore{Store: f.selected}
			f.owner.store = observer
			event, admitted := f.receive(t, "hello")
			prepared, err := prepareSessionSetup(f.ctx, admitted, f.trigger, f.channel)
			if err != nil {
				t.Fatal(err)
			}
			observer.failure = errors.New("selected observation fixture failure")
			if err := f.spool.settleNonClaim(f.ctx, prepared.nonClaim); !errors.Is(err, observer.failure) {
				t.Fatal("disposition swallowed selected observation failure", err)
			}
			pending, err := f.spool.pending(f.ctx)
			if err != nil || len(pending) != 1 || !pending[0].SameCapture(event) {
				t.Fatal("observation failure lost original input", len(pending), err)
			}
		})
	}
}

func TestWhatsAppNonClaimReceiptsDoNotConsumePendingQuota(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	beginRuntimeClaimFixture(t, f)
	event, admitted := f.receive(t, "hello")
	prepared, err := prepareSessionSetup(f.ctx, admitted, f.trigger, f.channel)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.spool.settleNonClaim(f.ctx, prepared.nonClaim); err != nil {
		t.Fatal(err)
	}
	admitted.Close()
	if err := f.state.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture, spool := openCaptureFixture(t, filepath.Join(f.state.directory.path, "provider.db"), f.operation.SessionConnectionID)
	defer fixture.Close()
	if err := fixture.SeedCaptureQuota(f.ctx, event, maxPendingCaptureCount-1); err != nil {
		t.Fatal(err)
	}
	newEvent := event
	newEvent.EventID = "last-pending-slot"
	if err := spool.capture(f.ctx, newEvent); err != nil {
		t.Fatal("historical non-claim consumed pending capacity", err)
	}
	newEvent.EventID = "over-pending-limit"
	if err := spool.capture(f.ctx, newEvent); !errors.Is(err, errCaptureCapacity) {
		t.Fatal("non-claim disposition weakened pending quota", err)
	}
	if receipts, err := spool.nonClaimReceipts(f.ctx); err != nil || len(receipts) != 1 || !receipts[0].SameCapture(event) {
		t.Fatal("quota accounting erased historical receipt", len(receipts), err)
	}
}

func TestWhatsAppSetupMappingRejectsAbsentAndAmbiguousText(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	beginRuntimeClaimFixture(t, f)
	_, admitted := f.receive(t, "hello")
	request, err := f.trigger.AdmitSessionInput(f.ctx, admitted)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := f.trigger.ProjectDelivery(request)
	if err != nil || len(delivery.Events) != 2 {
		t.Fatal("fixture normalization", err)
	}
	for _, shape := range []string{"missing", "ambiguous"} {
		t.Run(shape, func(t *testing.T) {
			probe := delivery
			probe.Events = append([]providertriggers.DeliveryEvent(nil), delivery.Events...)
			if shape == "missing" {
				probe.Events = probe.Events[:1]
			} else {
				probe.Events = append(probe.Events, delivery.Events[1])
			}
			if _, _, err := projectSessionSetupText(probe, f.channel, "message"); err == nil {
				t.Fatal("invalid mapping became an expected non-claim disposition")
			}
		})
	}
}

func TestWhatsAppNonClaimShapeClassificationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"prefix_text", "punctuation", "edit", "revoke"} {
			t.Run(backend+"/"+shape, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				identity := beginRuntimeClaimFixture(t, f)
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				content := &waE2E.Message{Conversation: proto.String("note " + identity.Challenge)}
				if shape == "punctuation" {
					content.Conversation = proto.String(identity.Challenge + ".")
				}
				client := whatsmeow.NewClient(f.sender, nil)
				self, destination := types.NewJID("100000000003", types.DefaultUserServer), *f.sender.ID
				if shape == "edit" {
					content = client.BuildEdit(destination, "ORIGINAL", &waE2E.Message{Conversation: proto.String(identity.Challenge)})
				} else if shape == "revoke" {
					content = client.BuildRevoke(destination, self, "ORIGINAL")
				}
				id := uuid.NewString()
				sendRuntimeIncomingContentFixture(t, f, c, id, content, time.Now())
				waitRuntimeIncomingReceipt(t, f, c, id)
				receipt := assertNonClaimReceiptFixture(t, f, c, id)
				if (shape == "edit" || shape == "revoke") && receipt.Kind != shape {
					t.Fatal("SDK edit/revoke shape lost", receipt.Kind)
				}
				validID, at := uuid.NewString(), time.Now()
				sendRuntimeIncomingAtFixture(t, f, c, validID, identity.Challenge, at)
				waitRuntimeIncomingReceipt(t, f, c, validID)
				assertRuntimeClaimReceipt(t, f, identity, runtimeClaimEventFixture(t, f, c, validID, identity.Challenge, at))
			})
		}
	}
}

func TestWhatsAppNonClaimDispositionRefusesMissingAndStaleAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			beginRuntimeClaimFixture(t, f)
			event, admitted := f.receive(t, "hello")
			prepared, err := prepareSessionSetup(f.ctx, admitted, f.trigger, f.channel)
			if err != nil || prepared.nonClaim.Empty() {
				t.Fatal("ordinary admitted text did not classify as non-claim", err)
			}
			if err := f.spool.settleNonClaim(f.ctx, authority.NonClaim{}); err == nil {
				t.Fatal("empty DTO disposed an arbitrary capture")
			}
			_, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase, Now: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.spool.settleNonClaim(f.ctx, prepared.nonClaim); err == nil {
				t.Fatal("stale native projection disposed input")
			}
			admitted.Close()
			if err := f.spool.settleNonClaim(f.ctx, prepared.nonClaim); err == nil {
				t.Fatal("released projection retained disposition authority")
			}
			pending, err := f.spool.pending(f.ctx)
			if err != nil || len(pending) != 1 || !pending[0].SameCapture(event) {
				t.Fatal("invalid disposition lost original evidence", len(pending), err)
			}
			if receipts, err := f.spool.nonClaimReceipts(f.ctx); err != nil || len(receipts) != 0 {
				t.Fatal("invalid disposition wrote a receipt", len(receipts), err)
			}
		})
	}
}

func TestWhatsAppNonClaimReceiptCorruptionRefusesRecoveryAndAcknowledgementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			id := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, id, "hello")
			waitRuntimeIncomingReceipt(t, f, c, id)
			assertNonClaimReceiptFixture(t, f, c, id)
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			fixture, _ := openCaptureFixture(t, filepath.Join(c.state.directory.path, "provider.db"), f.operation.SessionConnectionID)
			if err := fixture.CorruptCaptureIndex(f.ctx, sessionstate.CorruptSetupDisposition); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			c = openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			if err := c.ReconcileIncoming(f.ctx); err == nil {
				t.Fatal("corrupt non-claim history was silently suppressed")
			}
			validID := uuid.NewString()
			sendRuntimeIncomingFixture(t, f, c, validID, identity.Challenge)
			waitRuntimeIncomingFailure(t, f, c, validID)
		})
	}
}

func TestWhatsAppNonClaimSetupProgressAndRedeliveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []string{"same_occurrence", "reopen", "confirmation_revision"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				identity := beginRuntimeClaimFixture(t, f)
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				id, at := uuid.NewString(), time.Now()
				sendRuntimeIncomingAtFixture(t, f, c, id, "hello", at)
				waitRuntimeIncomingReceipt(t, f, c, id)
				original := assertNonClaimReceiptFixture(t, f, c, id)
				if cell == "reopen" {
					if err := c.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					c = openRuntimeIncomingFixture(t, f)
					connectRuntimeConnectionFixture(t, f, c)
					if err := c.ReconcileIncoming(f.ctx); err != nil {
						t.Fatal("non-claim recovery prevented later setup", err)
					}
				}
				for n := 0; n < 2; n++ {
					sendRuntimeIncomingAtFixture(t, f, c, id, "hello", at)
					waitRuntimeIncomingReceipt(t, f, c, id)
					if receipt := assertNonClaimReceiptFixture(t, f, c, id); !receipt.SameCapture(original) {
						t.Fatal("redelivery rewrote non-claim evidence")
					}
				}
				validID, validAt := uuid.NewString(), time.Now()
				sendRuntimeIncomingAtFixture(t, f, c, validID, identity.Challenge, validAt)
				waitRuntimeIncomingReceipt(t, f, c, validID)
				assertRuntimeClaimReceipt(t, f, identity, runtimeClaimEventFixture(t, f, c, validID, identity.Challenge, validAt))
				if cell == "confirmation_revision" {
					var err error
					f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
						OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision,
						Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, Now: time.Now()})
					if err != nil {
						t.Fatal(err)
					}
					sendRuntimeIncomingAtFixture(t, f, c, id, "hello", at)
					waitRuntimeIncomingReceipt(t, f, c, id)
					if receipt := assertNonClaimReceiptFixture(t, f, c, id); !receipt.SameCapture(original) {
						t.Fatal("later revision adopted the original non-claim input")
					}
				}
			})
		}
	}
}

func assertNonClaimReceiptFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id string) capturedEvent {
	t.Helper()
	rows, err := c.captures.nonClaimReceipts(f.ctx)
	if err != nil || len(rows) != 1 || rows[0].EventID != id {
		t.Fatal("SDK receipt preceded exact non-claim disposition", len(rows), err)
	}
	pending, err := c.captures.pending(f.ctx)
	if err != nil || len(pending) != 0 {
		t.Fatal("disposed setup input still blocks pending recovery", len(pending), err)
	}
	bindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
	if err != nil || len(bindings) != 0 {
		t.Fatal("non-claim disposition manufactured operator authority", bindings, err)
	}
	return rows[0]
}

func TestWhatsAppNonClaimDispositionRollbackAndInterruptedRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(c.state.directory.path, "provider.db")
			fixture, _ := openCaptureFixture(t, path, f.operation.SessionConnectionID)
			if err := fixture.SetCaptureFault(f.ctx, sessionstate.CaptureStageFault, true); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			c = openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			id, at := uuid.NewString(), time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, id, "hello", at)
			waitRuntimeIncomingFailure(t, f, c, id)
			fixture, spool := openCaptureFixture(t, path, f.operation.SessionConnectionID)
			rows, err := spool.pending(f.ctx)
			if err != nil || len(rows) != 1 || rows[0].EventID != id {
				t.Fatal("disposition rollback lost its pending original input", len(rows), err)
			}
			original := rows[0]
			if receipts, err := spool.nonClaimReceipts(f.ctx); err != nil || len(receipts) != 0 {
				t.Fatal("failed disposition became a receipt", len(receipts), err)
			}
			if err := fixture.SetCaptureFault(f.ctx, sessionstate.CaptureStageFault, false); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			c = openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			if err := c.ReconcileIncoming(f.ctx); err != nil {
				t.Fatal("interrupted disposition could not reconcile original input", err)
			}
			if receipt := assertNonClaimReceiptFixture(t, f, c, id); !receipt.SameCapture(original) {
				t.Fatal("disposition recovery rewrote captured evidence")
			}
			sendRuntimeIncomingAtFixture(t, f, c, id, "hello", at)
			waitRuntimeIncomingReceipt(t, f, c, id)
			validID, validAt := uuid.NewString(), time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, validID, identity.Challenge, validAt)
			waitRuntimeIncomingReceipt(t, f, c, validID)
			assertRuntimeClaimReceipt(t, f, identity, runtimeClaimEventFixture(t, f, c, validID, identity.Challenge, validAt))
		})
	}
}
