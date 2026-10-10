//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
)

func beginRuntimeClaimFixture(t *testing.T, f *activeInputFixture) operatorchannel.Operation {
	t.Helper()
	identity, err := f.identities.Begin(f.ctx, f.operation.Interface.Selector, operatorchannel.OperationConnect, 0,
		uuid.NewString(), uuid.NewString(), f.operation.OperationID,
		operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}, false, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase,
		IdentityOperationID: identity.OperationID, Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	f.setScope(channelonboarding.SessionInputOnboarding)
	return identity
}

func assertRuntimeClaimReceipt(t *testing.T, f *activeInputFixture, identity operatorchannel.Operation, event capturedEvent) {
	t.Helper()
	providerID, err := event.PublicationProviderEventID()
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(f.ctx, operatorchannel.SessionClaimReceiptID(f.operation.OperationID, providerID))
	fingerprint, fingerprintErr := event.PublicationFingerprint()
	if err != nil || !found || fingerprintErr != nil || receipt.NativeCaptureFingerprint != fingerprint ||
		receipt.OperationID != identity.OperationID || receipt.Challenge != identity.Challenge || receipt.Disposition != operatorchannel.DispositionConsumedBinding {
		t.Fatal("SDK claim acknowledgment lacks exact durable receipt", receipt, found, err, fingerprintErr)
	}
	bindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
	if err != nil || len(bindings) != 0 {
		t.Fatal("claim receipt substituted for human confirmation", bindings, err)
	}
	if _, err := f.selected.GetConnectedChannelActivation(f.ctx, f.operation.SlotKey); !errors.Is(err, channelonboarding.ErrNotFound) {
		t.Fatal("claim receipt granted business activation", err)
	}
}

func TestWhatsAppRuntimeClaimSettlesBeforeAcknowledgementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			identity := beginRuntimeClaimFixture(t, f)
			c := openRuntimeIncomingFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			id := uuid.NewString()
			at := time.Now()
			sendRuntimeIncomingAtFixture(t, f, c, id, identity.Challenge, at)
			waitRuntimeIncomingReceipt(t, f, c, id)
			rows, err := c.captures.pending(f.ctx)
			if err != nil || len(rows) != 0 {
				t.Fatal("committed claim capture was not retired", len(rows), err)
			}
			event := runtimeClaimEventFixture(t, f, c, id, identity.Challenge, at)
			assertRuntimeClaimReceipt(t, f, identity, event)
			// Identical transport redelivery reconciles the existing receipt;
			// it does not reopen the identity operation or create a second claim.
			sendRuntimeIncomingAtFixture(t, f, c, id, identity.Challenge, at)
			waitRuntimeIncomingReceipt(t, f, c, id)
			assertRuntimeClaimReceipt(t, f, identity, event)
			if err := c.ReconcileIncoming(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runtimeClaimEventFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection, id, text string, at time.Time) capturedEvent {
	t.Helper()
	person := types.NewJID("100000000003", types.DefaultUserServer).String()
	body, err := json.Marshal(incomingPayload{Kind: "message", Conversation: person, ConversationScope: "direct",
		Sender: person, MessageReference: id, Text: &text, ProviderTimestampMS: at.Unix() * 1000})
	if err != nil {
		t.Fatal(err)
	}
	return capturedEvent{Scope: f.scope, Source: f.source, Body: body, Conversation: person, EventID: id,
		Kind: "message", OccurrenceID: c.state.currentOccurrence().occurrenceID,
		ReceivedAt: time.Now().UTC().Truncate(time.Microsecond)}
}

func TestWhatsAppRuntimeClaimRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"captured", "settled", "settled_stale_revision", "stale_revision", "stale_publication", "retired_operation"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				identity := beginRuntimeClaimFixture(t, f)
				event, admitted := f.receive(t, identity.Challenge)
				if phase == "settled" || phase == "settled_stale_revision" {
					prepared, err := prepareSessionSetup(f.ctx, admitted, f.trigger, f.channel)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.selected.SettleSessionChannelClaim(f.ctx, prepared.claim); err != nil {
						t.Fatal(err)
					}
				}
				admitted.Close()
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				if phase == "stale_revision" || phase == "settled_stale_revision" || phase == "stale_publication" || phase == "retired_operation" {
					advance := channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
						ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase, Now: time.Now()}
					if phase == "retired_operation" {
						advance.Phase = channelonboarding.PhaseFailed
						advance.FailureCode = "runtime_claim_retired"
					}
					if phase == "stale_publication" {
						coordinate := f.operation.Coordinate
						coordinate.ContextPublicationGeneration++
						advance.RebindCoordinate = &coordinate
					}
					var err error
					f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, advance)
					if err != nil {
						t.Fatal(err)
					}
				}
				err := c.ReconcileIncoming(f.ctx)
				rows, readErr := c.captures.pending(context.Background())
				if phase == "stale_revision" || phase == "stale_publication" || phase == "retired_operation" {
					if err == nil || readErr != nil || len(rows) != 1 || !rows[0].SameCapture(event) {
						t.Fatal("stale claim recovery adopted authority or discarded evidence", err, readErr, len(rows))
					}
					providerID, _ := event.PublicationProviderEventID()
					_, found, receiptErr := f.selected.LoadOperatorChannelClaimReceipt(f.ctx,
						operatorchannel.SessionClaimReceiptID(f.operation.OperationID, providerID))
					if receiptErr != nil || found {
						t.Fatal("stale claim recovery persisted a receipt", found, receiptErr)
					}
					return
				}
				if err != nil || readErr != nil || len(rows) != 0 {
					t.Fatal("claim capture remained stuck after connection restart", err, readErr, len(rows))
				}
				assertRuntimeClaimReceipt(t, f, identity, event)
				for n := 0; n < 2; n++ {
					if err := c.ReconcileIncoming(f.ctx); err != nil {
						t.Fatal("repeated claim reconciliation", err)
					}
				}
			})
		}
	}
}

func TestWhatsAppRuntimeClaimDoesNotAutoBindFirstSenderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []string{"ordinary_text", "unknown_challenge"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				beginRuntimeClaimFixture(t, f)
				c := openRuntimeIncomingFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				text := "ordinary first sender"
				if cell == "unknown_challenge" {
					var err error
					text, err = operatorchannel.NewChallenge()
					if err != nil {
						t.Fatal(err)
					}
				}
				id := uuid.NewString()
				at := time.Now()
				sendRuntimeIncomingAtFixture(t, f, c, id, text, at)
				if cell == "ordinary_text" {
					waitRuntimeIncomingReceipt(t, f, c, id)
					rows, err := c.captures.nonClaimReceipts(f.ctx)
					if err != nil || len(rows) != 1 || rows[0].EventID != id || rows[0].Scope.Kind != channelonboarding.SessionInputOnboarding {
						t.Fatal("non-claim first sender lost exact setup receipt", len(rows), err)
					}
				} else {
					waitRuntimeIncomingReceipt(t, f, c, id)
					event := runtimeClaimEventFixture(t, f, c, id, text, at)
					providerID, _ := event.PublicationProviderEventID()
					receipt, found, err := f.selected.LoadOperatorChannelClaimReceipt(f.ctx,
						operatorchannel.SessionClaimReceiptID(f.operation.OperationID, providerID))
					if err != nil || !found || receipt.OperationID != "" || receipt.Disposition == operatorchannel.DispositionConsumedBinding {
						t.Fatal("unknown challenge did not retain its ignored receipt", receipt, found, err)
					}
				}
				bindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
				if err != nil || len(bindings) != 0 {
					t.Fatal("first sender became the operator", bindings, err)
				}
			})
		}
	}
}
