//go:build linux || darwin

package sessionprovider

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestWhatsAppOperatorCommitNativeAuthorityMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutation := range []string{"current", "release", "cancel", "unbind", "retire_activation", "reconstructed", "missing_admission", "changed_text", "changed_sender"} {
			t.Run(backend+"/"+mutation, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				eventBus := f.publicationBus(t)
				event := f.receiveCaptureMessageSDK(t, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: proto.String("Open inbox"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("original-delivery"),
						QuotedMessage: &waE2E.Message{Conversation: proto.String("original summary")}}}}, uuid.NewString(), time.Now())
				admitted, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
					OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
				if err != nil {
					t.Fatal(err)
				}
				defer admitted.Close()
				prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus,
					f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
				if err != nil || prepared.command.OperatorChannelText == nil || len(prepared.command.Finalization.Events) != 0 || len(prepared.command.Publications) != 0 {
					t.Fatal("native operator reply was not a zero-business-event command", prepared.command, err)
				}
				command := prepared.command
				ctx, stop := context.WithCancel(prepared.ctx)
				defer stop()
				now := time.Now().UTC().Truncate(time.Microsecond)
				switch mutation {
				case "release":
					admitted.Close()
				case "cancel":
					stop()
				case "unbind":
					_, _, err = f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), now)
				case "retire_activation":
					_, err = f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "native operator proof", Now: now})
				case "reconstructed":
					command = inbound.CommitCommand{Admission: command.Admission, Request: command.Request, Finalization: command.Finalization,
						OperatorChannelText: command.OperatorChannelText}
				case "missing_admission":
					command.Admission = providertriggers.PublicationAdmission{}
				case "changed_text":
					changed := *command.OperatorChannelText
					changed.Text = "Retire"
					command.OperatorChannelText = &changed
				case "changed_sender":
					changed := *command.OperatorChannelText
					changed.ExternalAccountRef = "foreign@s.whatsapp.net"
					command.OperatorChannelText = &changed
				}
				if err != nil {
					t.Fatal(err)
				}
				result, err := f.selected.(sessionBusinessStore).CommitInboundPublication(ctx, command)
				identity, identityErr := event.PublicationIdentity()
				if identityErr != nil {
					t.Fatal(identityErr)
				}
				_, found, lookupErr := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
				if mutation != "current" {
					if err == nil || result.Acknowledged || found || lookupErr != nil {
						t.Fatal("stale/reconstructed operator input persisted", result, found, err, lookupErr)
					}
					return
				}
				if err != nil || !result.Acknowledged || !found || !result.Record.Created || result.Record.OutputCount != 0 ||
					len(result.Record.Events) != 0 || len(result.Publications) != 0 {
					t.Fatal("owned native operator intent did not commit atomically", result, found, err, lookupErr)
				}
				texts, err := f.selected.(channeldelivery.Store).ListPendingChannelTexts(f.ctx, "", 10)
				if err != nil || len(texts) != 1 || texts[0].Fact.Text != "Open inbox" || texts[0].Fact.ReplyToReference != "original-delivery" {
					t.Fatal("operator intent lost its current text or genuine reply", texts, err)
				}
				duplicate, err := prepared.commitAndDispatch()
				if err != nil || !duplicate.Acknowledged || duplicate.Record.Created || duplicate.Record.OutputCount != 0 {
					t.Fatal("operator replay repeated business execution", duplicate, err)
				}
			})
		}
	}
}
