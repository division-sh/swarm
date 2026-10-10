//go:build linux || darwin

package sessionprovider

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sessioncapture"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestReviewerOperatorReceiptBoundariesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			eventBus := f.publicationBus(t)
			event := f.receiveCaptureMessageSDK(t, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("Open inbox"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("original-delivery"),
					QuotedMessage: &waE2E.Message{Conversation: proto.String("original summary")}}}}, uuid.NewString(), time.Now())
			input, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
				OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			prepared, err := prepareSessionBusinessPublication(f.ctx, input, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
			if err != nil || prepared.command.OperatorChannelText == nil {
				t.Fatal("positive preparation", err)
			}
			for _, name := range []string{"request", "conversation", "entry", "reply", "missing_source", "missing_admission"} {
				t.Run(name, func(t *testing.T) {
					command := prepared.command
					text := *command.OperatorChannelText
					command.OperatorChannelText = &text
					ctx := prepared.ctx
					switch name {
					case "request":
						command.Request.OriginalUserAgent = "changed"
					case "conversation":
						text.ConversationRef = "foreign@s.whatsapp.net"
					case "entry":
						text.EntryReference = "foreign"
					case "reply":
						text.ReplyToReference = "foreign"
					case "missing_source":
						ctx = context.Background()
					case "missing_admission":
						command.Admission = providertriggers.PublicationAdmission{}
					}
					result, err := f.selected.(sessionBusinessStore).CommitInboundPublication(ctx, command)
					if err == nil || result.Acknowledged {
						t.Fatal("mutated operator receipt accepted", result, err)
					}
				})
			}
			if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
				t.Fatal("stage original request as the real handoff does", err)
			}
			signed, err := f.trigger.AdmitSessionInput(prepared.ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			delivery, admission, err := f.trigger.ProjectPublication(signed, f.operation.Coordinate.BundleHash, prepared.command.Request.FlowPath)
			if err != nil {
				t.Fatal(err)
			}
			// Reproject the same authenticated output under a caller-chosen receipt
			// identity, before the new projection receipt freezes that request.
			request := prepared.command.Request
			request.ProviderEventID = uuid.NewString()
			request.PublicationID, request.MarkerEventID, err = inbound.DeterministicIDs(request.Identity())
			if err != nil {
				t.Fatal(err)
			}
			if err := sessioncapture.ValidateCapturePublication(event, request); err == nil {
				t.Fatal("counterexample did not contradict the canonical capture identity")
			}
			var projection *inbound.OperatorProjection
			for _, output := range delivery.Events {
				projection, err = inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel}, nil, projection)
				if err != nil {
					break
				}
			}
			if err == nil {
				command, buildErr := sessionOperatorCommand(prepared.ctx, eventBus, admission, request, projection, executionposture.Live)
				if buildErr == nil {
					result, commitErr := f.selected.(sessionBusinessStore).CommitInboundPublication(prepared.ctx, command)
					if commitErr == nil || result.Acknowledged {
						t.Errorf("authenticated capture minted caller-selected publication identity: created=%t acknowledged=%t err=%v", result.Record.Created, result.Acknowledged, commitErr)
					}
				}
			}
			if result, err := prepared.commitAndDispatch(); err != nil || !result.Acknowledged {
				t.Fatal("original positive commit", result, err)
			}
			texts, err := f.selected.(channeldelivery.Store).ListPendingChannelTexts(f.ctx, "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(texts) != 1 {
				t.Errorf("one genuine input created %d pending operator intents", len(texts))
			}
		})
	}
}

func TestReviewerBusinessReceiptIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			eventBus := f.publicationBus(t)
			event, input := f.receive(t, "ordinary business content")
			defer input.Close()
			original, err := prepareSessionBusinessPublication(f.ctx, input, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), original.plan) })
			signed, err := f.trigger.AdmitSessionInput(original.ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			delivery, admission, err := f.trigger.ProjectPublication(signed, f.operation.Coordinate.BundleHash, original.command.Request.FlowPath)
			if err != nil {
				t.Fatal(err)
			}
			request := original.command.Request
			request.ProviderEventID = uuid.NewString()
			request.PublicationID, request.MarkerEventID, err = inbound.DeterministicIDs(request.Identity())
			if err != nil {
				t.Fatal(err)
			}
			if err := sessioncapture.ValidateCapturePublication(event, request); err == nil {
				t.Fatal("invalid counterexample")
			}
			batch := bus.InboundDeliveryBatch{Provider: request.Provider, Admission: admission}
			projection := authoractivity.InboundProjection{}
			for ordinal, output := range delivery.Events {
				item, err := inbound.ProjectOutputEvent(request, ordinal, output, executionposture.Live)
				if err != nil {
					return
				}
				batch.Events = append(batch.Events, item)
				if output.Kind == providertriggers.OutputKindNormalized {
					projection = authoractivity.InboundProjection{SubjectType: output.AuthorSubjectType, SubjectID: output.AuthorSubjectID}
				}
			}
			batch.AuthorSubjectType, batch.AuthorSubjectID = projection.SubjectType, projection.SubjectID
			plan, err := eventBus.PrepareInboundDeliveryBatch(original.ctx, batch)
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), plan) })
			command, err := sessionBusinessCommand(original.ctx, eventBus, plan, request, projection, executionposture.Live)
			if err != nil {
				return
			}
			result, err := f.selected.(sessionBusinessStore).CommitInboundPublication(original.ctx, command)
			if err == nil || result.Acknowledged {
				t.Errorf("business sibling accepted a caller-selected capture receipt: created=%t acknowledged=%t err=%v", result.Record.Created, result.Acknowledged, err)
			}
		})
	}
}
