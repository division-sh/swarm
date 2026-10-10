//go:build linux || darwin

package sessionprovider

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestWhatsAppNativeInputConsumesSharedOperatorProjectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			f.publicationBus(t)
			for _, row := range []struct {
				name    string
				content *waE2E.Message
				want    string
			}{
				{"ordinary", &waE2E.Message{Conversation: proto.String("hello")}, "bare"},
				{"challenge", &waE2E.Message{Conversation: proto.String(f.claim.Challenge)}, "claim"},
				{"genuine_reply", &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: proto.String("Open inbox"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("original-delivery"),
						QuotedMessage: &waE2E.Message{Conversation: proto.String("quoted text is not an action")}}}}, "text"},
				{"forward", &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: proto.String("hello"), ContextInfo: &waE2E.ContextInfo{IsForwarded: proto.Bool(true)}}}, "bare"},
			} {
				t.Run(row.name, func(t *testing.T) {
					event := f.receiveCaptureMessageSDK(t, row.content, uuid.NewString(), time.Now())
					admitted, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
						OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
					if err != nil {
						t.Fatal(err)
					}
					defer admitted.Close()
					signed, err := f.trigger.AdmitSessionInput(f.ctx, admitted)
					if err != nil {
						t.Fatal(err)
					}
					request, err := sessionBusinessRequest(f.ctx, event, "whatsapp", f.selected.(sessionBusinessStore), admitted)
					if err != nil {
						t.Fatal(err)
					}
					delivery, _, err := f.trigger.ProjectPublication(signed, f.operation.Coordinate.BundleHash, request.FlowPath)
					if err != nil {
						t.Fatal(err)
					}
					var projected *inbound.OperatorProjection
					for _, output := range delivery.Events {
						projected, err = inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel}, nil, projected)
						if err != nil {
							t.Fatal(err)
						}
					}
					if projected == nil || (projected.BareCandidate != nil) != (row.want == "bare") ||
						(projected.Claim != nil) != (row.want == "claim") || (projected.Text != nil) != (row.want == "text") || projected.Action != nil {
						t.Fatal("native input diverged from neutral operator classification", projected)
					}
					if projected.Text != nil && (projected.Text.ReplyToReference != "original-delivery" || projected.Text.Text != "Open inbox" ||
						projected.Text.ExternalAccountRef != f.claim.ExternalAccountRef) {
						t.Fatal("reply projection adopted quoted text or another sender", projected.Text)
					}
					output := delivery.Events[len(delivery.Events)-1]
					if _, err := inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel, f.channel}, nil, nil); err == nil {
						t.Fatal("duplicate interfaces did not fail closed")
					}
					if row.want == "bare" {
						cause := errors.New("input draft observation failed")
						if _, err := inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel},
							func(operatorchannel.InboundText) (bool, error) { return false, cause }, nil); !errors.Is(err, cause) {
							t.Fatal("draft observation error became business input", err)
						}
						selected, err := inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel},
							func(operatorchannel.InboundText) (bool, error) { return true, nil }, nil)
						if err != nil || selected.Text == nil || selected.BareCandidate != nil {
							t.Fatal("current draft classification did not use the existing selector", selected, err)
						}
					}
					identity, err := event.PublicationIdentity()
					if err != nil {
						t.Fatal(err)
					}
					if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity); err != nil || found {
						t.Fatal("pure classification created publication or operator authority", found, err)
					}
				})
			}
		})
	}
}
