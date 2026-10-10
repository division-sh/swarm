//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	"github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func reprojectReceiptCommand(t *testing.T, ctx context.Context, f *activeInputFixture, eventBus *bus.EventBus,
	admitted input.Admission, request inbound.Request, operator bool,
) (inbound.CommitCommand, func(), error) {
	t.Helper()
	cleanup := func() {}
	signed, err := f.trigger.AdmitSessionInput(ctx, admitted)
	if err != nil {
		return inbound.CommitCommand{}, cleanup, err
	}
	delivery, admission, err := f.trigger.ProjectPublication(signed, f.operation.Coordinate.BundleHash, request.FlowPath)
	if err != nil {
		return inbound.CommitCommand{}, cleanup, err
	}
	if operator {
		var projection *inbound.OperatorProjection
		for _, output := range delivery.Events {
			projection, err = inbound.ProjectOperatorOutput(output, request, []packs.SatisfactionPlan{f.channel}, nil, projection)
			if err != nil {
				return inbound.CommitCommand{}, cleanup, err
			}
		}
		command, err := sessionOperatorCommand(ctx, eventBus, admission, request, projection, executionposture.Live)
		return command, cleanup, err
	}
	batch := bus.InboundDeliveryBatch{Provider: request.Provider, Admission: admission}
	projection := authoractivity.InboundProjection{}
	for ordinal, output := range delivery.Events {
		item, err := inbound.ProjectOutputEvent(request, ordinal, output, executionposture.Live)
		if err != nil {
			return inbound.CommitCommand{}, cleanup, err
		}
		batch.Events = append(batch.Events, item)
		if output.Kind == providertriggers.OutputKindNormalized {
			projection = authoractivity.InboundProjection{SubjectType: output.AuthorSubjectType, SubjectID: output.AuthorSubjectID}
		}
	}
	batch.AuthorSubjectType, batch.AuthorSubjectID = projection.SubjectType, projection.SubjectID
	plan, err := eventBus.PrepareInboundDeliveryBatch(ctx, batch)
	if err != nil {
		return inbound.CommitCommand{}, cleanup, err
	}
	cleanup = func() {
		if err := eventBus.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), plan); err != nil {
			t.Error("release adverse receipt preparation", err)
		}
	}
	command, err := sessionBusinessCommand(ctx, eventBus, plan, request, projection, executionposture.Live)
	return command, cleanup, err
}

func mutateNativeReceiptRequest(t *testing.T, original inbound.Request, mutation string) inbound.Request {
	t.Helper()
	request := original
	switch mutation {
	case "receipt_identity":
		request.ProviderEventID = uuid.NewString()
		var err error
		request.PublicationID, request.MarkerEventID, err = inbound.DeterministicIDs(request.Identity())
		if err != nil {
			t.Fatal(err)
		}
	case "fingerprint":
		request.RequestFingerprint = strings.Repeat("0", 64)
	case "received_time":
		request.OriginalReceivedAt = request.OriginalReceivedAt.Add(time.Microsecond)
	case "missing_capture":
		request.OriginalTransportMetadata = json.RawMessage(`{}`)
	case "capture_occurrence", "capture_body", "capture_source":
		capture, err := sessioncapture.PublicationCaptureProvenance(request)
		if err != nil {
			t.Fatal(err)
		}
		switch mutation {
		case "capture_occurrence":
			capture.OccurrenceID = uuid.NewString()
		case "capture_body":
			capture.Body = []byte(`{"invented":"not the authenticated input"}`)
		case "capture_source":
			capture.Source.Coordinate.ContextPublicationGeneration++
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(request.OriginalTransportMetadata, &fields); err != nil {
			t.Fatal(err)
		}
		fields[sessioncapture.CaptureProvenanceKey], err = json.Marshal(capture)
		if err != nil {
			t.Fatal(err)
		}
		request.OriginalTransportMetadata, err = json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown request mutation", mutation)
	}
	return request
}

func TestWhatsAppCapturedReceiptAdmissionMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, path := range []string{"operator", "business"} {
			for _, stage := range []string{"first", "staged", "recovered"} {
				t.Run(backend+"/"+path+"/"+stage, func(t *testing.T) {
					f := newActiveInputFixture(t, backend)
					f.activate(t)
					var event capturedEvent
					if path == "operator" {
						event = f.receiveCaptureMessageSDK(t, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
							Text: proto.String("Open inbox"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("original-delivery"),
								QuotedMessage: &waE2E.Message{Conversation: proto.String("original summary")}}}}, uuid.NewString(), time.Now())
					} else {
						event = f.receiveCaptureSDK(t, "ordinary business content", uuid.NewString(), time.Now())
					}
					admitted, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
						OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
					if err != nil {
						t.Fatal(err)
					}
					defer admitted.Close()
					eventBus := f.publicationBus(t)
					prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus,
						f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
					if err != nil {
						t.Fatal("original preparation", err)
					}
					if stage != "first" {
						if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
							t.Fatal(err)
						}
					}
					if stage == "recovered" {
						if path == "business" {
							if err := eventBus.AbandonInboundDeliveryPlan(f.ctx, prepared.plan); err != nil {
								t.Fatal(err)
							}
						}
						admitted.Close()
						f.restartBusinessConnection(t)
						admitted, err = f.owner.recoverBusiness(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
							OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
						if err != nil {
							t.Fatal(err)
						}
						defer admitted.Close()
						eventBus = f.publicationBus(t)
						prepared, err = prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus,
							f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
						if err != nil || prepared.command.Request.ExpectedPublicationSequence == admitted.PublicationSequence() {
							t.Fatal("recovery lost the distinction between frozen evidence and current fencing", err)
						}
					}
					if path == "business" {
						if err := eventBus.AbandonInboundDeliveryPlan(f.ctx, prepared.plan); err != nil {
							t.Fatal("release original planning claim before adverse reprojection", err)
						}
					}
					for _, mutation := range []string{"receipt_identity", "fingerprint", "received_time", "missing_capture", "capture_occurrence", "capture_body", "capture_source"} {
						t.Run(mutation, func(t *testing.T) {
							request := mutateNativeReceiptRequest(t, prepared.command.Request, mutation)
							if err := sessioncapture.ValidateCapturePublication(event, request); err == nil {
								t.Fatal("mutation did not contradict canonical capture evidence")
							}
							command, cleanup, err := reprojectReceiptCommand(t, prepared.ctx, f, eventBus, admitted, request, path == "operator")
							defer cleanup()
							if err == nil {
								result, commitErr := prepared.store.CommitInboundPublication(prepared.ctx, command)
								err = commitErr
								if result.Acknowledged || result.Record.Created {
									t.Fatal("contradictory capture request was acknowledged", result)
								}
								requireNoSessionPublicationEvents(t, f, command)
							}
							if !errors.Is(err, inbound.ErrRequestIdentityConflict) {
								t.Fatal("common admission did not preserve capture identity refusal", err)
							}
							if _, found, err := prepared.store.LoadInboundPublicationByIdentity(f.ctx, request.Identity()); err != nil || found {
								t.Fatal("refused capture mutation persisted a receipt", found, err)
							}
							texts, err := f.selected.(channeldelivery.Store).ListPendingChannelTexts(f.ctx, "", 10)
							if err != nil || len(texts) != 0 {
								t.Fatal("refused capture mutation persisted operator intent", texts, err)
							}
						})
					}
					_, foreign := f.receive(t, "different genuine SDK input")
					defer foreign.Close()
					signed, err := f.trigger.AdmitSessionInput(prepared.ctx, foreign)
					if err != nil {
						t.Fatal(err)
					}
					_, substitute, err := f.trigger.ProjectPublication(signed, f.operation.Coordinate.BundleHash, prepared.command.Request.FlowPath)
					if err != nil {
						t.Fatal(err)
					}
					for _, replacement := range []string{"missing", "substituted", "reconstructed"} {
						t.Run(replacement+"_admission", func(t *testing.T) {
							command, cleanup, err := reprojectReceiptCommand(t, prepared.ctx, f, eventBus, admitted,
								prepared.command.Request, path == "operator")
							defer cleanup()
							if err != nil {
								t.Fatal("valid original admission control", err)
							}
							switch replacement {
							case "missing":
								command.Admission = providertriggers.PublicationAdmission{}
							case "substituted":
								command.Admission = substitute
							case "reconstructed":
								raw, err := json.Marshal(command.Admission)
								if err != nil {
									t.Fatal(err)
								}
								command.Admission = providertriggers.PublicationAdmission{}
								if err := json.Unmarshal(raw, &command.Admission); err != nil {
									t.Fatal(err)
								}
							}
							result, err := prepared.store.CommitInboundPublication(prepared.ctx, command)
							if err == nil || result.Acknowledged || result.Record.Created {
								t.Fatal("replacement admission granted capture receipt permission", result, err)
							}
							requireNoSessionPublicationEvents(t, f, command)
							if _, found, err := prepared.store.LoadInboundPublicationByIdentity(f.ctx, command.Request.Identity()); err != nil || found {
								t.Fatal("replacement admission left a receipt", found, err)
							}
						})
					}
					prepared, err = prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus,
						f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
					if err != nil {
						t.Fatal("valid original preparation after adverse probes", err)
					}
					result, err := prepared.commitAndDispatch()
					if err != nil || !result.Acknowledged || !result.Record.Created {
						t.Fatal("original captured request failed after adverse probes", result, err)
					}
					history, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus,
						f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
					if err != nil {
						t.Fatal("historical receipt reconciliation", err)
					}
					duplicate, err := history.commitAndDispatch()
					if err != nil || !duplicate.Acknowledged || duplicate.Record.Created {
						t.Fatal("original receipt repeated execution", duplicate, err)
					}
				})
			}
		}
	}
}
