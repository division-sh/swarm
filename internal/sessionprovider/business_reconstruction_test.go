//go:build linux || darwin

package sessionprovider

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func reconstructPublication(command bus.PublicationCommand) bus.PublicationCommand {
	return bus.PublicationCommand{
		Commit: command.Commit, Activations: command.Activations, RouteTopology: command.RouteTopology,
		DynamicFlowCreation: command.DynamicFlowCreation, AuthorScope: command.AuthorScope,
		HasAuthorScope: command.HasAuthorScope, AuthorDescriptor: command.AuthorDescriptor,
		HasAuthorDescriptor: command.HasAuthorDescriptor,
	}
}

func reconstructCommit(commit bus.CommitPublishRequest) bus.CommitPublishRequest {
	return bus.CommitPublishRequest{
		Event: commit.Event, RouteSettlement: commit.RouteSettlement, DeliveryRoutes: commit.DeliveryRoutes,
		DeliveryAuthority: commit.DeliveryAuthority, ReplayScope: commit.ReplayScope, PipelineClaim: commit.PipelineClaim,
		Disposition: commit.Disposition, DeadLetter: commit.DeadLetter, ReplyCreations: commit.ReplyCreations,
		ReplyClaims: commit.ReplyClaims, JoinAdmissionFences: commit.JoinAdmissionFences,
	}
}

func TestWhatsAppBusinessReconstructionAuthorityMatrixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutation := range []string{"current", "release", "retire_activation", "unbind"} {
			t.Run(backend+"/"+mutation, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				eventBus := f.publicationBus(t)
				_, admitted := f.receive(t, "original sealed content")
				prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), prepared.plan) })
				_, otherInput := f.receive(t, "different sealed content")
				other, err := prepareSessionBusinessPublication(f.ctx, otherInput, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), other.plan) })
				now := time.Now().UTC().Truncate(time.Microsecond)
				switch mutation {
				case "release":
					admitted.Close()
				case "retire_activation":
					_, err = f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "reconstruction proof", Now: now})
				case "unbind":
					_, _, err = f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), now)
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, original := range prepared.command.Publications {
					rebuilt := reconstructPublication(original)
					if _, err := f.selected.(bus.CommitPublicationOwner).CommitPublication(f.ctx, rebuilt); err == nil {
						t.Fatal("generic publication accepted reconstructed ingress")
					}
					apiCommand := bus.APIEventPublicationCommand{Publication: rebuilt,
						Idempotency: apiidempotency.Request{Method: "event.publish"},
						Completion:  apiidempotency.Completion{ResourceID: rebuilt.Commit.Event.ID(), Response: []byte(`{}`)}}
					if _, err := f.selected.(bus.APIEventPublicationCommitOwner).CommitAPIEventPublication(f.ctx, apiCommand); err == nil {
						t.Fatal("API adapter accepted reconstructed ingress")
					}
					if err := rebuilt.ValidateFanOut(); err == nil {
						t.Fatal("fan-out adapter accepted reconstructed ingress")
					}
					rebuilt.Commit = reconstructCommit(original.Commit)
					if _, err := f.selected.(bus.CommitPublicationOwner).CommitPublication(f.ctx, rebuilt); err == nil {
						t.Fatal("generic publication accepted reconstructed commit")
					}
				}
				for _, transfer := range []string{"missing", "substituted", "reconstructed_commit", "retained"} {
					if transfer == "retained" && mutation == "current" {
						continue // Covered by the actual commit/dispatch positive journey.
					}
					command := prepared.command
					command.Publications = nil
					for _, original := range prepared.command.Publications {
						rebuilt := reconstructPublication(original)
						if transfer == "reconstructed_commit" {
							rebuilt.Commit = reconstructCommit(original.Commit)
						}
						command.Publications = append(command.Publications, rebuilt)
					}
					switch transfer {
					case "missing":
						command.Admission = providertriggers.PublicationAdmission{}
					case "substituted":
						command.Admission = other.command.Admission
					}
					if _, err := f.selected.(sessionBusinessStore).CommitInboundPublication(f.ctx, command); err == nil {
						t.Fatalf("inbound accepted %s authority after %s", transfer, mutation)
					}
					requireNoSessionPublicationEvents(t, f, prepared.command)
					if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity()); err != nil || found {
						t.Fatalf("%s left receipt: found=%t err=%v", transfer, found, err)
					}
				}
			})
		}
	}
}

func TestWhatsAppBusinessCommandReconstructionCannotDropFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, route := range []string{"generic", "inbound_without_admission"} {
			t.Run(backend+"/"+route, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				eventBus := f.publicationBus(t)
				_, admitted := f.receive(t, "must remain native")
				prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), prepared.plan) })
				original := prepared.command.Publications[0]
				admitted.Close()
				if _, err := f.selected.(bus.CommitPublicationOwner).CommitPublication(f.ctx, original); err == nil {
					t.Fatal("control: native command unexpectedly accepted")
				}
				if route == "generic" {
					_, err = f.selected.(bus.CommitPublicationOwner).CommitPublication(f.ctx, reconstructPublication(original))
				} else {
					command := prepared.command
					command.Admission = providertriggers.PublicationAdmission{}
					command.Publications = nil
					for _, publication := range prepared.command.Publications {
						command.Publications = append(command.Publications, reconstructPublication(publication))
					}
					_, err = f.selected.(sessionBusinessStore).CommitInboundPublication(f.ctx, command)
				}
				if err == nil {
					t.Fatal("reconstructed command bypassed native lifetime")
				}
				requireNoSessionPublicationEvents(t, f, prepared.command)
				if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity()); err != nil || found {
					t.Fatalf("reconstructed command left receipt: found=%t err=%v", found, err)
				}
			})
		}
	}
}
