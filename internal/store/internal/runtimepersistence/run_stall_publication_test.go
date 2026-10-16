package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// The event commit and the feedback read are separate facts. An error in the
// latter must not turn acknowledged publication into apparent agent silence.
type stallFeedbackFailureStore struct {
	storeTestDurableEventBusStore
	bus.CommitPublicationOwner
	pipeline.WorkflowEmitFeedbackOwner
	feedbackFault    error
	publicationFault error
}

func (s stallFeedbackFailureStore) ReadWorkflowPublicationStages(ctx context.Context, id string, instance flowidentity.RunScopedFlowInstance) (pipeline.WorkflowPublicationStageEvidence, bool, error) {
	if s.feedbackFault != nil {
		return pipeline.WorkflowPublicationStageEvidence{}, false, s.feedbackFault
	}
	return s.WorkflowEmitFeedbackOwner.ReadWorkflowPublicationStages(ctx, id, instance)
}

func (s stallFeedbackFailureStore) CommitPublication(ctx context.Context, command bus.PublicationCommand) (bus.CommittedPublication, error) {
	if s.publicationFault != nil {
		return bus.CommittedPublication{}, s.publicationFault
	}
	return s.CommitPublicationOwner.CommitPublication(ctx, command)
}

func TestRunStallAttributionPublicationEvidenceBothStores(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "claude_cli"} {
		t.Run(adapter, func(t *testing.T) {
			for _, scope := range []string{"root", "static"} {
				t.Run(scope, func(t *testing.T) {
					phases := []string{"no_proposal", "proposal_only", "accepted", "accepted_feedback_error", "missing_receipt", "rejected"}
					if adapter == "claude_cli" {
						phases = append(phases, "native_output")
					}
					for _, phase := range phases {
						t.Run(phase, func(t *testing.T) {
							proveRunStallPublicationEvidence(t, adapter, scope, phase)
						})
					}
				})
			}
		})
	}
}

func proveRunStallPublicationEvidence(t *testing.T, adapter, scope, phase string) {
	t.Helper()
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		flow := ""
		if scope == "static" {
			flow = "global"
		}
		fixture := newCompletionSettlementFixtureForFlow(t, selected.selected.(completionSettlementTestStore), selected.db, !selected.postgres, agentmemory.Plan{}, flow)
		bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", sourceartifactfixture.Artifact(), platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ctx := correlation.WithRunID(fixture.context, fixture.origin.RunID())
		ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
		ctx = effects.WithLifecycleToken(ctx, fixture.authority.Normal)
		staticSource := constructStaticPublicationSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), bundle, "global")
		routing := eventtest.RootRoutingSource(fixture.origin.RunID())
		eventType := events.EventType("test.node_emitted")
		instance := flowidentity.RunScopedFlowInstance{RunID: fixture.origin.RunID(), Route: flowidentity.StoredRoute(".", fixture.origin.RunID(), fixture.origin.RunID())}
		if scope == "static" {
			routing, eventType = staticSource, "global/test.node_emitted"
			instance.Route = flowidentity.StoredRoute("global", "global", "global")
		}
		catalogScope := authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash)
		ctx = authoractivity.WithScope(ctx, catalogScope)
		toolName := tools.EmitToolName("test.node_emitted")
		surface := managedCompletionTestSurface(t, fixture.authority, adapter)
		binding := managedcapabilities.DeliveryBinding{Kind: managedcapabilities.BindingAPIDefinition, ExactName: toolName, RequiredEvidenceKind: "api_definition"}
		if adapter == "claude_cli" {
			binding = managedcapabilities.DeliveryBinding{Kind: managedcapabilities.BindingMCPTool, ExactName: "mcp__runtime-tools__" + toolName, RequiredEvidenceKind: "mcp_listed"}
		}
		surface, err = managedcapabilities.New(managedcapabilities.Plan{
			ActorIdentity: surface.ActorIdentity, RuntimeMode: surface.RuntimeMode,
			Provider: surface.Provider, Transport: surface.Transport, ProviderContract: surface.ProviderContract,
			Authority: surface.Authority, CreatedAt: surface.CreatedAt,
			Tools: []managedcapabilities.PlannedTool{{Name: toolName, DefinitionHash: strings.Repeat("a", 64),
				Capability: toolcapabilities.Capability{Name: toolName, Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
				Bindings:   []managedcapabilities.DeliveryBinding{binding}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{
			BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: binding.RequiredEvidenceKind, Status: managedcapabilities.EvidenceConfirmed,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, adapter)
		ctx = managedcapabilities.WithContext(effects.WithLogicalOperationIdentity(ctx, "stall-publication"), surface)
		frame := managedCompletionTestFrameWithSurface(t, fixture.authority, managedCompletionTestEvent(fixture.authority), sourceartifactfixture.BundleHash, surface)
		handle, err := effects.BeginManagedCompletion(ctx, adapter, []byte("stall-publication"), frame, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkLaunched(ctx); err != nil {
			t.Fatal(err)
		}
		if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "stall-publication"}); err != nil {
			t.Fatal(err)
		}
		settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, adapter, "", "")
		settlement.ProviderHead = nil
		settlement.Now = settlement.Now.Truncate(time.Microsecond)
		settlement.AgentTurn.CapabilitySurfaceID = surface.ID
		settlement.AgentTurn.CapabilitySurface = mustJSON(t, surface)
		payload, messages := completionSuccessorPayload(t, adapter, fixture, settlement, frame.FrameID)
		var document map[string]json.RawMessage
		if err := json.Unmarshal(payload, &document); err != nil {
			t.Fatal(err)
		}
		var response llm.Response
		if err := json.Unmarshal(document["response"], &response); err != nil {
			t.Fatal(err)
		}
		response.ToolCalls[0].Name = toolName
		if phase == "no_proposal" || phase == "native_output" {
			response.ToolCalls = nil
		}
		response.Message.ToolCalls = response.ToolCalls
		response.ToolOutputAuthority = &llm.ToolOutputAuthority{ProviderOperationID: handle.Attempt().OperationID, SettledAt: settlement.Now}
		document["response"] = mustJSON(t, response)
		messages[len(messages)-1] = response.Message
		var projection map[string]json.RawMessage
		if err := json.Unmarshal(document["projection"], &projection); err != nil {
			t.Fatal(err)
		}
		projection["messages"] = mustJSON(t, messages)
		document["projection"] = mustJSON(t, projection)
		payload = mustJSON(t, document)
		if err := effects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte("stall-publication"), payload); err != nil {
			t.Fatal(err)
		}
		if result, err := handle.SettleCompletion(ctx, settlement); err != nil || !result.Committed {
			t.Fatalf("settle: %+v %v", result, err)
		}
		continuation, ok := handle.CompletionContinuation()
		if !ok {
			t.Fatal("settled completion has no canonical continuation")
		}
		if err := handle.ProjectCompletionConversation(ctx, effects.CompletionConversationProjection{
			SessionID: fixture.sessionID, Identity: settlement.AgentTurn.Identity, Memory: settlement.AgentTurn.Memory,
			ExpectedTurnCount: 0, TurnCount: 1, Messages: mustJSON(t, messages), Payload: continuation.Payload,
		}); err != nil {
			t.Fatal(err)
		}
		candidates, err := llm.ReadCompletionOutputCandidates(continuation.Payload, adapter, handle.Attempt().OperationID, surface)
		wantCandidates := 1
		if phase == "no_proposal" || phase == "native_output" {
			wantCandidates = 0
		}
		if err != nil || len(candidates) != wantCandidates {
			t.Fatalf("exact granted output: %+v %v", candidates, err)
		}
		outputID, outputAt := uuid.NewString(), settlement.Now
		if len(candidates) == 1 {
			outputID, outputAt = candidates[0].EventID, candidates[0].CreatedAt
		}
		if phase != "proposal_only" && phase != "no_proposal" {
			lease, err := selected.selected.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(catalogScope, []authoractivity.EventDescriptor{{EventType: string(eventType), Disposition: authoractivity.StoryAuthored}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lease.Release)
			publisherStore := selected.selected.(storeTestDurableEventBusStore)
			feedbackErr := errors.New("feedback read unavailable after publication")
			publicationErr := errors.New("publication refused before native commit")
			if phase == "accepted_feedback_error" || phase == "rejected" {
				wrapper := stallFeedbackFailureStore{storeTestDurableEventBusStore: publisherStore, CommitPublicationOwner: selected.selected.(bus.CommitPublicationOwner), WorkflowEmitFeedbackOwner: selected.selected.(pipeline.WorkflowEmitFeedbackOwner)}
				if phase == "rejected" {
					wrapper.publicationFault = publicationErr
				} else {
					wrapper.feedbackFault = feedbackErr
				}
				publisherStore = wrapper
			}
			publisher, err := newStoreTestEventBus(t, publisherStore, bus.EventBusOptions{RuntimeInstanceID: catalogScope.RuntimeInstanceID})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ChildForProducerWithRoutingSource(outputID, eventType, eventtest.Producer(events.EventProducerAgent, fixture.agentID), "store-test",
				json.RawMessage(`{"request":"store-test"}`), 1, events.LineageFromEvent(managedCompletionTestEvent(fixture.authority)), events.EnvelopeForSourceRoute(events.EventEnvelope{}, routing.Route()), routing, outputAt)
			if phase == "missing_receipt" {
				if err := publisher.Publish(ctx, event); err != nil {
					t.Fatal(err)
				}
			} else {
				result, err := publisher.PublishEmit(ctx, event, pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: routing.Route().EntityID})
				if phase == "rejected" {
					if result.Accepted || result.FeedbackCommitted || !errors.Is(err, publicationErr) {
						t.Fatalf("failed commit fabricated accepted output: %+v %v", result, err)
					}
				} else if !result.Accepted || ((phase == "accepted" || phase == "native_output") && (err != nil || !result.FeedbackCommitted)) || (phase == "accepted_feedback_error" && (!errors.Is(err, feedbackErr) || result.FeedbackCommitted)) {
					t.Fatalf("publication acceptance/feedback: %+v %v", result, err)
				}
			}
		}
		if err := handle.ConsumeCompletionResponse(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
			t.Fatal(err)
		}
		var previous *operatorread.RunTurnAttribution
		for read := 0; read < 2; read++ {
			report, err := fixture.store.(operatorread.RunReader).LoadRunDebugReport(ctx, fixture.origin.RunID(), operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var got *operatorread.RunTurnAttribution
			for _, instanceFacts := range report.OperationalFacts.Instances {
				if instanceFacts.FlowInstance == instance.Route.InstancePath {
					got = instanceFacts.LastTurn
				}
			}
			want := operatorread.RunEmissionUnknown
			accepted := phase == "accepted" || phase == "accepted_feedback_error" || phase == "native_output"
			if accepted {
				want = operatorread.RunEmissionAccepted
			} else if phase == "no_proposal" {
				want = operatorread.RunEmissionNone
			}
			if got == nil || got.Emission != want || got.DeliveryID != fixture.origin.DeliveryID() || got.AgentID != fixture.agentID || got.EventID != managedCompletionTestEvent(fixture.authority).ID() || got.LogicalTurnID == "" || len(got.ProviderTurns) != 1 || !reflect.DeepEqual(got.GrantedEmitTools, []string{toolName}) {
				t.Fatalf("publication attribution = %+v, want %s", got, want)
			}
			if phase == "proposal_only" && got.EvidenceReason != "proposed_publication_unproven" || phase == "missing_receipt" && got.EvidenceReason != "publication_origin_ambiguous" {
				t.Fatalf("incomplete output evidence lost its exact reason: %+v", got)
			}
			wantEvents := 1
			if phase != "proposal_only" && phase != "no_proposal" && phase != "rejected" {
				wantEvents++
			}
			if report.EventCount != wantEvents || report.Header.EventCount != wantEvents {
				t.Fatalf("diagnostic published work: report=%d header=%d want=%d", report.EventCount, report.Header.EventCount, wantEvents)
			}
			if !accepted && len(got.EmittedEvents) != 0 || accepted && !reflect.DeepEqual(got.EmittedEvents, []operatorread.RunAcceptedEmission{{EventID: outputID, EventType: string(eventType)}}) {
				t.Fatalf("publication evidence = %+v", got.EmittedEvents)
			}
			if previous != nil && !reflect.DeepEqual(previous, got) {
				t.Fatal("diagnostic read changed immutable attribution")
			}
			previous = got
		}
		requireProviderAttemptCount(t, fixture, 1)
	})
}
