package agentframe

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

const testBundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestToolContinuationIsCanonicalAndValidByConstruction(t *testing.T) {
	parent := "agent-frame:v1:00000000-0000-4000-8000-000000000099"
	continuation, err := NewToolContinuation(parent, json.RawMessage(`[{"result":{"b":2,"a":1},"ok":true,"name":"echo"}]`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := continuation.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeToolContinuation(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ParentFrameID() != parent || string(decoded.ToolResult()) != `[{"name":"echo","ok":true,"result":{"a":1,"b":2}}]` {
		t.Fatalf("decoded continuation parent=%q result=%s", decoded.ParentFrameID(), decoded.ToolResult())
	}
	for _, hostile := range []json.RawMessage{
		json.RawMessage(`{"version":"agent-tool-continuation.v1","parent_frame_id":"bad","tool_result":[{"ok":true}]}`),
		json.RawMessage(`{"version":"agent-tool-continuation.v1","parent_frame_id":"` + parent + `","tool_result":[]}`),
		json.RawMessage(`{"version":"foreign","parent_frame_id":"` + parent + `","tool_result":[{"ok":true}]}`),
	} {
		if _, err := DecodeToolContinuation(hostile); err == nil {
			t.Fatalf("hostile continuation accepted: %s", hostile)
		}
	}
}

func TestToolContinuationDecodeRejectsNonCanonicalEnvelope(t *testing.T) {
	parent := "agent-frame:v1:00000000-0000-4000-8000-000000000099"
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"version":"agent-tool-continuation.v1","parent_frame_id":"` + parent + `","tool_result":[{"name":"echo","ok":true}],"extra":true}`),
		json.RawMessage(`{"version":"agent-tool-continuation.v1","parent_frame_id":"` + parent + `","tool_result":[{"name":"echo","ok":true}]} {}`),
	} {
		if _, err := DecodeToolContinuation(raw); err == nil {
			t.Fatalf("decoded hostile continuation %s", raw)
		}
	}
}

func TestExecutionFrameCapabilityProjectionUsesPlanNotObservedEvidence(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	frame, err := Complete(seed, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: surface,
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if frame.Turn.Event.ProducerType != string(events.EventProducerExternal) || frame.Turn.Event.RoutingSource.Kind == "" {
		t.Fatalf("admitted event provenance was not preserved: %#v", frame.Turn.Event)
	}
	if got := CapabilityNames(frame); len(got) != 1 || got[0] != "event.publish" {
		t.Fatalf("authorized candidates = %#v", got)
	}
	role, content, err := frame.ProviderInput()
	if err != nil || role != "user" {
		t.Fatalf("ProviderInput = role %q err %v", role, err)
	}
	var rendered map[string]any
	if err := json.Unmarshal([]byte(content), &rendered); err != nil || rendered["kind"] != string(TurnInitial) {
		t.Fatalf("provider rendering = %q err=%v", content, err)
	}

	observed, err := surface.Observe(managedcapabilities.DeliveryEvidence{
		BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: "event.publish",
		Kind: "definition_attached", Status: managedcapabilities.EvidenceConfirmed,
	})
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	observedFrame, err := Complete(seed, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: observed,
	})
	if err != nil {
		t.Fatalf("Complete observed: %v", err)
	}
	if observedFrame.FrameID != frame.FrameID || observedFrame.ContentHash != frame.ContentHash {
		t.Fatalf("post-call evidence mutated frame identity: before=%#v after=%#v", frame, observedFrame)
	}
}

func TestExecutionFrameContentHashBindsCanonicalSemanticFacts(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	base := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: event}, surface)

	changedIntent := seed
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.worker.intent", "Process different admitted work.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	changedIntent.Intent = intent
	changedIntent.ProviderPrompt, err = agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}

	changedCriteria := seed
	changedCriteria.Criteria = []string{"criteria/review.md"}
	criteriaPrompt, err := agentintent.ContractCriteriaPrompt(changedCriteria.Intent, changedCriteria.Criteria, map[string]flowmodel.PolicyCriteriaSet{
		"criteria/review.md": {
			Classes: map[string]flowmodel.PolicyCriteriaClass{"required": {Disposition: "reject"}},
			Rules:   []flowmodel.PolicyCriteriaRule{{ID: "REVIEW-01", Class: "required", Text: "Review the admitted work."}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	changedCriteria.ProviderPrompt, err = agentintent.AssembleProviderPrompt(changedCriteria.Intent, changedCriteria.Criteria, criteriaPrompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}

	changedActor := seed
	changedActor.AgentIdentity = agentidentity.Identity{
		Name:  agentidentity.Name{AgentID: "reviewer", Owner: "agentframe-test", Source: agentidentity.NameSourceDeclared},
		Route: agentidentity.RootRoute(),
	}
	actorSurface, err := managedcapabilities.New(testCapabilityPlan(changedActor.AgentIdentity, event.RunID(), surface.Authority.ID, surface.Authority.TurnOrdinal))
	if err != nil {
		t.Fatal(err)
	}

	changedEvent := eventtest.RunCreatingRootIngress(
		"00000000-0000-4000-8000-000000000004", "work.requested", "operator", "task-1",
		json.RawMessage(`{"a":2}`), 0, event.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, event.EntityID()), time.Unix(1, 0).UTC(),
	)

	changedPlan := testCapabilityPlan(seed.AgentIdentity, event.RunID(), surface.Authority.ID, surface.Authority.TurnOrdinal)
	changedPlan.Tools[0].DefinitionHash = "different-definition-hash"
	planSurface, err := managedcapabilities.New(changedPlan)
	if err != nil {
		t.Fatal(err)
	}
	changedProviderAlias := seed
	changedProviderAlias.ModelAlias = "frontier"

	variants := map[string]Frame{
		"intent":         completeTestFrame(t, changedIntent, TurnDraft{Kind: TurnInitial, Event: event}, surface),
		"criteria":       completeTestFrame(t, changedCriteria, TurnDraft{Kind: TurnInitial, Event: event}, surface),
		"actor":          completeTestFrame(t, changedActor, TurnDraft{Kind: TurnInitial, Event: event}, actorSurface),
		"event":          completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: changedEvent}, surface),
		"provider alias": completeTestFrame(t, changedProviderAlias, TurnDraft{Kind: TurnInitial, Event: event}, surface),
		"kind and parent": completeTestFrame(t, seed, TurnDraft{
			Kind: TurnToolContinuation, Event: event,
			ParentFrameID: "agent-frame:v1:00000000-0000-4000-8000-000000000099",
			InputRole:     "tool", InputContent: `[{"ok":true,"result":{"status":"ready"}}]`,
		}, surface),
		"capability plan": completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: event}, planSurface),
	}
	for name, variant := range variants {
		if variant.FrameID != base.FrameID {
			t.Errorf("%s changed occurrence identity: got %q want %q", name, variant.FrameID, base.FrameID)
		}
		if variant.ContentHash == base.ContentHash {
			t.Errorf("%s did not change canonical content hash %q", name, base.ContentHash)
		}
	}
}

func TestExecutionFrameConsumesAdmittedProviderTriggerEventFactsWithoutNormalization(t *testing.T) {
	seed, _, surface := testExecutionFrameInputs(t)
	route := events.RouteIdentity{FlowID: "telegram-ingress", EntityID: "00000000-0000-4000-8000-000000000003"}
	routingSource, err := events.NewExternalIngressRoutingSource(route.FlowID, route.EntityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(
		"00000000-0000-4000-8000-000000000005", "inbound.telegram.text_message", "telegram-provider", "task-1",
		json.RawMessage(`{"message":"hello","provider_update_id":"42"}`), 0, surface.Authority.RunID,
		events.EventEnvelope{Source: route}, routingSource, time.Unix(2, 0).UTC(),
	)
	frame := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: event}, surface)
	got := frame.Turn.Event
	if got.ProducerType != string(events.EventProducerExternal) || got.ProducerID != "telegram-provider" {
		t.Fatalf("provider producer projection = %#v", got)
	}
	if got.RoutingSource.Kind != routingSource.Kind().StorageCode() || got.RoutingSource.Authority != routingSource.Authority().StorageCode() {
		t.Fatalf("provider routing-source projection = %#v, want kind=%q authority=%q", got.RoutingSource, routingSource.Kind().StorageCode(), routingSource.Authority().StorageCode())
	}
	if got.RoutingSource.Route.FlowID != route.FlowID || got.RoutingSource.Route.EntityID != route.EntityID || string(got.Payload) != `{"message":"hello","provider_update_id":"42"}` {
		t.Fatalf("provider trigger facts were normalized or inferred: %#v", got)
	}
}

func TestExecutionFrameRequiresPairedExactProviderModelSelection(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	for name, mutate := range map[string]func(*SessionSeed){
		"missing alias":      func(seed *SessionSeed) { seed.ModelAlias = "" },
		"missing model":      func(seed *SessionSeed) { seed.Model = "" },
		"non-exact alias":    func(seed *SessionSeed) { seed.ModelAlias = " regular " },
		"non-exact concrete": func(seed *SessionSeed) { seed.Model = " test-model " },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := seed
			mutate(&candidate)
			if _, err := Complete(candidate, TurnDraft{Kind: TurnInitial, Event: event}, Completion{BundleHash: testBundleHash, Surface: surface}); err == nil {
				t.Fatal("execution frame accepted an incomplete or non-exact provider model selection")
			}
		})
	}
}

func TestExecutionFramePreservesAndHashesExactAdmittedPayloadBytes(t *testing.T) {
	seed, baseEvent, surface := testExecutionFrameInputs(t)
	makeEvent := func(payload string) events.Event {
		return eventtest.RunCreatingRootIngress(
			baseEvent.ID(), baseEvent.Type(), "operator", baseEvent.TaskID(), json.RawMessage(payload),
			0, baseEvent.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, baseEvent.EntityID()), time.Unix(1, 0).UTC(),
		)
	}
	payloads := []string{
		`{"b":2,"a":1}`,
		`{"a":1,"b":2}`,
		`{ "b":2, "a":1 }`,
	}
	frames := make([]Frame, 0, len(payloads))
	for _, payload := range payloads {
		frame := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: makeEvent(payload)}, surface)
		if got := string(frame.Turn.Event.Payload); got != payload {
			t.Fatalf("frame payload = %q, want exact admitted bytes %q", got, payload)
		}
		decoded, err := base64.StdEncoding.DecodeString(frame.Turn.Event.PayloadBytesBase64)
		if err != nil || string(decoded) != payload {
			t.Fatalf("exact payload evidence = %q err=%v, want %q", decoded, err, payload)
		}
		_, providerInput, err := frame.ProviderInput()
		if err != nil {
			t.Fatal(err)
		}
		var rendered struct {
			Event Event `json:"event"`
		}
		if err := json.Unmarshal([]byte(providerInput), &rendered); err != nil {
			t.Fatal(err)
		}
		providerBytes, err := base64.StdEncoding.DecodeString(rendered.Event.PayloadBytesBase64)
		if err != nil || string(providerBytes) != payload {
			t.Fatalf("provider payload evidence = %q err=%v, want %q", providerBytes, err, payload)
		}
		frames = append(frames, frame)
	}
	for i := range frames {
		for j := i + 1; j < len(frames); j++ {
			if frames[i].ContentHash == frames[j].ContentHash {
				t.Fatalf("byte-distinct payloads %q and %q collapsed to hash %q", payloads[i], payloads[j], frames[i].ContentHash)
			}
		}
	}
	hostile := frames[0]
	hostile.Turn.Event.PayloadBytesBase64 = base64.StdEncoding.EncodeToString([]byte(payloads[1]))
	if err := hostile.Validate(); err == nil {
		t.Fatal("frame validation accepted exact-payload evidence from a different admitted byte sequence")
	}
}

func TestExecutionFrameUsesOnlyAdmittedLifecycleFacts(t *testing.T) {
	seed, baseEvent, surface := testExecutionFrameInputs(t)
	event := eventtest.RunCreatingRootIngress(
		baseEvent.ID(), baseEvent.Type(), "operator", baseEvent.TaskID(),
		json.RawMessage(`{"loop_revision":"payload-guess","pack_provenance":"payload-guess","stage":"payload-guess"}`),
		0, baseEvent.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, baseEvent.EntityID()), time.Unix(1, 0).UTC(),
	)
	frame := completeTestFrame(t, seed, TurnDraft{Kind: TurnInitial, Event: event}, surface)
	if frame.Turn.Lifecycle.Stage != Unresolved() || frame.Turn.Lifecycle.LoopRevision != Unresolved() || frame.Turn.PackProvenance != Unresolved() {
		t.Fatalf("frame inferred lifecycle or pack facts from payload: %#v", frame.Turn)
	}
	hostile := frame
	hostile.Turn.PackProvenance = Resolved("pack-guess")
	if err := hostile.Validate(); err == nil {
		t.Fatal("frame accepted resolved pack provenance without a promoted typed owner")
	}
}

func TestExecutionFrameContinuationBindsParentAndCanonicalToolResult(t *testing.T) {
	seed, event, firstSurface := testExecutionFrameInputs(t)
	first, err := Complete(seed, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: firstSurface,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPlan := testCapabilityPlan(seed.AgentIdentity, event.RunID(), "00000000-0000-4000-8000-000000000012", 2)
	secondSurface, err := managedcapabilities.New(secondPlan)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := Complete(seed, TurnDraft{
		Kind: TurnToolContinuation, Event: event, ParentFrameID: first.FrameID,
		InputRole: "tool", InputContent: `[{"result":{"b":2,"a":1},"ok":true}]`,
	}, Completion{BundleHash: testBundleHash, Surface: secondSurface})
	if err != nil {
		t.Fatalf("Complete continuation: %v", err)
	}
	if continuation.Turn.ParentFrameID != first.FrameID || string(continuation.Turn.ToolResult) != `[{"ok":true,"result":{"a":1,"b":2}}]` {
		t.Fatalf("continuation facts = %#v", continuation.Turn)
	}
	role, content, err := continuation.ProviderInput()
	if err != nil || role != "tool" {
		t.Fatalf("ProviderInput continuation = role %q err %v", role, err)
	}
	if !json.Valid([]byte(content)) {
		t.Fatalf("continuation rendering is not JSON: %q", content)
	}
	entries, err := DecodeProviderToolResults(content)
	if err != nil || len(entries) != 1 || !entries[0].OK {
		t.Fatalf("DecodeProviderToolResults = %#v err=%v", entries, err)
	}
}

func TestExecutionFrameRejectsRunAndAuthorityDrift(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	wrongRun := surface.Clone()
	wrongRun.Authority.RunID = "00000000-0000-4000-8000-000000000099"
	if _, err := Complete(seed, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: wrongRun,
	}); err == nil {
		t.Fatal("frame accepted capability authority from another run")
	}

	other := seed
	other.Provider = "other-provider"
	if _, err := Complete(other, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: surface,
	}); err == nil {
		t.Fatal("frame accepted a mismatched provider contract")
	}
}

func TestExecutionFrameRejectsProviderPromptFromAnotherSessionContract(t *testing.T) {
	seed, event, surface := testExecutionFrameInputs(t)
	otherIntent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.worker.intent", "Process unrelated work.")
	if err != nil {
		t.Fatal(err)
	}
	otherDerived, err := agentintent.IntentOnlyPrompt(otherIntent)
	if err != nil {
		t.Fatal(err)
	}
	seed.ProviderPrompt, err = agentintent.AssembleProviderPrompt(otherIntent, nil, otherDerived, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Complete(seed, TurnDraft{Kind: TurnInitial, Event: event}, Completion{
		BundleHash: testBundleHash, Surface: surface,
	}); err == nil {
		t.Fatal("frame accepted provider prompt from another resolved intent")
	}
}

func completeTestFrame(t testing.TB, seed SessionSeed, draft TurnDraft, surface managedcapabilities.Surface) Frame {
	t.Helper()
	frame, err := Complete(seed, draft, Completion{BundleHash: testBundleHash, Surface: surface})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return frame
}

func testExecutionFrameInputs(t testing.TB) (SessionSeed, events.Event, managedcapabilities.Surface) {
	t.Helper()
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.worker.intent", "Process the admitted work.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentity.Identity{
		Name:  agentidentity.Name{AgentID: "worker", Owner: "agentframe-test", Source: agentidentity.NameSourceDeclared},
		Route: agentidentity.RootRoute(),
	}
	seed := SessionSeed{
		AgentIdentity: identity, Role: "worker", Intent: intent, ProviderPrompt: providerPrompt,
		RuntimeMode: "api", Provider: "anthropic", Transport: "api", ModelAlias: "regular", Model: "test-model",
	}
	runID := "00000000-0000-4000-8000-000000000001"
	event := eventtest.RunCreatingRootIngress(
		"00000000-0000-4000-8000-000000000002", "work.requested", "operator", "task-1",
		json.RawMessage(`{"b":2,"a":1}`), 0, runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-4000-8000-000000000003"), time.Unix(1, 0).UTC(),
	)
	surface, err := managedcapabilities.New(testCapabilityPlan(identity, runID, "00000000-0000-4000-8000-000000000011", 1))
	if err != nil {
		t.Fatal(err)
	}
	return seed, event, surface
}

func testCapabilityPlan(identity agentidentity.Identity, runID, authorityID string, ordinal int) managedcapabilities.Plan {
	return managedcapabilities.Plan{
		ActorIdentity: identity, RuntimeMode: "api", Provider: "anthropic", Transport: "api", ProviderContract: "messages.v1",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authorityID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: "runtime-owner", RunID: runID, SessionID: "00000000-0000-4000-8000-000000000010", TurnOrdinal: ordinal,
		},
		Tools: []managedcapabilities.PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings: []managedcapabilities.DeliveryBinding{{
				Kind: managedcapabilities.BindingAPIDefinition, ExactName: "event.publish", RequiredEvidenceKind: "definition_attached",
			}},
		}},
		CreatedAt: time.Unix(1, 0).UTC(),
	}
}
