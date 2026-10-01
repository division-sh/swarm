package packs_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func TestChannelLearnedObjectDestination(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	var profile packs.ChannelOnboardingProfile
	if err := yaml.Unmarshal([]byte(`activation: webhook_registration
ceremony: authenticated_text_challenge
provider_credential: mock_api_key
signing_credential: mock_callback_key
confirmation: deliver
learned_destination:
  destination.queue: conversation_reference.room
`), &profile); err != nil {
		t.Fatal(err)
	}
	channel.Manifest.Onboarding = &profile
	// A learned relation must be provable over its complete admitted source domain.
	destinationLeaf, _ := channel.Manifest.OpaqueTypes["destination"].Property("queue")
	channel.Manifest.OpaqueTypes["conversation_reference"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"room": destinationLeaf}, "room")
	for name, event := range trigger.Events {
		field := event.Fields["room"]
		field.Schema = destinationLeaf
		event.Fields["room"] = field
		trigger.Events[name] = event
	}
	plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	authorization := provideroutput.MustAuthorization("mock", "mock.text", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation)
	fact, matched, err := plan.ProjectTextFact("mock.text", authorization, map[string]any{
		"text": "SWARM-AAAAAAAAAAAAAAAA", "principal": "operator-a", "room": "queue-a", "scope": "direct", "message_ref": "mock-delivery:12345678",
	})
	if err != nil || !matched || fact.ConversationRef != `{"room":"queue-a"}` {
		t.Fatalf("verified object conversation: %+v matched=%t err=%v", fact, matched, err)
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "mock@1.0.0#bundle",
		PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1,
	}
	candidate := channelonboarding.Candidate{
		Provider: "mock", Interface: identity, Coordinate: coordinate,
		Target: channelonboarding.CandidateTarget{
			Selector: "ingress:mock-ingress:mock", ServiceID: "mock-ingress", FlowPath: "mock-ingress", Alias: "mock",
			Provider: "mock", Generation: 1, PublicationSequence: 1,
			AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("mock-admission")), SigningCredentialKey: "mock.signing",
		},
		Posture: channelonboarding.ActivationWebhookRegistration, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
		ProviderCredentialRole: "mock_api_key", SigningCredentialRole: "mock_callback_key",
		ConfirmationOperation: "deliver", Plan: plan,
	}
	activation := channelonboarding.ConnectedChannelActivation{
		ActivationID: uuid.NewString(), SlotKey: "slot-a", OperationID: uuid.NewString(), OperationRevision: 8,
		PrincipalID: "principal-a", Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, BindingRevision: 3, ConversationRef: fact.ConversationRef,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: "mock_api_key", StoreKey: "mock.provider", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("a")},
			{Role: "mock_callback_key", StoreKey: "mock.signing", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("b")},
		},
		Revision: 1, Status: channelonboarding.ActivationCurrent,
	}
	compiled, err := channelonboarding.CompileLearnedActivation(candidate, activation)
	if err != nil {
		t.Fatalf("accepted learned object profile cannot publish: %v", err)
	}
	if got := compiled.Plan.Destination().Interface(); !reflect.DeepEqual(got, map[string]any{"queue": "queue-a"}) {
		t.Fatalf("learned destination=%#v, want declared room-to-queue projection", got)
	}
	for _, stored := range []string{"", `{"room":"queue-a","room":"queue-b"}`, `{"queue":"queue-a"}`, `{"room":"invalid room"}`} {
		invalid := activation
		invalid.ConversationRef = stored
		if _, err := channelonboarding.CompileLearnedActivation(candidate, invalid); err == nil {
			t.Errorf("invalid retained conversation accepted: %q", stored)
		}
	}
	foreign := activation
	foreign.Coordinate.PlanGeneration, err = plangeneration.FromCanonicalValue("foreign")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channelonboarding.CompileLearnedActivation(candidate, foreign); err == nil {
		t.Fatal("foreign plan generation accepted")
	}
}

func TestChannelStoredObjectOperationInputs(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := packs.NewOutboundBindingPlan("object-reference-control", plan, map[string]any{"queue": "queue-a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	authorization := provideroutput.MustAuthorization("mock", "mock.action", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation)
	fact, matched, err := plan.ProjectActionFact("mock.action", authorization, map[string]any{
		"token": "approve", "cursor": "callback-1", "principal": "operator-a", "room": "queue-a", "scope": "shared", "message_ref": "mock-delivery:12345678",
	})
	if err != nil || !matched {
		t.Fatalf("verified object action: %+v matched=%t err=%v", fact, matched, err)
	}
	for _, tc := range []struct {
		operation, inputSlot, opaqueSlot, stored string
		typed                                    map[string]any
		extra                                    map[string]any
	}{
		{"acknowledge_interaction", "interaction_reference", "interaction_reference", fact.InteractionRef, map[string]any{"cursor": "callback-1"}, nil},
		{"read_shared_inbox_entry", "member_reference", "external_account_reference", fact.ExternalAccountRef, map[string]any{"principal": "operator-a"}, map[string]any{"language_code": "fr"}},
		{"install_shared_inbox_entry", "member_reference", "external_account_reference", fact.ExternalAccountRef, map[string]any{"principal": "operator-a"}, map[string]any{"commands": []any{map[string]any{"command": "open", "description": "Open inbox"}}}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			input := map[string]any{tc.inputSlot: tc.typed}
			for key, value := range tc.extra {
				input[key] = value
			}
			if _, _, err := binding.PrepareOperation(tc.operation, input); err != nil {
				t.Fatalf("typed control: %v", err)
			}
			input[tc.inputSlot] = tc.stored
			if _, _, err := binding.PrepareOperation(tc.operation, input); err == nil {
				t.Fatal("strict operation admission accepted an encoded object as a typed value")
			}
			value, err := binding.RestoreOpaqueReference(tc.opaqueSlot, tc.stored)
			if err != nil {
				t.Fatal(err)
			}
			input[tc.inputSlot] = value
			if _, _, err := binding.PrepareOperation(tc.operation, input); err != nil {
				t.Fatalf("restored admitted value: %v", err)
			}
		})
	}
}

func TestChannelOpaqueReferenceRestoration(t *testing.T) {
	telegram := loadTelegramChannelPlan(t)
	channel, trigger, connector := mockChannelSatisfier()
	mock, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, slot, stored string
		plan               packs.SatisfactionPlan
		want               any
		valid              bool
	}{
		{"exact string", "interaction_reference", "  callback-1  ", telegram, "  callback-1  ", true},
		{"JSON-looking string", "interaction_reference", `{"cursor":"callback-1"}`, telegram, `{"cursor":"callback-1"}`, true},
		{"object chosen by schema", "interaction_reference", `{"cursor":"callback-1"}`, mock, map[string]any{"cursor": "callback-1"}, true},
		{"typed integer leaf", "delivery_reference", `{"id":7}`, telegram, map[string]any{"id": float64(7)}, true},
		{"integer outside schema", "delivery_reference", `{"id":0}`, telegram, nil, false},
		{"fractional integer", "delivery_reference", `{"id":1.5}`, telegram, nil, false},
		{"missing plan", "interaction_reference", "callback", packs.SatisfactionPlan{}, nil, false},
		{"unknown slot", "member_reference", `{"principal":"operator-a"}`, mock, nil, false},
		{"wrong known slot", "external_account_reference", `{"cursor":"callback-1"}`, mock, nil, false},
		{"malformed", "interaction_reference", `{"cursor":`, mock, nil, false},
		{"empty carrier", "interaction_reference", "", mock, nil, false},
		{"empty string", "interaction_reference", " \t", telegram, nil, false},
		{"empty object", "interaction_reference", `{}`, mock, nil, false},
		{"object encoded as string", "interaction_reference", `"{\"cursor\":\"callback-1\"}"`, mock, nil, false},
		{"duplicate key", "interaction_reference", `{"cursor":"a","cursor":"b"}`, mock, nil, false},
		{"noncanonical object", "interaction_reference", `{ "cursor": "callback-1" }`, mock, nil, false},
		{"invalid string encoding", "interaction_reference", "\xff", telegram, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.plan.RestoreOpaqueReference(tc.slot, tc.stored)
			if (err == nil) != tc.valid {
				t.Fatalf("restoration valid=%t want=%t: %v", err == nil, tc.valid, err)
			}
			if tc.valid && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("restored=%#v want=%#v", got, tc.want)
			}
		})
	}
}

func TestChannelLearnedDestinationRelationAdmissionAndGeneration(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	leaf := mockStringSchema(1, 10, `^[a-z0-9-]+$`)
	destination := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"queue": leaf, "secondary": leaf}, "queue", "secondary")
	conversation := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"room": leaf, "other": leaf}, "room", "other")
	channel.Manifest.OpaqueTypes["destination"], channel.Manifest.OpaqueTypes["conversation_reference"] = destination, conversation
	for name, event := range trigger.Events {
		event.Fields["room"] = packs.TriggerEventField{Schema: leaf, Required: true}
		event.Fields["other"] = packs.TriggerEventField{Schema: leaf, Required: true}
		trigger.Events[name] = event
	}
	for name, event := range channel.Manifest.Events {
		event.Fields["conversation_reference.other"] = "event.other"
		channel.Manifest.Events[name] = event
	}
	for name, operation := range channel.Manifest.Operations {
		if _, ok := operation.Input["destination.queue"]; ok {
			operation.Input["destination.secondary"] = packs.ChannelMapping{From: "context.destination.secondary"}
			channel.Manifest.Operations[name] = operation
		}
	}
	for name, tool := range connector.Tools {
		properties := tool.InputSchema().Properties()
		if _, ok := properties["destination"]; !ok {
			continue
		}
		properties["destination"] = destination
		tool, err := tool.WithSchemas(mockObjectSchema(properties, tool.InputSchema().RequiredProperties()...), tool.OutputSchema())
		if err != nil {
			t.Fatal(err)
		}
		connector.Tools[name] = tool
	}
	valid := map[string]packs.ChannelMapping{"destination.queue": {From: "conversation_reference.room"}, "destination.secondary": {From: "conversation_reference.other"}}
	channel.Manifest.Onboarding = &packs.ChannelOnboardingProfile{Activation: "webhook_registration", Ceremony: "authenticated_text_challenge", ProviderCredentialRole: "mock_api_key", SigningCredentialRole: "mock_callback_key", Confirmation: "deliver", LearnedDestination: valid}
	compile := func() (packs.SatisfactionPlan, error) {
		return packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	}
	first, err := compile()
	if err != nil {
		t.Fatal(err)
	}
	firstGeneration, err := first.Generation()
	if err != nil {
		t.Fatal(err)
	}
	channel.Manifest.Onboarding.LearnedDestination = map[string]packs.ChannelMapping{"destination.queue": {From: "conversation_reference.other"}, "destination.secondary": {From: "conversation_reference.room"}}
	second, err := compile()
	if err != nil {
		t.Fatal(err)
	}
	secondGeneration, err := second.Generation()
	if err != nil {
		t.Fatal(err)
	}
	if firstGeneration.Equal(secondGeneration) {
		t.Fatal("different complete learned relations share a generation")
	}
	stored := `{"other":"queue-b","room":"queue-a"}`
	got, err := first.LearnedDestination(stored)
	if err != nil || !reflect.DeepEqual(got, map[string]any{"queue": "queue-a", "secondary": "queue-b"}) {
		t.Fatalf("authored mutation changed immutable first plan: %#v %v", got, err)
	}
	got, err = second.LearnedDestination(stored)
	if err != nil || !reflect.DeepEqual(got, map[string]any{"queue": "queue-b", "secondary": "queue-a"}) {
		t.Fatalf("replacement relation not consumed: %#v %v", got, err)
	}
	for _, tc := range []struct {
		name    string
		mapping map[string]packs.ChannelMapping
	}{
		{"missing", nil},
		{"partial", map[string]packs.ChannelMapping{"destination.queue": {From: "conversation_reference.room"}}},
		{"duplicate source", map[string]packs.ChannelMapping{"destination.queue": {From: "conversation_reference.room"}, "destination.secondary": {From: "conversation_reference.room"}}},
		{"overlapping target", map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}, "destination.queue": {From: "conversation_reference.room"}}},
		{"whole object", map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}}},
		{"foreign source", map[string]packs.ChannelMapping{"destination.queue": {From: "context.destination.queue"}, "destination.secondary": {From: "conversation_reference.other"}}},
		{"foreign target", map[string]packs.ChannelMapping{"queue": {From: "conversation_reference.room"}, "destination.secondary": {From: "conversation_reference.other"}}},
		{"unknown source", map[string]packs.ChannelMapping{"destination.queue": {From: "conversation_reference.missing"}, "destination.secondary": {From: "conversation_reference.other"}}},
		{"literal", map[string]packs.ChannelMapping{"destination.queue": {From: "'queue-a'"}, "destination.secondary": {From: "conversation_reference.other"}}},
		{"fanout", map[string]packs.ChannelMapping{"destination.queue": {Each: "conversation_reference.room", Item: []map[string]packs.ChannelMapping{{"queue": {From: "item.room"}}}}, "destination.secondary": {From: "conversation_reference.other"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel.Manifest.Onboarding.LearnedDestination = tc.mapping
			if _, err := compile(); err == nil {
				t.Fatal("invalid learned relation admitted")
			}
		})
	}
	channel.Manifest.Onboarding.LearnedDestination = valid
	channel.Manifest.OpaqueTypes["conversation_reference"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"room": mockStringSchema(1, 20, ""), "other": leaf}, "room", "other")
	if _, err := compile(); err == nil {
		t.Fatal("unprovable broad source domain admitted")
	}
}

func TestChannelNestedTypedOpaqueReferenceRestoration(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	integer := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger, runtimecontracts.ToolSchemaMinimum(1), runtimecontracts.ToolSchemaMaximum(5))
	boolean := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean)
	member := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"principal": mockStringSchema(1, 20, ""),
		"metadata":  mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": integer, "trusted": boolean}, "revision", "trusted"),
	}, "principal", "metadata")
	channel.Manifest.OpaqueTypes["external_account_reference"] = member
	for name, event := range trigger.Events {
		event.Fields["revision"] = packs.TriggerEventField{Schema: integer, Required: true}
		event.Fields["trusted"] = packs.TriggerEventField{Schema: boolean, Required: true}
		trigger.Events[name] = event
	}
	for name, event := range channel.Manifest.Events {
		event.Fields["external_account_reference.metadata.revision"] = "event.revision"
		event.Fields["external_account_reference.metadata.trusted"] = "event.trusted"
		channel.Manifest.Events[name] = event
	}
	for _, name := range []string{"read_shared_inbox_entry", "install_shared_inbox_entry"} {
		operation := channel.Manifest.Operations[name]
		operation.Input["member.metadata.revision"] = packs.ChannelMapping{From: "input.member_reference.metadata.revision"}
		operation.Input["member.metadata.trusted"] = packs.ChannelMapping{From: "input.member_reference.metadata.trusted"}
		channel.Manifest.Operations[name] = operation
		tool := connector.Tools[operation.Tool]
		properties := tool.InputSchema().Properties()
		properties["member"] = member
		updated, err := tool.WithSchemas(mockObjectSchema(properties, tool.InputSchema().RequiredProperties()...), tool.OutputSchema())
		if err != nil {
			t.Fatal(err)
		}
		connector.Tools[operation.Tool] = updated
	}
	plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	stored := `{"metadata":{"revision":3,"trusted":true},"principal":"operator-a"}`
	value, err := plan.RestoreOpaqueReference("external_account_reference", stored)
	if err != nil {
		t.Fatal(err)
	}
	object := value.(map[string]any)
	metadata := object["metadata"].(map[string]any)
	if metadata["revision"] != float64(3) || metadata["trusted"] != true {
		t.Fatalf("typed nested leaves changed: %#v", value)
	}
	metadata["revision"] = float64(4)
	again, err := plan.RestoreOpaqueReference("external_account_reference", stored)
	if err != nil || again.(map[string]any)["metadata"].(map[string]any)["revision"] != float64(3) {
		t.Fatal("restored values retain caller ownership")
	}
	for _, invalid := range []string{
		`{"metadata":{"revision":6,"trusted":true},"principal":"operator-a"}`,
		`{"metadata":{"revision":3,"trusted":"true"},"principal":"operator-a"}`,
		`{"metadata":{"revision":3,"revision":4,"trusted":true},"principal":"operator-a"}`,
		`{"metadata":{"revision":3},"principal":"operator-a"}`,
	} {
		if _, err := plan.RestoreOpaqueReference("external_account_reference", invalid); err == nil {
			t.Errorf("invalid nested reference accepted: %s", invalid)
		}
	}
}
