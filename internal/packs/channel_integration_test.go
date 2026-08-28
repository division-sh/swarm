package packs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func TestChannelSchemaAdmissionRejectsRecursiveMalformedSchemasAtEveryTypedBoundary(t *testing.T) {
	tests := []struct {
		name  string
		build func() error
		want  string
	}{
		{name: "scalar regex", build: func() error {
			_, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaPattern("["))
			return err
		}, want: "pattern is invalid"},
		{name: "object required", build: func() error {
			_, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaRequired("missing"))
			return err
		}, want: "is not declared"},
		{name: "array items", build: func() error {
			_, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaArray)
			return err
		}, want: "array requires items"},
		{name: "typed enum", build: func() error {
			_, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaInteger, runtimecontracts.ToolSchemaEnum("one"))
			return err
		}, want: "must be integer"},
		{name: "programmatic empty enum", build: func() error {
			_, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum())
			return err
		}, want: "enum must contain at least one value"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.build(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("schema owner admission error = %v, want %q", err, tc.want)
			}
		})
	}

	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	channel.Manifest.OpaqueTypes["external_account_reference"] = runtimecontracts.ToolInputSchema{}
	if _, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector}); err == nil || !strings.Contains(err.Error(), "schema is missing") {
		t.Fatalf("channel boundary accepted missing admitted schema: %v", err)
	}
}

func TestChannelGenerationRejectsProgrammaticEmptyEnum(t *testing.T) {
	if _, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum()); err == nil || !strings.Contains(err.Error(), "enum must contain at least one value") {
		t.Fatalf("schema admission empty enum error = %v", err)
	}
}

func TestChannelSchemaYAMLAdmissionRejectsExplicitNullAtEveryBoundary(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		admit func(*testing.T, []byte) error
	}{
		{
			name: "interface",
			body: "interfaces:\n  swarm.hitl-channel:\n    v1:\n      kind: pack_channel\n      schemas:\n        presentation:\n          type: string\n          enum: null\n      operations: {}\n      events: {}\n",
			admit: func(_ *testing.T, body []byte) error {
				var spec runtimecontracts.PlatformSpecDocument
				if err := yaml.Unmarshal(body, &spec); err != nil {
					return err
				}
				_, err := packs.NewInterfaceRegistry(spec)
				return err
			},
		},
		{
			name: "trigger",
			body: "provider: test\nnormalized_events:\n  - event: inbound.test.text\n    fields:\n      text:\n        from: message.text\n        schema:\n          type: string\n          enum: null\n",
			admit: func(_ *testing.T, body []byte) error {
				manifest, err := providertriggers.ParseManifest(body)
				if err != nil {
					return err
				}
				return manifest.Validate()
			},
		},
		{
			name: "channel",
			body: "provider: test\nopaque_types:\n  destination:\n    type: string\n    enum: null\noperations: {}\nevents: {}\n",
			admit: func(t *testing.T, body []byte) error {
				var manifest packs.ChannelManifest
				if err := yaml.Unmarshal(body, &manifest); err != nil {
					return err
				}
				registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
				channel.Manifest = manifest
				_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
				return err
			},
		},
		{
			name: "connector",
			body: "provider: test\ntools:\n  test.send:\n    category: provider_connector\n    handler_type: http\n    effect_class: non_idempotent_write\n    input_schema:\n      type: string\n      enum: null\n    output_schema: {type: object}\n    response_success: {kind: http_status_2xx}\n    http: {method: POST, url: 'https://example.invalid/send'}\n",
			admit: func(_ *testing.T, body []byte) error {
				var manifest providerconnectors.ConnectorManifest
				if err := yaml.Unmarshal(body, &manifest); err != nil {
					return err
				}
				_, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
					Envelope: packs.Envelope{ID: "test.connector"}, Manifest: manifest,
				})
				return err
			},
		},
	}
	for _, tc := range tests {
		for _, form := range []string{"direct", "alias"} {
			t.Run(tc.name+"/"+form, func(t *testing.T) {
				body := tc.body
				if form == "alias" {
					body = "null_anchor: &nil null\n" + strings.Replace(body, "enum: null", "enum: *nil", 1)
				}
				err := tc.admit(t, []byte(body))
				if err == nil || !strings.Contains(err.Error(), `tool schema field "enum" must not be null`) {
					t.Fatalf("YAML admission error = %v, want explicit null schema rejection", err)
				}
			})
		}
	}
}

func TestChannelCompilerPreservesExactEnumAndPinsItInGeneration(t *testing.T) {
	var exact runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: string
minLength: 1
pattern: ' approved $'
enum: [' approved ']
`), &exact); err != nil {
		t.Fatalf("decode exact schema: %v", err)
	}
	compile := func(t *testing.T, schema runtimecontracts.ToolInputSchema) packs.SatisfactionPlan {
		registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
		channel.Manifest.OpaqueTypes["external_account_reference"] = schema
		for _, eventName := range []string{"inbound.telegram.text_message", "inbound.telegram.callback_action"} {
			event := trigger.Events[eventName]
			field := event.Fields["external_account_reference"]
			field.Schema = schema
			event.Fields["external_account_reference"] = field
			trigger.Events[eventName] = event
		}
		plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
		if err != nil {
			t.Fatalf("CompileChannel: %v", err)
		}
		return plan
	}
	original := compile(t, exact)
	for _, eventName := range []string{"text", "action"} {
		eventSchema, ok := original.EventFieldSchema(eventName, "external_account_reference")
		if !ok || eventSchema.Pattern() != " approved $" || channelSchemaEnumText(t, eventSchema) != " approved " {
			t.Fatalf("%s exact schema = %#v", eventName, eventSchema)
		}
	}
	originalGeneration, err := original.Generation()
	if err != nil {
		t.Fatalf("original Generation: %v", err)
	}
	changed := runtimecontracts.MustToolInputSchema(
		runtimecontracts.ToolSchemaString,
		runtimecontracts.ToolSchemaMinLength(1),
		runtimecontracts.ToolSchemaPattern(" accepted $"),
		runtimecontracts.ToolSchemaEnum(" accepted "),
	)
	changedGeneration, err := compile(t, changed).Generation()
	if err != nil {
		t.Fatalf("changed Generation: %v", err)
	}
	if changedGeneration.Equal(originalGeneration) {
		t.Fatal("exact enum/pattern change did not change compiled generation")
	}
}

func channelSchemaEnumText(t *testing.T, schema runtimecontracts.ToolInputSchema) string {
	t.Helper()
	values, declared := schema.EnumValues()
	if !declared || len(values) == 0 {
		t.Fatal("schema enum is missing")
	}
	value, ok := values[0].Interface().(string)
	if !ok {
		t.Fatalf("schema enum = %#v, want string", values[0].Interface())
	}
	return value
}

func TestTelegramChannelPackCompilesThroughAcceptedProductionInventories(t *testing.T) {
	plan := loadTelegramChannelPlan(t)

	wantMax := map[string]int{
		"presentation.text": 4096,
		"actions":           8,
		"actions[].label":   64,
		"actions[].token":   64,
	}
	for name, want := range wantMax {
		schema, ok := plan.Constraint(name)
		if !ok {
			t.Fatalf("constraint %q missing", name)
		}
		got, declared := schema.MaxLength()
		if name == "actions" {
			got, declared = schema.MaxItems()
		}
		if !declared || got != want {
			t.Fatalf("constraint %q max = %v declared=%v, want %d", name, got, declared, want)
		}
	}

	prepared, err := plan.PrepareOperationInput("deliver", map[string]any{
		"presentation": map[string]any{"text": "Review launch"},
		"actions": []any{
			map[string]any{"label": "Approve", "token": "approve_1"},
			map[string]any{"label": "Reject", "token": "reject_1"},
		},
	}, map[string]any{"destination": "-100123"})
	if err != nil {
		t.Fatalf("PrepareOperationInput(deliver): %v", err)
	}
	keyboard := prepared["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
	if len(keyboard) != 2 {
		t.Fatalf("inline keyboard = %#v, want two ordered rows", keyboard)
	}

	cleared, err := plan.PrepareOperationInput("edit", map[string]any{
		"delivery_reference": map[string]any{"id": float64(42)},
		"presentation":       map[string]any{"text": "Already decided"},
		"actions":            []any{},
	}, map[string]any{"destination": "-100123"})
	if err != nil {
		t.Fatalf("PrepareOperationInput(edit clear): %v", err)
	}
	clearedKeyboard := cleared["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
	if len(clearedKeyboard) != 0 {
		t.Fatalf("cleared inline keyboard = %#v, want empty", clearedKeyboard)
	}
	if got := cleared["message_id"]; got != float64(42) {
		t.Fatalf("projected message id = %#v, want bounded integer identity", got)
	}

	if _, err := packs.NewOutboundBindingPlan("telegram_ops", plan, "not-a-chat", nil); err == nil {
		t.Fatal("NewOutboundBindingPlan accepted invalid provider-owned destination")
	}
	binding, err := packs.NewOutboundBindingPlan("telegram_ops", plan, "-100123", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	if subject, err := plan.CapabilitySubject(); err != nil || subject.Kind != packs.SubjectChannelPack || subject.Status != packs.StatusAvailable {
		t.Fatalf("channel pack subject = %#v, err=%v", subject, err)
	}
	if subject, err := binding.CapabilitySubject(); err != nil || subject.Kind != packs.SubjectChannelOutbound || subject.Status != packs.StatusReady {
		t.Fatalf("channel outbound subject = %#v, err=%v", subject, err)
	}
}

func TestCompileChannelActivationsRejectsDeclaredLearnedCollisionAndContradiction(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	target := "ingress:.:telegram-ingress:telegram"
	declaredPlan, err := packs.NewOutboundBindingPlanWithRegistration(
		"declared_ops", plan, "-100123", nil,
		map[string]string{"telegram_bot_token": "telegram.provider", "webhook_signing_secret": "telegram.signing"}, target,
	)
	if err != nil {
		t.Fatal(err)
	}
	learnedPlan, err := packs.NewOutboundBindingPlanWithRegistration(
		"learned_ops", plan, "-100456", nil,
		map[string]string{"telegram_bot_token": "telegram.provider", "webhook_signing_secret": "telegram.signing"}, target,
	)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), BundleSource: "persisted",
		BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
		RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1,
	}
	admissions := []channelonboarding.CredentialAdmission{
		{Role: "telegram_bot_token", StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "provider-receipt", Epoch: "provider-epoch"},
		{Role: "webhook_signing_secret", StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "signing-receipt", Epoch: "signing-epoch"},
	}
	for _, plans := range [][2]packs.OutboundBindingPlan{{declaredPlan, learnedPlan}, {learnedPlan, declaredPlan}} {
		declared := channelonboarding.CompiledActivation{Source: channelonboarding.ActivationSourceDeclared, Coordinate: coordinate, Plan: plans[0], CredentialAdmissions: admissions}
		learned := channelonboarding.CompiledActivation{Source: channelonboarding.ActivationSourceLearned, OnboardingOperationID: "collision-operation", OnboardingRevision: 1, Coordinate: coordinate, ActivationRevision: 1, Plan: plans[1], CredentialAdmissions: admissions}
		if _, err := channelonboarding.MergeCompiledActivations([]channelonboarding.CompiledActivation{declared}, []channelonboarding.CompiledActivation{learned}); err == nil || !strings.Contains(err.Error(), "target collision") {
			t.Fatalf("declared/learned target collision error = %v", err)
		}
	}
}

func TestChannelActivationPublicationGenerationRetainsCompleteNonSecretProvenance(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	planGeneration, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	newBinding := func(destination string) packs.OutboundBindingPlan {
		binding, bindingErr := packs.NewOutboundBindingPlanWithRegistration(
			"telegram_ops", plan, destination, nil,
			map[string]string{"telegram_bot_token": "telegram.provider", "webhook_signing_secret": "telegram.signing"},
			"ingress:.:telegram-ingress:telegram",
		)
		if bindingErr != nil {
			t.Fatal(bindingErr)
		}
		return binding
	}
	base := channelonboarding.CompiledActivation{
		Source: channelonboarding.ActivationSourceDeclared,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), BundleSource: "persisted",
			BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
			RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
			ContextPublicationGeneration: 7, PlanGeneration: planGeneration, TargetGeneration: 3,
		},
		Plan: newBinding("-100123"),
		CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: "telegram_bot_token", StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "provider-receipt", Epoch: "provider-epoch"},
			{Role: "webhook_signing_secret", StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "signing-receipt", Epoch: "signing-epoch"},
		},
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{base})
	if err != nil {
		t.Fatal(err)
	}
	declaredOnly, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication([]packs.OutboundBindingPlan{base.Plan})
	if err != nil {
		t.Fatal(err)
	}
	if declaredOnly.Executable() || len(declaredOnly.Activations()) != 0 || len(declaredOnly.Bindings()) != 1 || declaredOnly.Bindings()[0].BindingID() != base.Plan.BindingID() {
		t.Fatalf("declared-only publication leaked execution or lost configured plan: %#v", declaredOnly)
	}
	changedDeclaredOnly, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication([]packs.OutboundBindingPlan{newBinding("-100456")})
	if err != nil {
		t.Fatal(err)
	}
	if declaredOnly.Generation().Equal(changedDeclaredOnly.Generation()) {
		t.Fatal("declared-only publication ignored behavior-bearing destination change")
	}
	if _, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication([]packs.OutboundBindingPlan{base.Plan, base.Plan}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate declared-only publication error = %v", err)
	}
	reordered := base
	reordered.CredentialAdmissions = []channelonboarding.CredentialAdmission{base.CredentialAdmissions[1], base.CredentialAdmissions[0]}
	repeated, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{reordered})
	if err != nil || !publication.Generation().Equal(repeated.Generation()) {
		t.Fatalf("deterministic publication generation = %s/%s err=%v", publication.Generation().Diagnostic(), repeated.Generation().Diagnostic(), err)
	}

	mutations := map[string]func(channelonboarding.CompiledActivation) channelonboarding.CompiledActivation{
		"source and revision": func(value channelonboarding.CompiledActivation) channelonboarding.CompiledActivation {
			value.Source = channelonboarding.ActivationSourceLearned
			value.OnboardingOperationID = "publication-provenance-operation"
			value.OnboardingRevision = 1
			value.ActivationRevision = 1
			return value
		},
		"runtime context": func(value channelonboarding.CompiledActivation) channelonboarding.CompiledActivation {
			value.Coordinate.ContextPublicationGeneration++
			return value
		},
		"destination": func(value channelonboarding.CompiledActivation) channelonboarding.CompiledActivation {
			value.Plan = newBinding("-100456")
			return value
		},
		"credential epoch": func(value channelonboarding.CompiledActivation) channelonboarding.CompiledActivation {
			value.CredentialAdmissions = append([]channelonboarding.CredentialAdmission(nil), value.CredentialAdmissions...)
			value.CredentialAdmissions[0].Epoch = "provider-epoch-2"
			return value
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed, changedErr := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{mutate(base)})
			if changedErr != nil {
				t.Fatal(changedErr)
			}
			if publication.Generation().Equal(changed.Generation()) {
				t.Fatalf("%s did not change publication generation", name)
			}
		})
	}

	incomplete := base
	incomplete.CredentialAdmissions = incomplete.CredentialAdmissions[:1]
	if _, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{incomplete}); err == nil || !strings.Contains(err.Error(), "missing credential admission") {
		t.Fatalf("incomplete publication error = %v", err)
	}
	if strings.Contains(publication.Generation().Diagnostic(), "provider-secret") {
		t.Fatal("publication identity exposed credential material")
	}
}

func TestTelegramChannelRejectsOutOfRangeDeliveryReferenceBeforeConnectorProjection(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	if _, err := plan.PrepareOperationInput("edit", map[string]any{
		"delivery_reference": map[string]any{"id": float64(2147483648)},
		"presentation":       map[string]any{"text": "Outside Telegram message id range"},
		"actions":            []any{},
	}, map[string]any{"destination": "-100123"}); err == nil || !strings.Contains(err.Error(), "must be <=") {
		t.Fatalf("PrepareOperationInput accepted out-of-range delivery reference: %v", err)
	}
}

func TestTelegramChannelCompilerRejectsUnboundedMessageIDOutput(t *testing.T) {
	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	tool := connector.Tools["telegram.send_interactive"]
	allow := false
	output := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
		"message_id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger, runtimecontracts.ToolSchemaMinimum(1)),
	}), runtimecontracts.ToolSchemaRequired("message_id"), runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(allow))
	var err error
	tool, err = tool.WithSchemas(tool.InputSchema(), output)
	if err != nil {
		t.Fatalf("replace output schema: %v", err)
	}
	connector.Tools["telegram.send_interactive"] = tool

	_, err = packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), "source maximum is broader than target maximum") {
		t.Fatalf("CompileChannel error = %v, want unbounded provider result rejection", err)
	}
}

func TestTelegramChannelCompilerConsumesExactNormalizedIdentifierSchemas(t *testing.T) {
	tests := []struct {
		name  string
		event string
		field string
		widen func(runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema
		want  string
	}{
		{name: "text message", event: "inbound.telegram.text_message", field: "provider_message_reference", widen: withoutMaximum, want: "source maximum is broader"},
		{name: "text account", event: "inbound.telegram.text_message", field: "external_account_reference", widen: withoutPattern, want: "not provably assignable"},
		{name: "text conversation", event: "inbound.telegram.text_message", field: "conversation_reference", widen: withoutMaximumLength, want: "source maximum is broader"},
		{name: "callback message", event: "inbound.telegram.callback_action", field: "provider_message_reference", widen: withoutMaximum, want: "source maximum is broader"},
		{name: "callback account", event: "inbound.telegram.callback_action", field: "external_account_reference", widen: withoutPattern, want: "not provably assignable"},
		{name: "callback conversation", event: "inbound.telegram.callback_action", field: "conversation_reference", widen: withoutMaximumLength, want: "source maximum is broader"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
			event := trigger.Events[tc.event]
			field := event.Fields[tc.field]
			field.Schema = tc.widen(field.Schema)
			event.Fields[tc.field] = field
			trigger.Events[tc.event] = event
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want %q", err, tc.want)
			}
		})
	}
}

func withoutMaximum(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return schema.WithoutMaximum()
}

func withoutPattern(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return schema.WithoutPattern()
}

func withoutMaximumLength(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return schema.WithoutMaxLength()
}

func TestProductionCompilerAcceptsStructurallyDifferentTighterSatisfier(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(mock): %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"mock", "mock.text", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation,
	)
	fact, matched, err := plan.ProjectTextFact("mock.text", authorization, map[string]any{
		"text": "SWARM-AAAAAAAAAAAAAAAA", "principal": "operator-a", "room": "ops-room", "scope": "shared", "message_ref": "mock-delivery:12345678",
	})
	if err != nil || !matched {
		t.Fatalf("ProjectTextFact(mock) = %#v matched=%v err=%v", fact, matched, err)
	}
	if fact.ExternalAccountRef != `{"principal":"operator-a"}` || fact.ConversationRef != `{"room":"ops-room"}` || fact.ConversationScope != operatorchannel.ConversationScopeShared {
		t.Fatalf("mock operator-channel fact = %#v", fact)
	}
	wantMax := map[string]int{
		"presentation.text": 128,
		"actions":           2,
		"actions[].label":   24,
		"actions[].token":   20,
	}
	for name, want := range wantMax {
		schema, ok := plan.Constraint(name)
		if !ok {
			t.Fatalf("constraint %q missing", name)
		}
		got, declared := schema.MaxLength()
		if name == "actions" {
			got, declared = schema.MaxItems()
		}
		if !declared || got != want {
			t.Fatalf("mock constraint %q max = %v declared=%v, want %d", name, got, declared, want)
		}
	}
	if _, err := packs.NewOutboundBindingPlan("mock_ops", plan, "queue-a", nil); err == nil {
		t.Fatal("mock binding accepted Telegram-shaped scalar destination")
	}
	binding, err := packs.NewOutboundBindingPlan("mock_ops", plan, map[string]any{"queue": "queue-a"}, nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan(mock): %v", err)
	}
	_, prepared, err := binding.PrepareOperation("acknowledge_interaction", map[string]any{
		"interaction_reference": map[string]any{"cursor": "cursor-a"},
	})
	if err != nil {
		t.Fatalf("PrepareOperation(mock acknowledge): %v", err)
	}
	if _, hasDestination := prepared["destination"]; hasDestination {
		t.Fatalf("acknowledgment gained ambient destination context: %#v", prepared)
	}
}

func TestChannelRegistrationCompilerIsProviderNeutralAcrossTelegramAndDifferentialMock(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(mock registration): %v", err)
	}
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("mock registration plan is missing")
	}
	if registration.Provider() != "mock" || registration.SlotNamespace() != "workspace_webhook" || registration.SigningCredential() != "mock_callback_key" {
		t.Fatalf("mock registration identity = provider=%q namespace=%q signing=%q", registration.Provider(), registration.SlotNamespace(), registration.SigningCredential())
	}
	identify, err := registration.Operation(packs.RegistrationOperationIdentify)
	if err != nil {
		t.Fatalf("identify operation: %v", err)
	}
	identified, err := identify.Project(map[string]any{"workspace": map[string]any{"key": "mock-workspace:alpha"}})
	if err != nil || identified["resource_id"] != "mock-workspace:alpha" {
		t.Fatalf("identify projection = %#v, %v", identified, err)
	}
	apply, err := registration.Operation(packs.RegistrationOperationApply)
	if err != nil {
		t.Fatalf("apply operation: %v", err)
	}
	input, err := apply.Prepare(map[string]any{"callback_url": "https://hooks.example.test/webhooks/support/mock?swarm_callback_generation=opaque"})
	if err != nil {
		t.Fatalf("apply projection: %v", err)
	}
	endpoint, ok := input["endpoint"].(map[string]any)
	if !ok || endpoint["callback"] == "" {
		t.Fatalf("apply input = %#v, want nested endpoint.callback", input)
	}
	readback, err := registration.Operation(packs.RegistrationOperationReadback)
	if err != nil {
		t.Fatalf("readback operation: %v", err)
	}
	projected, err := readback.Project(map[string]any{"registration": map[string]any{"callback": endpoint["callback"]}})
	if err != nil || projected["callback_url"] != endpoint["callback"] {
		t.Fatalf("readback projection = %#v, %v", projected, err)
	}
}

func TestTelegramOnboardingProfileCompilesAsWebhookTextChallenge(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	profile, ok := plan.OnboardingProfile()
	if !ok {
		t.Fatal("Telegram onboarding profile is missing")
	}
	if profile.Provider() != "telegram" || profile.ActivationPosture() != packs.ChannelActivationWebhookRegistration || profile.IdentityCeremony() != packs.ChannelCeremonyAuthenticatedTextChallenge {
		t.Fatalf("Telegram onboarding profile = %#v", profile)
	}
	if profile.ProviderCredential() != "telegram_bot_token" || profile.SigningCredential() != "webhook_signing_secret" || profile.ConfirmationOperation() != "deliver" {
		t.Fatalf("Telegram onboarding credentials/confirmation = %#v", profile)
	}
}

func TestLearnedActivationCompilationUsesDurableConversationDestination(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	profile, ok := plan.OnboardingProfile()
	if !ok {
		t.Fatal("Telegram onboarding profile is missing")
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), BundleSource: "persisted",
		BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
		RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1,
	}
	target := "ingress:.:telegram-ingress:telegram"
	candidate := channelonboarding.Candidate{
		Provider: profile.Provider(), Interface: identity, Coordinate: coordinate,
		Target: channelonboarding.CandidateTarget{
			Selector: target, ServiceID: "telegram-ingress", PackageKey: ".", FlowID: "telegram-ingress",
			Alias: "telegram", Provider: profile.Provider(), Generation: 1, PublicationSequence: 1,
			AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("telegram-admission")), SigningCredentialKey: "telegram.signing",
		},
		Posture: channelonboarding.ActivationWebhookRegistration, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
		ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(),
		ConfirmationOperation: profile.ConfirmationOperation(), Plan: plan,
	}
	activation := channelonboarding.ConnectedChannelActivation{
		ActivationID: uuid.NewString(), SlotKey: "slot-a", OperationID: uuid.NewString(), OperationRevision: 8,
		PrincipalID: "principal-a", Provider: candidate.Provider, Interface: candidate.Interface,
		Coordinate: candidate.Coordinate, TargetSelector: target, Posture: candidate.Posture,
		BindingRevision: 3, ConversationRef: "-100123",
		CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: profile.ProviderCredential(), StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "provider-receipt", Epoch: "provider-epoch"},
			{Role: profile.SigningCredential(), StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, Receipt: "signing-receipt", Epoch: "signing-epoch"},
		},
		Revision: 1, Status: channelonboarding.ActivationCurrent,
	}

	compiled, err := channelonboarding.CompileLearnedActivation(candidate, activation)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.Plan.Destination().Interface(); got != activation.ConversationRef {
		t.Fatalf("compiled destination = %q, want durable predecessor %q", got, activation.ConversationRef)
	}

	activation.ConversationRef = ""
	if _, err := channelonboarding.CompileLearnedActivation(candidate, activation); err == nil || !strings.Contains(err.Error(), "conversation") {
		t.Fatalf("missing durable destination error = %v", err)
	}
}

func TestChannelOnboardingProfileAxesAreProviderNeutral(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profile  packs.ChannelOnboardingProfile
		posture  packs.ChannelActivationPosture
		ceremony packs.ChannelIdentityCeremony
	}{
		{
			name:    "discord webhook text",
			profile: packs.ChannelOnboardingProfile{Activation: "webhook_registration", Ceremony: "authenticated_text_challenge", ProviderCredentialRole: "discord_app_token", SigningCredentialRole: "discord_signature_key", Confirmation: "deliver"},
			posture: packs.ChannelActivationWebhookRegistration, ceremony: packs.ChannelCeremonyAuthenticatedTextChallenge,
		},
		{
			name:    "whatsapp session pairing",
			profile: packs.ChannelOnboardingProfile{Activation: "session_connection", Ceremony: "provider_pairing", ProviderCredentialRole: "whatsapp_session", Confirmation: "deliver", ConnectionHealth: "bridge_connection"},
			posture: packs.ChannelActivationSessionConnection, ceremony: packs.ChannelCeremonyProviderPairing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := packs.CompileChannelOnboardingProfile("paper-port", tc.profile, []string{"deliver"})
			if err != nil {
				t.Fatalf("CompileChannelOnboardingProfile: %v", err)
			}
			if compiled.ActivationPosture() != tc.posture || compiled.IdentityCeremony() != tc.ceremony {
				t.Fatalf("compiled axes = %#v", compiled)
			}
		})
	}
}

type mockRegistrationEffectStore struct{ *effecttest.Harness }

func (s mockRegistrationEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}

type mockRegistrationTransport struct {
	mu            sync.Mutex
	identifyCount int
	applyCount    int
	currentURL    string
	readbackURL   string
	readbackErr   error
	loseAck       bool
}

func (t *mockRegistrationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/v2/workspace":
		t.identifyCount++
		return mockRegistrationResponse(http.StatusOK, `{"data":{"workspace":{"key":"mock-workspace:alpha"}}}`), nil
	case request.Method == http.MethodPut && request.URL.Path == "/v2/callback":
		var payload struct {
			Subscription struct {
				Endpoint string `json:"endpoint"`
				Proof    string `json:"proof"`
			} `json:"subscription"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			return nil, err
		}
		if payload.Subscription.Endpoint == "" || payload.Subscription.Proof == "" || request.Header.Get("X-Mock-Key") == "" {
			return nil, errors.New("mock registration request omitted admitted credentials or endpoint")
		}
		t.currentURL = payload.Subscription.Endpoint
		t.applyCount++
		if t.loseAck {
			return nil, fmt.Errorf("injected acknowledgment loss for %s %s", request.Header.Get("X-Mock-Key"), payload.Subscription.Proof)
		}
		return mockRegistrationResponse(http.StatusOK, `{"result":{"accepted":true}}`), nil
	case request.Method == http.MethodGet && request.URL.Path == "/v2/callback":
		if t.readbackErr != nil {
			return nil, t.readbackErr
		}
		callback := t.currentURL
		if t.readbackURL != "" {
			callback = t.readbackURL
		}
		body, err := json.Marshal(map[string]any{"data": map[string]any{"subscription": map[string]any{"endpoint": callback}}})
		if err != nil {
			return nil, err
		}
		return mockRegistrationResponse(http.StatusOK, string(body)), nil
	default:
		return nil, fmt.Errorf("unexpected mock registration request %s %s", request.Method, request.URL)
	}
}

func (t *mockRegistrationTransport) counts() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.identifyCount, t.applyCount
}

func mockRegistrationStartupGrant(t *testing.T, owner string) startupownership.GrantEvidence {
	t.Helper()
	grant := startupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: owner,
		ProcessBootID: uuid.NewString(), BundleHash: "bundle-v1:sha256:" + strings.Repeat("d", 64), BundleSource: "ephemeral",
		RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1, SourceSetRevision: "mock-registration-source-set",
		StateVersion: 3, State: startupownership.GrantAdmitted,
	}
	if err := grant.Validate(); err != nil {
		t.Fatalf("mock registration startup grant: %v", err)
	}
	return grant
}

func TestDifferentialMockRegistrationExecutesThroughProviderNeutralLifecycle(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel: %v", err)
	}
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("compiled mock registration is missing")
	}
	planGeneration, err := plan.Generation()
	if err != nil {
		t.Fatalf("plan generation: %v", err)
	}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range map[string]string{"api": "mock-api-v1", "signing": "mock-proof-v1"} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	snapshots, err := runtimecredentials.NewSnapshotOwner(credentialStore)
	if err != nil {
		t.Fatalf("NewSnapshotOwner: %v", err)
	}
	startup := mockRegistrationStartupGrant(t, "mock-serve")
	transport := &mockRegistrationTransport{}
	readiness := runtimepublicingress.NewReadinessOwner(true)
	controller, err := runtimepublicingress.NewProviderRegistrationController(runtimepublicingress.RegistrationControllerOptions{
		CredentialOwner:   snapshots,
		EffectsStore:      mockRegistrationEffectStore{Harness: effecttest.New()},
		HTTP:              runtimeregistration.HTTPExecutor{Client: &http.Client{Transport: transport}},
		Posture:           executionposture.Live,
		RuntimeInstanceID: uuid.NewString(),
		StartupAuthority:  func() (startupownership.GrantEvidence, error) { return startup, nil },
		Readiness:         readiness,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistrationController: %v", err)
	}
	exposure := runtimepublicingress.Generation{ID: uuid.NewString(), Mode: runtimepublicingress.ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: "127.0.0.1:8443", CreatedAt: time.Now().UTC()}
	readiness.SetRuntimeReady(true)
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{
		GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
		StartupAuthorityID: startup.GrantID, ObservedAt: exposure.CreatedAt, ExpiresAt: exposure.CreatedAt.Add(runtimepublicingress.EvidenceTTL),
	})
	onboardingID := uuid.NewString()
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("e", 64), BundleSource: "persisted",
		BundleIdentity: "bundle:test@sha256:mock-registration", PackInventoryGeneration: "sha256:mock-registration-inventory",
		RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1,
		PlanGeneration: planGeneration, TargetGeneration: 1,
	}
	pair := runtimepublicingress.RegistrationPair{
		BindingID: "mock-hitl", PlanGeneration: planGeneration, OnboardingOperationID: onboardingID, OnboardingRevision: 1,
		OnboardingCoordinate: coordinate, PrebindingOperationID: onboardingID, Registration: registration,
		CredentialKeys: map[string]string{"mock_api_key": "api"},
		Target: runtimepublicingress.RegistrationTarget{
			Selector: "ingress:support:mock:mock", BundleHash: "bundle-v1:sha256:" + strings.Repeat("e", 64),
			ServiceID: uuid.NewString(), PackageKey: "support", FlowID: "mock", Alias: "support", Provider: "mock",
			Generation: 1, PublicationSequence: 1, AdmissionPlanGeneration: triggergeneration.FromCanonicalBytes([]byte("mock-admission")),
			SigningCredentialKey: "signing",
		},
	}
	other := pair
	other.BindingID = "mock-alerts"
	other.Target.Selector = "ingress:alerts:mock:mock"
	other.Target.PackageKey = "alerts"
	other.Target.Alias = "alerts"
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair, other}); err == nil || !strings.Contains(err.Error(), "selected by both") {
		t.Fatalf("slot collision error = %v", err)
	}
	if identified, applied := transport.counts(); identified != 2 || applied != 0 {
		t.Fatalf("collision identify/apply counts = %d/%d, want 2/0", identified, applied)
	}

	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("known-success reconcile: %v", err)
	}
	first := readiness.Snapshot(time.Now().UTC())
	if len(first.Registrations) != 1 || !first.Registrations[0].Applied || !first.Registrations[0].CallbackMatched {
		t.Fatalf("known-success readiness = %#v", first)
	}
	firstCallback := first.Registrations[0].CallbackURL
	if _, applied := transport.counts(); applied != 1 {
		t.Fatalf("known-success apply count = %d, want 1", applied)
	}

	startup = mockRegistrationStartupGrant(t, "mock-serve-successor")
	readiness.SetExposure(runtimepublicingress.ExposureEvidence{
		GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
		StartupAuthorityID: startup.GrantID, ObservedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(runtimepublicingress.EvidenceTTL),
	})
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("unchanged handoff reconcile: %v", err)
	}
	if _, applied := transport.counts(); applied != 1 {
		t.Fatalf("startup handoff performed another apply: %d", applied)
	}

	if err := credentialStore.Set(context.Background(), "signing", "mock-proof-v2"); err != nil {
		t.Fatalf("rotate signing credential: %v", err)
	}
	transport.mu.Lock()
	transport.loseAck = true
	transport.readbackURL = "https://hooks.example.test/stale"
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil || strings.Contains(err.Error(), "mock-proof-v2") {
		t.Fatalf("ack-loss mismatch error = %v", err)
	}
	if _, applied := transport.counts(); applied != 2 {
		t.Fatalf("rotated acknowledgment-loss apply count = %d, want 2", applied)
	}
	transport.mu.Lock()
	transport.loseAck = false
	transport.readbackURL = ""
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("same-base uncertain reconcile: %v", err)
	}
	if _, applied := transport.counts(); applied != 2 {
		t.Fatalf("mismatched readback resent apply: %d", applied)
	}
	uncertain := readiness.Snapshot(time.Now().UTC())
	if uncertain.PublicIngressReady || len(uncertain.Registrations) != 1 || uncertain.Registrations[0].Phase != "outcome_uncertain" {
		t.Fatalf("mismatched mock registration = %#v", uncertain)
	}

	if err := credentialStore.Set(context.Background(), "signing", "mock-proof-v3"); err != nil {
		t.Fatalf("rotate signing credential for unavailable readback: %v", err)
	}
	transport.mu.Lock()
	transport.loseAck = true
	transport.readbackErr = errors.New("mock readback unavailable")
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err == nil {
		t.Fatal("unavailable mock readback returned nil")
	}
	if _, applied := transport.counts(); applied != 3 {
		t.Fatalf("unavailable readback apply count = %d, want 3", applied)
	}
	unavailable := readiness.Snapshot(time.Now().UTC())
	if unavailable.PublicIngressReady || unavailable.Registrations[0].Phase != "outcome_uncertain" {
		t.Fatalf("unavailable mock registration = %#v", unavailable)
	}
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("same-base unavailable reconcile: %v", err)
	}
	if _, applied := transport.counts(); applied != 3 {
		t.Fatalf("unavailable readback resent apply: %d", applied)
	}

	if err := credentialStore.Set(context.Background(), "signing", "mock-proof-v4"); err != nil {
		t.Fatalf("rotate signing credential for fresh intent: %v", err)
	}
	transport.mu.Lock()
	transport.loseAck = false
	transport.readbackErr = nil
	transport.mu.Unlock()
	if err := controller.Reconcile(context.Background(), exposure, []runtimepublicingress.RegistrationPair{pair}); err != nil {
		t.Fatalf("fresh semantic intent reconcile: %v", err)
	}
	if _, applied := transport.counts(); applied != 4 {
		t.Fatalf("fresh semantic intent apply count = %d, want 4", applied)
	}
	rotated := readiness.Snapshot(time.Now().UTC())
	if len(rotated.Registrations) != 1 || rotated.Registrations[0].CallbackURL == firstCallback || !rotated.Registrations[0].CallbackMatched {
		t.Fatalf("rotated readiness = %#v", rotated)
	}

	var admitted int
	handler := controller.Handler(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		admitted++
		response.WriteHeader(http.StatusNoContent)
	}))
	staleRecorder := httptest.NewRecorder()
	handler.ServeHTTP(staleRecorder, httptest.NewRequest(http.MethodPost, firstCallback, nil))
	currentRecorder := httptest.NewRecorder()
	handler.ServeHTTP(currentRecorder, httptest.NewRequest(http.MethodPost, rotated.Registrations[0].CallbackURL, nil))
	if staleRecorder.Code != http.StatusNotFound || currentRecorder.Code != http.StatusNoContent || admitted != 1 {
		t.Fatalf("callback fencing stale/current/admitted = %d/%d/%d", staleRecorder.Code, currentRecorder.Code, admitted)
	}

	readiness.SetExposure(runtimepublicingress.ExposureEvidence{
		GenerationID: exposure.ID, Mode: exposure.Mode, PublicOrigin: exposure.PublicOrigin, ListenAddress: exposure.ListenAddress,
		StartupAuthorityID: startup.GrantID, ObservedAt: time.Now().UTC(), ExpiresAt: rotated.Registrations[0].ExpiresAt.Add(time.Minute),
	})
	if snapshot := readiness.Snapshot(rotated.Registrations[0].ExpiresAt); snapshot.Ready || snapshot.PublicIngressReady {
		t.Fatalf("expired registration remained ready: %#v", snapshot)
	}
}

func mockRegistrationResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestProductionCompilerFailsClosedAcrossChannelContractPhases(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	tests := []struct {
		name   string
		mutate func(*packs.LoadedChannelPack, *packs.TriggerPackDescriptor, *packs.ConnectorPackDescriptor)
		want   string
	}{
		{
			name: "incomplete operation surface",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.Operations, "edit")
			},
			want: "channel operations",
		},
		{
			name: "unknown operation",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				channel.Manifest.Operations["delete"] = packs.ChannelOperationBinding{Tool: "mock.edit"}
			},
			want: "channel operations",
		},
		{
			name: "missing opaque slot",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.OpaqueTypes, "conversation_reference")
			},
			want: "channel opaque_types",
		},
		{
			name: "effect mismatch",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				tool = mustToolWithEffect(tool, runtimecontracts.ActivityEffectClassReadOnly)
				connector.Tools["mock.deliver"] = tool
			},
			want: "effect class does not match",
		},
		{
			name: "unconsumed interface input",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.Operations["deliver"].Input, "body")
			},
			want: `selected channel constraint "presentation.text" is not mapped`,
		},
		{
			name: "incompatible selected patterns",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.edit"]
				controls := mockArraySchema(0, 2, mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"name": mockStringSchema(1, 24, ""), "value": mockStringSchema(1, 20, `^[A-Z]+$`),
				}, "name", "value"))
				input := mustSchemaWithProperty(tool.InputSchema(), "controls", controls)
				tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
				connector.Tools["mock.edit"] = tool
			},
			want: "incompatible patterns",
		},
		{
			name: "missing finite text maximum",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				for _, name := range []string{"mock.deliver", "mock.edit"} {
					tool := connector.Tools[name]
					input := tool.InputSchema()
					body, ok := input.Property("body")
					if !ok {
						panic("mock body schema is missing")
					}
					input = mustSchemaWithProperty(input, "body", body.WithoutMaxLength())
					tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
					connector.Tools[name] = tool
				}
			},
			want: "finite maxLength",
		},
		{
			name: "event field type mismatch",
			mutate: func(_ *packs.LoadedChannelPack, trigger *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				event := trigger.Events["mock.text"]
				event.Fields["text"] = packs.TriggerEventField{Schema: runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("integer")), Required: true}
				trigger.Events["mock.text"] = event
			},
			want: "incompatible types",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			tc.mutate(&channel, &trigger, &connector)
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestProductionCompilerRejectsPartialRequiredConnectorObject(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	tool := connector.Tools["mock.deliver"]
	input := tool.InputSchema()
	destination, _ := input.Property("destination")
	queue, _ := destination.Property("queue")
	destination = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"queue": queue, "region": mockStringSchema(1, 10, ""),
	}, "queue", "region")
	input = mustSchemaWithProperty(input, "destination", destination)
	tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
	connector.Tools["mock.deliver"] = tool

	_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), `required path "destination.region" is covered 0 times`) {
		t.Fatalf("CompileChannel error = %v, want missing destination.region", err)
	}
}

func TestProductionCompilerRejectsDroppedRequiredInterfaceArrayLeaf(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	tool := connector.Tools["mock.deliver"]
	input := tool.InputSchema()
	controls, _ := input.Property("controls")
	controls = mustSchemaWithItems(controls, mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"name": mockStringSchema(1, 24, ""),
	}, "name"))
	input = mustSchemaWithProperty(input, "controls", controls)
	tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
	connector.Tools["mock.deliver"] = tool
	binding := channel.Manifest.Operations["deliver"]
	binding.Input["controls"] = packs.ChannelMapping{
		Each: "input.actions",
		Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}}},
	}
	channel.Manifest.Operations["deliver"] = binding

	_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), `selected channel constraint "actions[].token" is not mapped`) {
		t.Fatalf("CompileChannel error = %v, want unconsumed actions token", err)
	}
}

func TestProductionCompilerRejectsRecursiveDirectionalAndCardinalityGaps(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	tests := []struct {
		name   string
		mutate func(*packs.LoadedChannelPack, *packs.ConnectorPackDescriptor)
		want   string
	}{
		{
			name: "input scalar source broader than target",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				input := tool.InputSchema()
				destination, _ := input.Property("destination")
				queue, _ := destination.Property("queue")
				queue = mustSchemaWithMaxLength(queue, 5)
				destination = mustSchemaWithProperty(destination, "queue", queue)
				input = mustSchemaWithProperty(input, "destination", destination)
				tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
				connector.Tools["mock.deliver"] = tool
				channel.Manifest.OpaqueTypes["destination"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`),
				}, "queue")
			},
			want: "source maximum is broader than target maximum 5",
		},
		{
			name: "whole input object misses required target child",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				input := tool.InputSchema()
				destination, _ := input.Property("destination")
				destination = mustSchemaWithRequiredProperty(destination, "region", mockStringSchema(1, 10, ""))
				input = mustSchemaWithProperty(input, "destination", destination)
				tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
				connector.Tools["mock.deliver"] = tool
				channel.Manifest.OpaqueTypes["destination"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`),
				}, "queue")
				binding := channel.Manifest.Operations["deliver"]
				delete(binding.Input, "destination.queue")
				binding.Input["destination"] = packs.ChannelMapping{From: "context.destination"}
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `target requires property "region" that source does not require`,
		},
		{
			name: "output scalar source broader than target",
			mutate: func(_ *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				output := tool.OutputSchema()
				ref, _ := output.Property("ref")
				ref = mustSchemaWithLengthBounds(ref, 1, 100)
				output = mustSchemaWithProperty(output, "ref", ref)
				tool = mustToolWithSchemas(tool, tool.InputSchema(), output)
				connector.Tools["mock.deliver"] = tool
			},
			want: "source minimum is broader than target minimum 22",
		},
		{
			name: "whole output object misses required target child",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				receipt := channel.Manifest.OpaqueTypes["delivery_receipt"]
				receipt = mustSchemaWithRequiredProperty(receipt, "status", mockStringSchema(1, 12, ""))
				channel.Manifest.OpaqueTypes["delivery_receipt"] = receipt
				tool := connector.Tools["mock.edit"]
				output := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"receipt": mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger)}, "revision"),
				}, "receipt")
				tool = mustToolWithSchemas(tool, tool.InputSchema(), output)
				connector.Tools["mock.edit"] = tool
				binding := channel.Manifest.Operations["edit"]
				delete(binding.Output, "delivery_receipt.revision")
				binding.Output["delivery_receipt"] = packs.ChannelMapping{From: "result.receipt"}
				channel.Manifest.Operations["edit"] = binding
			},
			want: `target requires property "status" that source does not require`,
		},
		{
			name: "ancestor and descendant target overlap",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.ConnectorPackDescriptor) {
				binding := channel.Manifest.Operations["deliver"]
				binding.Input["destination"] = packs.ChannelMapping{From: "context.destination"}
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `target path "destination.queue" overlaps "destination"`,
		},
		{
			name: "duplicate recursive item source",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				input := tool.InputSchema()
				controls, _ := input.Property("controls")
				items, _ := controls.ItemsSchema()
				items = mustSchemaWithRequiredProperty(items, "alias", mockStringSchema(1, 24, ""))
				controls = mustSchemaWithItems(controls, items)
				input = mustSchemaWithProperty(input, "controls", controls)
				tool = mustToolWithSchemas(tool, input, tool.OutputSchema())
				connector.Tools["mock.deliver"] = tool
				binding := channel.Manifest.Operations["deliver"]
				mapping := binding.Input["controls"]
				mapping.Item[0]["alias"] = packs.ChannelMapping{From: "item.label"}
				binding.Input["controls"] = mapping
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `item source path "label" overlaps "label"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			tc.mutate(&channel, &connector)
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestChannelRuntimeToolsExposeOnlyProviderNeutralContract(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	tools, err := binding.RuntimeTools()
	if err != nil {
		t.Fatalf("RuntimeTools: %v", err)
	}
	tool := tools["channel.ops.deliver"]
	_, hasHTTP := tool.HTTP()
	_, hasManagedCredential := tool.ManagedCredential()
	if tool.Category() != runtimecontracts.ToolCategoryChannelOperation || tool.Handler() != runtimecontracts.ToolHandlerChannel || hasHTTP || len(tool.Credentials()) != 0 || hasManagedCredential {
		t.Fatalf("public channel tool leaked connector execution details: %#v", tool)
	}
	input := tool.InputSchema()
	if _, ok := input.Property("presentation"); !ok {
		t.Fatalf("public channel input = %#v, want presentation", input)
	}
	if _, ok := input.Property("chat_id"); ok {
		t.Fatalf("public channel input exposed connector destination: %#v", input)
	}
	activityTool, err := binding.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool: %v", err)
	}
	target, err := binding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("RuntimeActivityTarget: %v", err)
	}
	_, activityHasHTTP := activityTool.HTTP()
	_, activityHasCompiledResult := activityTool.CompiledResult()
	if target.ToolID() == binding.RuntimeToolID("deliver") || !target.Generation().Valid() || !activityHasHTTP || !activityHasCompiledResult {
		t.Fatalf("private channel activity target is not separated: id=%q generation=%q tool=%#v", target.ToolID(), target.Generation().Diagnostic(), activityTool)
	}
}

func TestSatisfactionPlanReadbackIsImmutableWithoutClone(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	tool, err := plan.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool: %v", err)
	}
	input := tool.InputSchema()
	text, ok := input.Property("text")
	if !ok {
		t.Fatal("Telegram text maxLength missing")
	}
	originalMaximum, declared := text.MaxLength()
	if !declared {
		t.Fatal("Telegram text maxLength missing")
	}
	changedText := mustSchemaWithMaxLength(text, 7)
	changedInput := mustSchemaWithProperty(input, "text", changedText)
	_ = mustToolWithSchemas(tool, changedInput, tool.OutputSchema())

	httpSpec, ok := tool.HTTP()
	if !ok {
		t.Fatal("Telegram HTTP contract missing")
	}
	httpSpec.URL = "https://mutated.invalid"
	httpSpec.Headers["X-Test"] = "mutated"

	originalTool, err := plan.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool readback: %v", err)
	}
	originalText, _ := originalTool.InputSchema().Property("text")
	if got, declared := originalText.MaxLength(); !declared || got != originalMaximum {
		t.Fatalf("schema readback changed after derived-value mutation: max=%d declared=%v, want %d", got, declared, originalMaximum)
	}
	originalHTTP, ok := originalTool.HTTP()
	if !ok || originalHTTP.URL == "https://mutated.invalid" || originalHTTP.Headers["X-Test"] == "mutated" {
		t.Fatalf("HTTP readback changed after snapshot mutation: %#v", originalHTTP)
	}
}

func TestAdmittedToolMutationCannotChangeCompiledPlanGeneration(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel: %v", err)
	}
	beforeGeneration, err := plan.Generation()
	if err != nil {
		t.Fatalf("Generation before mutation: %v", err)
	}
	input := map[string]any{
		"presentation": map[string]any{"text": "hello"},
		"actions":      []any{},
	}
	context := map[string]any{"destination": map[string]any{"queue": "ops"}}
	beforeProjection, err := plan.PrepareOperationInput("deliver", input, context)
	if err != nil {
		t.Fatalf("PrepareOperationInput before mutation: %v", err)
	}

	deliver := channel.Manifest.Operations["deliver"]
	deliver.Input["body"] = packs.ChannelMapping{From: "input.actions"}
	deliver.Output["delivery_reference"] = packs.ChannelMapping{From: "result.mutated"}
	channel.Manifest.Operations["deliver"] = deliver
	connector.Tools["mock.deliver"] = connector.Tools["mock.edit"]
	triggerEvent := trigger.Events["mock.text"]
	triggerEvent.Fields["text"] = packs.TriggerEventField{Schema: runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean), Required: true}
	trigger.Events["mock.text"] = triggerEvent
	toolReadback, err := plan.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool: %v", err)
	}
	resultReadback, ok := toolReadback.CompiledResult()
	if !ok {
		t.Fatal("compiled result readback is missing")
	}
	resultReadback.Fields["delivery_reference"] = runtimecontracts.CompiledResultField{From: "result.mutated"}

	afterGeneration, err := plan.Generation()
	if err != nil {
		t.Fatalf("Generation after mutation: %v", err)
	}
	afterProjection, err := plan.PrepareOperationInput("deliver", input, context)
	if err != nil {
		t.Fatalf("PrepareOperationInput after mutation: %v", err)
	}
	if !beforeGeneration.Equal(afterGeneration) || !reflect.DeepEqual(beforeProjection, afterProjection) {
		t.Fatalf("admitted plan changed after caller mutation: generation=%q/%q projection=%#v/%#v",
			beforeGeneration.Diagnostic(), afterGeneration.Diagnostic(), beforeProjection, afterProjection)
	}
}

func TestRuntimeActivityTargetPinsCompleteCompiledPlanGeneration(t *testing.T) {
	original := loadTelegramChannelPlan(t)
	originalGeneration, err := original.Generation()
	if err != nil {
		t.Fatalf("original Generation: %v", err)
	}

	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	tool := connector.Tools["telegram.send_interactive"]
	input := tool.InputSchema()
	text, ok := input.Property("text")
	if !ok {
		t.Fatal("Telegram connector text schema missing")
	}
	maximum, declared := text.MaxLength()
	if !declared || maximum < 2 {
		t.Fatalf("Telegram connector text schema = %#v", text)
	}
	text = mustSchemaWithMaxLength(text, maximum-1)
	input = mustSchemaWithProperty(input, "text", text)
	connector.Tools["telegram.send_interactive"] = mustToolWithSchemas(tool, input, tool.OutputSchema())
	changed, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(changed): %v", err)
	}
	changedGeneration, err := changed.Generation()
	if err != nil {
		t.Fatalf("changed Generation: %v", err)
	}
	if changedGeneration.Equal(originalGeneration) {
		t.Fatal("compiled schema change did not change the plan generation")
	}

	originalBinding, err := packs.NewOutboundBindingPlan("ops", original, "42", nil)
	if err != nil {
		t.Fatalf("original binding: %v", err)
	}
	changedBinding, err := packs.NewOutboundBindingPlan("ops", changed, "42", nil)
	if err != nil {
		t.Fatalf("changed binding: %v", err)
	}
	originalTarget, err := originalBinding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("original target: %v", err)
	}
	changedTarget, err := changedBinding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("changed target: %v", err)
	}
	if originalTarget.ToolID() == changedTarget.ToolID() || originalTarget.Generation().Equal(changedTarget.Generation()) {
		t.Fatalf("replacement plan reused private target: original=(%q,%q) changed=(%q,%q)",
			originalTarget.ToolID(), originalTarget.Generation().Diagnostic(),
			changedTarget.ToolID(), changedTarget.Generation().Diagnostic())
	}
}

func TestToolExecutionContractHasOneAuthorityAcrossPublicConnectorAndPrivateTargets(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	owner, err := binding.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool: %v", err)
	}
	identity, err := binding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("RuntimeActivityTarget: %v", err)
	}
	target, err := runtimepipeline.NewChannelActivityTarget(owner, identity.Generation())
	if err != nil {
		t.Fatalf("NewChannelActivityTarget: %v", err)
	}
	carried, ok := target.Tool()
	if !ok {
		t.Fatal("private target lost the admitted execution contract")
	}
	ownerHash, err := owner.CanonicalHash()
	if err != nil {
		t.Fatalf("owner hash: %v", err)
	}
	carriedHash, err := carried.CanonicalHash()
	if err != nil {
		t.Fatalf("carried hash: %v", err)
	}
	if carriedHash != ownerHash || !target.Generation().Equal(identity.Generation()) {
		t.Fatalf("private target reconstructed authority: owner=%s carried=%s generation=%q", ownerHash, carriedHash, target.Generation().Diagnostic())
	}
	httpSpec, ok := carried.HTTP()
	if !ok {
		t.Fatal("private target lost HTTP execution semantics")
	}
	httpSpec.URL = "https://mutated.invalid"
	carriedAgain, _ := target.Tool()
	httpAgain, _ := carriedAgain.HTTP()
	if httpAgain.URL == httpSpec.URL {
		t.Fatal("private target retained caller-owned execution mutation")
	}
}

func TestChannelCompilerZoneHasNoProviderSpecificRuntimeBranch(t *testing.T) {
	body, err := os.ReadFile("channel.go")
	if err != nil {
		t.Fatalf("read channel compiler: %v", err)
	}
	text := strings.ToLower(string(body))
	for _, forbidden := range []string{"internal/providertriggers", "internal/providerconnectors", "telegram", "slack", "discord"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("generic channel compiler contains provider-specific dependency %q", forbidden)
		}
	}
}

func loadTelegramChannelPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(Telegram): %v", err)
	}
	return plan
}

func loadTelegramChannelCompilerInputs(t *testing.T) (*packs.InterfaceRegistry, packs.LoadedChannelPack, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	registry := loadChannelInterfaceRegistry(t)
	triggerCatalog := packfixture.TriggerCatalog(t)
	channels := packfixture.ChannelPacks(t)
	if len(channels) != 1 {
		t.Fatalf("Telegram channel packs = %#v, want one", channels)
	}
	triggerID := channels[0].Envelope.Requires.Packs[packs.TypeTrigger]
	var trigger packs.TriggerPackDescriptor
	for _, candidate := range triggerCatalog.PackDescriptors() {
		if candidate.Identity.ID() == triggerID {
			trigger = candidate
			break
		}
	}
	if trigger.Identity.ID() == "" {
		t.Fatalf("Telegram trigger descriptor %q is missing", triggerID)
	}
	connectorID := channels[0].Envelope.Requires.Packs[packs.TypeConnector]
	var connector packs.ConnectorPackDescriptor
	for _, candidate := range packfixture.ConnectorRegistry(t).PackDescriptors() {
		if candidate.Identity.ID() == connectorID {
			connector = candidate
			break
		}
	}
	if connector.Identity.ID() == "" {
		t.Fatalf("Telegram connector descriptor %q is missing", connectorID)
	}
	return registry, channels[0], trigger, connector
}

func loadChannelInterfaceRegistry(t *testing.T) *packs.InterfaceRegistry {
	t.Helper()
	spec := loadChannelPlatformSpec(t)
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	return registry
}

func loadChannelPlatformSpec(t *testing.T) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", ".."))
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	return spec
}

func mockChannelSatisfier() (packs.LoadedChannelPack, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	text128 := mockStringSchema(1, 128, "")
	label24 := mockStringSchema(1, 24, "")
	token20 := mockStringSchema(1, 20, `^[a-z0-9-]+$`)
	actions := mockArraySchema(0, 2, mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"name": label24, "value": token20,
	}, "name", "value"))
	destination := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`)}, "queue")
	deliveryReference := mockStringSchema(22, 22, `^mock-delivery:[0-9a-f]{8}$`)
	deliveryReceipt := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger)}, "revision")
	interaction := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"cursor": mockStringSchema(1, 16, "")}, "cursor")
	externalAccount := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"principal": mockStringSchema(1, 20, "")}, "principal")
	conversation := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"room": mockStringSchema(1, 20, "")}, "room")
	connectorTools := map[string]runtimecontracts.ToolSchemaEntry{
		"mock.deliver": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "body": text128, "controls": actions,
		}, "destination", "body", "controls"), mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"ref": deliveryReference}, "ref")),
		"mock.edit": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "reference": deliveryReference, "body": text128, "controls": actions,
		}, "destination", "reference", "body", "controls"), mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaInteger)}, "revision")),
		"mock.ack": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"cursor": mockStringSchema(1, 16, ""),
		}, "cursor"), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))),
		"mock.identify_workspace": mockRegistrationTool(
			runtimecontracts.ActivityEffectClassReadOnly,
			[]string{"mock_api_key"},
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{}),
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
				"workspace": mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"key": mockStringSchema(1, 64, `^mock-workspace:[a-z]+$`)}, "key"),
			}, "workspace"),
			runtimecontracts.HTTPToolSpec{Method: http.MethodGet, URL: "https://mock.example.test/v2/workspace", Headers: map[string]string{"X-Mock-Key": "{{credentials.mock_api_key}}"}},
			map[string]any{"workspace": map[string]any{"key": "{{response.body.data.workspace.key}}"}},
		),
		"mock.apply_callback": mockRegistrationTool(
			runtimecontracts.ActivityEffectClassNonIdempotentWrite,
			[]string{"mock_api_key", "mock_callback_key"},
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
				"endpoint": mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"callback": mockStringSchema(1, 512, "")}, "callback"),
			}, "endpoint"),
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"accepted": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean)}, "accepted"),
			runtimecontracts.HTTPToolSpec{Method: http.MethodPut, URL: "https://mock.example.test/v2/callback", Headers: map[string]string{"X-Mock-Key": "{{credentials.mock_api_key}}"}, Body: map[string]any{"subscription": map[string]any{"endpoint": "{{input.endpoint.callback}}", "proof": "{{credentials.mock_callback_key}}"}}},
			map[string]any{"accepted": "{{response.body.result.accepted}}"},
		),
		"mock.read_callback": mockRegistrationTool(
			runtimecontracts.ActivityEffectClassReadOnly,
			[]string{"mock_api_key"},
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{}),
			mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
				"registration": mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"callback": mockStringSchema(0, 512, "")}, "callback"),
			}, "registration"),
			runtimecontracts.HTTPToolSpec{Method: http.MethodGet, URL: "https://mock.example.test/v2/callback", Headers: map[string]string{"X-Mock-Key": "{{credentials.mock_api_key}}"}},
			map[string]any{"registration": map[string]any{"callback": "{{response.body.data.subscription.endpoint}}"}},
		),
	}
	manifest := packs.ChannelManifest{
		Provider: "mock",
		Registration: &packs.ChannelRegistrationProfile{
			Slot: packs.ChannelRegistrationSlot{
				Namespace: "workspace_webhook",
				Identify: packs.ChannelRegistrationOperation{
					Tool: "mock.identify_workspace", Output: map[string]packs.ChannelMapping{"resource_id": {From: "result.workspace.key"}},
				},
			},
			Credentials: packs.ChannelRegistrationCredentials{Provider: []string{"mock_api_key"}, Signing: "mock_callback_key"},
			Apply: packs.ChannelRegistrationOperation{
				Tool: "mock.apply_callback", Input: map[string]packs.ChannelMapping{"endpoint.callback": {From: "context.callback_url"}},
			},
			Readback: packs.ChannelRegistrationOperation{
				Tool: "mock.read_callback", Output: map[string]packs.ChannelMapping{"callback_url": {From: "result.registration.callback"}},
			},
		},
		OpaqueTypes: map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "delivery_reference": deliveryReference, "delivery_receipt": deliveryReceipt,
			"interaction_reference": interaction, "external_account_reference": externalAccount, "conversation_reference": conversation,
		},
		Operations: map[string]packs.ChannelOperationBinding{
			"deliver": {
				Tool: "mock.deliver",
				Input: map[string]packs.ChannelMapping{
					"destination.queue": {From: "context.destination.queue"}, "body": {From: "input.presentation.text"},
					"controls": {Each: "input.actions", Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}, "value": {From: "item.token"}}}},
				},
				Output: map[string]packs.ChannelMapping{"delivery_reference": {From: "result.ref"}},
			},
			"edit": {
				Tool: "mock.edit",
				Input: map[string]packs.ChannelMapping{
					"destination.queue": {From: "context.destination.queue"}, "reference": {From: "input.delivery_reference"}, "body": {From: "input.presentation.text"},
					"controls": {Each: "input.actions", Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}, "value": {From: "item.token"}}}},
				},
				Output: map[string]packs.ChannelMapping{"delivery_receipt.revision": {From: "result.revision"}},
			},
			"acknowledge_interaction": {Tool: "mock.ack", Input: map[string]packs.ChannelMapping{"cursor": {From: "input.interaction_reference.cursor"}}},
		},
		Events: map[string]packs.ChannelEventBinding{
			"action": {Event: "mock.action", Fields: map[string]string{
				"token": "event.token", "interaction_reference.cursor": "event.cursor", "external_account_reference.principal": "event.principal",
				"conversation_reference.room": "event.room", "conversation_scope": "event.scope", "provider_message_reference": "event.message_ref",
			}},
			"text": {Event: "mock.text", Fields: map[string]string{
				"text": "event.text", "external_account_reference.principal": "event.principal", "conversation_reference.room": "event.room", "conversation_scope": "event.scope", "provider_message_reference": "event.message_ref",
			}},
		},
	}
	channel := packs.LoadedChannelPack{
		Envelope: packs.Envelope{
			ID: "provider.mock.hitl_channel", Type: packs.TypeChannel, Version: "0.1.0", ManifestHash: "sha256:" + strings.Repeat("a", 64),
			Implements: []string{"swarm.hitl-channel/v2"}, Provenance: packs.Provenance{Source: "external"},
			Requires: packs.Requires{Packs: map[string]string{packs.TypeTrigger: "provider.mock", packs.TypeConnector: "provider.mock.connector"}},
		},
		Manifest: manifest, Source: packs.MustPackSource("external", "mock-channel"),
	}
	triggerFields := func(names ...string) map[string]packs.TriggerEventField {
		fields := make(map[string]packs.TriggerEventField, len(names))
		for _, name := range names {
			var schema runtimecontracts.ToolInputSchema
			switch name {
			case "token":
				schema = mockStringSchema(1, 20, `^[a-z0-9-]+$`)
			case "cursor":
				schema = mockStringSchema(1, 16, "")
			case "principal", "room":
				schema = mockStringSchema(1, 20, "")
			case "scope":
				schema = runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string"), runtimecontracts.ToolSchemaEnum("direct", "shared"))
			case "message_ref":
				schema = deliveryReference
			case "text":
				schema = text128
			default:
				panic("missing mock trigger field schema for " + name)
			}
			fields[name] = packs.TriggerEventField{Schema: schema, Required: true}
		}
		return fields
	}
	trigger := packs.TriggerPackDescriptor{
		Identity: packs.MustPackIdentity("provider.mock", "0.1.0", "sha256:"+strings.Repeat("b", 64), packs.TypeTrigger, packs.MustPackSource("test", "mock-trigger")), Provider: "mock",
		Generation: triggergeneration.FromCanonicalBytes([]byte("mock-trigger-generation")),
		Events: map[string]packs.TriggerEvent{
			"mock.action": {Name: "mock.action", Fields: triggerFields("token", "cursor", "principal", "room", "scope", "message_ref")},
			"mock.text":   {Name: "mock.text", Fields: triggerFields("text", "principal", "room", "scope", "message_ref")},
		},
	}
	connector := packs.ConnectorPackDescriptor{
		Identity: packs.MustPackIdentity("provider.mock.connector", "0.1.0", "sha256:"+strings.Repeat("c", 64), packs.TypeConnector, packs.MustPackSource("test", "mock-connector")),
		Provider: "mock", Tools: connectorTools,
	}
	return channel, trigger, connector
}

func mockConnectorTool(input, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(input, output))
}

func mockRegistrationTool(effect runtimecontracts.ActivityEffectClass, credentials []string, input, output runtimecontracts.ToolInputSchema, httpSpec runtimecontracts.HTTPToolSpec, responseMapping map[string]any) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory(runtimecontracts.ToolCategoryProviderRegistration.String()),
		runtimecontracts.WithToolDescription("mock provider registration operation"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolEffect(effect),
		runtimecontracts.WithToolSchemas(input, output),
		runtimecontracts.WithToolHTTP(httpSpec),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
		runtimecontracts.WithToolResponseMapping(responseMapping),
		runtimecontracts.WithToolCredentials(credentials...),
	)
}

func mockStringSchema(min, max int, pattern string) runtimecontracts.ToolInputSchema {
	return runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string"), runtimecontracts.ToolSchemaMinLength(min), runtimecontracts.ToolSchemaMaxLength(max), runtimecontracts.ToolSchemaPattern(pattern))
}

func mockArraySchema(min, max int, items runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("array"), runtimecontracts.ToolSchemaItems(items), runtimecontracts.ToolSchemaMinItems(min), runtimecontracts.ToolSchemaMaxItems(max))
}

func mockObjectSchema(properties map[string]runtimecontracts.ToolInputSchema, required ...string) runtimecontracts.ToolInputSchema {
	allowed := false
	return runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(properties), runtimecontracts.ToolSchemaRequired(required...), runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(allowed))

}

func mustSchemaWithProperty(schema runtimecontracts.ToolInputSchema, name string, property runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithProperty(name, property)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustSchemaWithRequiredProperty(schema runtimecontracts.ToolInputSchema, name string, property runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithRequiredProperty(name, property)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustSchemaWithItems(schema, items runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithItems(items)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustSchemaWithMaxLength(schema runtimecontracts.ToolInputSchema, maximum int) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithMaxLength(maximum)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustSchemaWithLengthBounds(schema runtimecontracts.ToolInputSchema, minimum, maximum int) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithLengthBounds(minimum, maximum)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustToolWithSchemas(tool runtimecontracts.ToolSchemaEntry, input, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	updated, err := tool.WithSchemas(input, output)
	if err != nil {
		panic(err)
	}
	return updated
}

func mustToolWithEffect(tool runtimecontracts.ToolSchemaEntry, effect runtimecontracts.ActivityEffectClass) runtimecontracts.ToolSchemaEntry {
	updated, err := tool.WithEffect(effect)
	if err != nil {
		panic(err)
	}
	return updated
}
