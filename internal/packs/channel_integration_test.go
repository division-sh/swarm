package packs_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

func TestChannelGenerationRejectsProgrammaticEmptyEnum(t *testing.T) {
	if _, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum()); err == nil || !strings.Contains(err.Error(), "enum must contain at least one value") {
		t.Fatalf("schema admission empty enum error = %v", err)
	}
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
	if capacity, err := binding.PresentationBounds(); err != nil || capacity.Actions != 8 || capacity.TextRunes != 4096 || capacity.LabelRunes != 64 {
		t.Fatalf("Telegram selected action capacity = %+v, %v", capacity, err)
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
	target := "ingress:telegram-ingress:telegram"
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
		BundleHash:     "bundle-v2:sha256:" + strings.Repeat("a", 64),
		BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
		RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: generation,
	}
	admissions := []channelonboarding.CredentialAdmission{
		{Role: "telegram_bot_token", StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("a")},
		{Role: "webhook_signing_secret", StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("b")},
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
			"ingress:.:telegram",
		)
		if bindingErr != nil {
			t.Fatal(bindingErr)
		}
		return binding
	}
	base := channelonboarding.CompiledActivation{
		Source: channelonboarding.ActivationSourceDeclared,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash:     "bundle-v2:sha256:" + strings.Repeat("a", 64),
			BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
			RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
			ContextPublicationGeneration: 7, PlanGeneration: planGeneration,
		},
		Plan: newBinding("-100123"),
		CredentialAdmissions: []channelonboarding.CredentialAdmission{
			{Role: "telegram_bot_token", StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("a")},
			{Role: "webhook_signing_secret", StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("b")},
		},
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{base})
	if err != nil {
		t.Fatal(err)
	}
	contradictory := base
	contradictory.SessionAccount = operatorchannel.SessionAccountAdmission{Provider: "whatsapp"}
	if _, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{contradictory}); err == nil {
		t.Fatal("webhook publication accepted native session authority")
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
		"credential value seal": func(value channelonboarding.CompiledActivation) channelonboarding.CompiledActivation {
			value.CredentialAdmissions = append([]channelonboarding.CredentialAdmission(nil), value.CredentialAdmissions...)
			value.CredentialAdmissions[0].ValueSeal = packTestCredentialSeal("c")
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

func TestChannelDeliveryReplyProjectionTelegram(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	_, _, trigger, _ := loadTelegramChannelCompilerInputs(t)
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.text_message", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation,
	)
	base := map[string]any{
		"text": "reason", "external_account": "12345", "conversation_reference": "-100123",
		"conversation_scope": "shared", "provider_message_reference": json.Number("17"),
	}
	fact, matched, err := plan.ProjectTextFact("inbound.telegram.text_message", authorization, base)
	if err != nil || !matched || fact.MessageReference != `{"id":17}` || fact.ReplyToReference != "" {
		t.Fatalf("unquoted Telegram fact = %#v matched=%v err=%v", fact, matched, err)
	}
	base["command_invocation"] = map[string]any{"reference": "inbox_abc123", "address": "SampleBot"}
	fact, matched, err = plan.ProjectTextFact("inbound.telegram.text_message", authorization, base)
	if err != nil || !matched || fact.EntryReference != "inbox_abc123" || fact.EntryAddress != "SampleBot" {
		t.Fatalf("addressed Telegram entry fact = %#v matched=%v err=%v", fact, matched, err)
	}
	delete(base, "command_invocation")
	base["reply_to_message_reference"] = json.Number("11")
	fact, matched, err = plan.ProjectTextFact("inbound.telegram.text_message", authorization, base)
	if err != nil || !matched || fact.ReplyToReference != `{"id":11}` {
		t.Fatalf("quoted Telegram fact = %#v matched=%v err=%v", fact, matched, err)
	}
	base["reply_to_message_reference"] = json.Number("2147483648")
	if _, _, err := plan.ProjectTextFact("inbound.telegram.text_message", authorization, base); err == nil {
		t.Fatal("out-of-range present Telegram reply reference was admitted")
	}
	delete(base, "reply_to_message_reference")
	delete(base, "provider_message_reference")
	if _, _, err := plan.ProjectTextFact("inbound.telegram.text_message", authorization, base); err == nil {
		t.Fatal("missing required Telegram message reference was admitted")
	}
	actionAuthorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.callback_action", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation,
	)
	action, matched, err := plan.ProjectActionFact("inbound.telegram.callback_action", actionAuthorization, map[string]any{
		"token": "approve_1", "interaction_reference": "callback-1", "external_account": "12345",
		"conversation_reference": "-100123", "conversation_scope": "shared", "provider_message_reference": json.Number("11"),
	})
	if err != nil || !matched || action.Token != "approve_1" || action.InteractionRef != "callback-1" || action.MessageReference != `{"id":11}` {
		t.Fatalf("Telegram action projection = %#v matched=%v err=%v", action, matched, err)
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
	if profile.ProviderCredential() != "telegram.telegram_bot_token" || profile.SigningCredential() != "telegram.webhook_signing_secret" || profile.ConfirmationOperation() != "deliver" {
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
		BundleHash:     "bundle-v2:sha256:" + strings.Repeat("a", 64),
		BundleIdentity: "telegram@1.0.0#bundle", PackInventoryGeneration: "sha256:inventory",
		RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: generation,
	}
	target := "ingress:telegram-ingress:telegram"
	candidate := channelonboarding.Candidate{
		Provider: profile.Provider(), Interface: identity, Coordinate: coordinate,
		Target: channelonboarding.CandidateTarget{
			Selector: target, FlowPath: "telegram-ingress",
			Alias: "telegram", Provider: profile.Provider(),
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
			{Role: profile.ProviderCredential(), StoreKey: "telegram.provider", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("a")},
			{Role: profile.SigningCredential(), StoreKey: "telegram.signing", Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: packTestCredentialSeal("b")},
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

	t.Run("registered_account_without_operator_destination", func(t *testing.T) {
		op := channelonboarding.Operation{DeploymentRegistration: true, AccountConfirmed: true,
			OperationID: uuid.NewString(), Revision: 3, Provider: "telegram", AccountReference: "420079",
			Posture: channelonboarding.ActivationWebhookRegistration, Phase: channelonboarding.PhaseSucceeded,
			CredentialAdmissions: activation.CredentialAdmissions}
		registered, err := channelonboarding.CompileRegisteredActivation(candidate, op)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := registered.Plan.RuntimeTools()
		if err != nil || len(tools) != 0 || !registered.Plan.ActivitiesOnly() || registered.Plan.HasNativeInbox() || registered.Plan.Destination().Interface() != nil {
			t.Fatalf("account registration acquired operator authority: tools=%d err=%v", len(tools), err)
		}
		if _, _, err := registered.Plan.PrepareOperation("deliver", map[string]any{}); err == nil {
			t.Fatal("account registration acquired operator delivery")
		}
		if _, err := packs.NewOutboundBindingPlanWithRegistration("ordinary", plan, nil, nil, nil, target); err == nil {
			t.Fatal("ordinary webhook binding accepted a missing destination")
		}
		op.AccountConfirmed = false
		if _, err := channelonboarding.CompileRegisteredActivation(candidate, op); err == nil {
			t.Fatal("unconfirmed registration acquired source activation")
		}
	})
}

func packTestCredentialSeal(digit string) runtimecredentials.ValueSeal {
	return runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat(digit, 64))
}

func TestChannelOnboardingProfileAxesAreProviderNeutral(t *testing.T) {
	for _, tc := range []struct {
		name     string
		profile  packs.ChannelOnboardingProfile
		posture  packs.ChannelActivationPosture
		ceremony packs.ChannelIdentityCeremony
	}{
		{
			name:    "discord gateway text",
			profile: packs.ChannelOnboardingProfile{Ceremony: "authenticated_text_challenge", ProviderCredentialRole: "discord_app_token", Confirmation: "deliver", ConnectionHealth: "provider_connection", LearnedDestination: map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}}},
			posture: packs.ChannelActivationSessionConnection, ceremony: packs.ChannelCeremonyAuthenticatedTextChallenge,
		},
		{
			name:    "session without a token requirement",
			profile: packs.ChannelOnboardingProfile{Ceremony: "authenticated_text_challenge", Confirmation: "deliver", ConnectionHealth: "provider_connection", LearnedDestination: map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}}},
			posture: packs.ChannelActivationSessionConnection, ceremony: packs.ChannelCeremonyAuthenticatedTextChallenge,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := packs.CompileChannelOnboardingProfile("paper-port", packs.ChannelTransportSession, tc.profile, []string{"deliver"})
			if err != nil {
				t.Fatalf("CompileChannelOnboardingProfile: %v", err)
			}
			if compiled.ActivationPosture() != tc.posture || compiled.IdentityCeremony() != tc.ceremony {
				t.Fatalf("compiled axes = %#v", compiled)
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

func TestChannelCompilerRetainsExactConnectorDeclaration(t *testing.T) {
	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	plan, err := packs.CompileProviderChannel(registry, channel, trigger, connector)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
	if err != nil {
		t.Fatal(err)
	}
	id, declaration, err := binding.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	original, found := connector.Tools[id]
	if !found {
		t.Fatal("declaration is absent from the admitted connector")
	}
	want, err := original.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	got, err := declaration.CanonicalHash()
	if err != nil || got != want {
		t.Fatal("channel compilation mutated the connector declaration", err, got, want)
	}
	executable, err := binding.OperationTool("deliver")
	if err != nil {
		t.Fatal(err)
	}
	if _, compiled := executable.CompiledResultExecution(); !compiled {
		t.Fatal("preserving declaration evidence removed channel result execution")
	}
	if _, _, err := binding.ConnectorDeclaration("missing"); err == nil {
		t.Fatal("undeclared operation acquired connector evidence")
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
	plan, err := packs.CompileProviderChannel(registry, channel, trigger, connector)
	if err != nil {
		t.Fatalf("CompileChannel(Telegram): %v", err)
	}
	return plan
}

func loadTelegramChannelCompilerInputs(t *testing.T) (*packs.InterfaceRegistry, packartifact.AdmittedProvider, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	registry := loadChannelInterfaceRegistry(t)
	channels := packfixture.ChannelPacks(t)
	var channel packartifact.AdmittedProvider
	count := 0
	for _, candidate := range channels {
		if candidate.Key() == "telegram" {
			channel = candidate
			count++
		}
	}
	if count != 1 {
		t.Fatalf("exact Telegram channel count = %d, want one", count)
	}
	trigger, connector := s1AcceptedChannelRoleDescriptors(t, channel)
	return registry, channel, trigger, connector
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
	spec, err = runtimecontracts.AdmitPlatformSpecValue(snapshot.Document(filepath.Join(repo, "platform-spec.yaml")).Root())
	if err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	return spec
}

func mustSchemaWithProperty(schema runtimecontracts.ToolInputSchema, name string, property runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	updated, err := schema.WithProperty(name, property)
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

func mustToolWithSchemas(tool runtimecontracts.ToolSchemaEntry, input, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	updated, err := tool.WithSchemas(input, output)
	if err != nil {
		panic(err)
	}
	return updated
}
