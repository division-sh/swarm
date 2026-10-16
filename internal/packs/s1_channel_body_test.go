package packs_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

const s1ChannelDirectoryBody = `transport: webhook
capabilities:
  card_render: true
  reply_to_reference: true
  actions_as_buttons: false
  actions_as_text: true
  edit: false
  acknowledgment: false
  inbox_listing: true
opaque_types: {destination: {type: string}}
operations:
  deliver:
    tool: send_message
    input: {text: input.presentation.text, room: context.destination}
    output: {delivery_reference: result.id}
events:
  text:
    event: text.received
    fields: {text: event.text, conversation_reference: event.room}
onboarding:
  ceremony: authenticated_text_challenge
  provider_credential: bot_token
  signing_credential: webhook_signing
  confirmation: deliver
  learned_destination: {destination: conversation_reference}
registration:
  slot:
    namespace: bot_webhook
    identify: {tool: identify_bot, output: {resource_id: result.id}}
  credentials: {provider: [bot_token], signing: webhook_signing}
  apply: {tool: apply_webhook, input: {callback_url: context.callback_url}}
  readback: {tool: read_webhook, output: {callback_url: result.callback_url}}
`

func s1CapturedChannelBody(t testing.TB, key, source string) packartifact.ProviderBody {
	t.Helper()
	provider, err := packartifact.AdmitProviderMembers(key, map[string][]byte{
		"manifest.yaml":  []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n"),
		"channel.yaml":   []byte(source),
		"trigger.yaml":   []byte("event_name: {literal: raw}\n"),
		"connector.yaml": []byte("tools:\n  send_message:\n    category: provider_connector\n    handler_type: http\n    effect_class: non_idempotent_write\n    response_success: {kind: http_status_2xx}\n    http: {method: POST, url: 'https://example.test/send'}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, present := provider.Body(packmodel.BodyChannel)
	if !present {
		t.Fatal("provider lost channel BODY")
	}
	return body
}

func TestS1ChannelBodyQualifiesOnlyProviderReferences(t *testing.T) {
	var hash string
	for _, key := range []string{"team-bot", "team_bot", "9-team"} {
		t.Run(key, func(t *testing.T) {
			body := s1CapturedChannelBody(t, key, s1ChannelDirectoryBody)
			manifest, err := packs.AdmitChannelBody(body)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Provider != key || manifest.Operations["deliver"].Tool != key+".send_message" || manifest.Events["text"].Event != "inbound."+key+".text.received" {
				t.Errorf("directory did not own channel references: %+v", manifest)
			}
			if manifest.Onboarding.ProviderCredentialRole != key+".bot_token" || manifest.Onboarding.SigningCredentialRole != key+".webhook_signing" || manifest.Onboarding.Confirmation != "deliver" || manifest.Onboarding.Ceremony != "authenticated_text_challenge" {
				t.Errorf("onboarding roles were not projected independently of policy: %+v", manifest.Onboarding)
			}
			registration := manifest.Registration
			if registration.Slot.Namespace != "bot_webhook" || registration.Slot.Identify.Tool != key+".identify_bot" || registration.Apply.Tool != key+".apply_webhook" || registration.Readback.Tool != key+".read_webhook" || !reflect.DeepEqual(registration.Credentials.Provider, []string{key + ".bot_token"}) || registration.Credentials.Signing != key+".webhook_signing" {
				t.Errorf("registration roles/tools or slot policy changed: %+v", registration)
			}
			if manifest.Operations["deliver"].Input["room"].From != "context.destination" || manifest.Events["text"].Fields["text"] != "event.text" || manifest.Onboarding.LearnedDestination["destination"].From != "conversation_reference" {
				t.Fatal("namespace qualification rewrote mapping paths")
			}
			if !bytes.Equal(body.Bytes(), []byte(s1ChannelDirectoryBody)) || manifest.SourceValue().Location().File != key+"/channel.yaml" || (hash != "" && hash != body.Hash()) {
				t.Fatal("projection changed captured source bytes, provenance or hash")
			}
			hash = body.Hash()
			profile, err := packs.CompileChannelOnboardingProfile(manifest.Provider, manifest.Transport, *manifest.Onboarding, []string{"deliver"})
			if err != nil || profile.ProviderCredential() != key+".bot_token" || profile.SigningCredential() != key+".webhook_signing" {
				t.Errorf("compiled onboarding disagreed with BODY identity: %+v %v", profile, err)
			}
		})
	}
}

func TestS1ChannelBodyRejectsQualifiedForeignAndNoncanonicalReferences(t *testing.T) {
	for _, cell := range []struct{ name, old, replacement string }{
		{"qualified tool", "tool: send_message", "tool: team-bot.send_message"},
		{"foreign tool", "tool: send_message", "tool: foreign.send_message"},
		{"path tool", "tool: send_message", "tool: child/send_message"},
		{"whitespace tool", "tool: send_message", "tool: ' send_message '"},
		{"qualified event", "event: text.received", "event: inbound.team-bot.text.received"},
		{"foreign event", "event: text.received", "event: inbound.foreign.text.received"},
		{"path event", "event: text.received", "event: child/text.received"},
		{"wildcard event", "event: text.received", "event: text.*"},
		{"whitespace event", "event: text.received", "event: 'text. received'"},
		{"unicode whitespace event", "event: text.received", "event: 'text.\u00a0received'"},
		{"qualified onboarding role", "provider_credential: bot_token", "provider_credential: team-bot.bot_token"},
		{"foreign onboarding role", "signing_credential: webhook_signing", "signing_credential: foreign.webhook_signing"},
		{"whitespace onboarding role", "provider_credential: bot_token", "provider_credential: ' bot_token '"},
		{"qualified registration tool", "tool: identify_bot", "tool: team-bot.identify_bot"},
		{"foreign apply tool", "tool: apply_webhook", "tool: foreign.apply_webhook"},
		{"qualified readback tool", "tool: read_webhook", "tool: team-bot.read_webhook"},
		{"qualified registration role", "provider: [bot_token]", "provider: [team-bot.bot_token]"},
		{"foreign registration signing", "signing: webhook_signing", "signing: foreign.webhook_signing"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			body := s1CapturedChannelBody(t, "team-bot", strings.ReplaceAll(s1ChannelDirectoryBody, cell.old, cell.replacement))
			if _, err := packs.AdmitChannelBody(body); err == nil || !strings.Contains(err.Error(), "team-bot/channel.yaml:") || !strings.Contains(err.Error(), "short") {
				t.Fatalf("invalid reference gained channel admission without teaching error: %v", err)
			}
		})
	}
}

func TestS1ChannelBodySessionAndNativeNamesRemainPolicyOwned(t *testing.T) {
	session := strings.Split(s1ChannelDirectoryBody, "registration:")[0]
	session = strings.Replace(session, "transport: webhook", "transport: session", 1)
	session = strings.Replace(session, "signing_credential: webhook_signing", "connection_health: provider_connection", 1)
	for _, optionalCredential := range []bool{true, false} {
		bodySource := session
		if !optionalCredential {
			bodySource = strings.Replace(bodySource, "  provider_credential: bot_token\n", "", 1)
		}
		bodySource += "native_inbox:\n  kind: scoped_commands_v1\n  client_languages: [en, fr]\n  direct_launcher_read: read_inbox_launcher\n  default_launcher_read: read_default_inbox_launcher\n  commands_launcher: commands\n  inherited_launcher: default\n"
		manifest, err := packs.AdmitChannelBody(s1CapturedChannelBody(t, "team-bot", bodySource))
		if err != nil {
			t.Fatal(err)
		}
		profile, err := packs.CompileChannelOnboardingProfile(manifest.Provider, manifest.Transport, *manifest.Onboarding, []string{"deliver"})
		if err != nil || profile.ConnectionHealth() != "provider_connection" || profile.SigningCredential() != "" || profile.ConfirmationOperation() != "deliver" || manifest.Registration != nil {
			t.Fatalf("session projection changed connection semantics: %+v %v", profile, err)
		}
		if (profile.ProviderCredential() == "team-bot.bot_token") != optionalCredential || (!optionalCredential && profile.ProviderCredential() != "") {
			t.Fatal("session projection fabricated or lost a genuine credential")
		}
		native := manifest.NativeInbox
		if native.DirectLauncherRead != "read_inbox_launcher" || native.DefaultLauncherRead != "read_default_inbox_launcher" || native.CommandsLauncher != "commands" || native.InheritedLauncher != "default" || !reflect.DeepEqual(native.ClientLanguages, []string{"en", "fr"}) {
			t.Fatalf("native semantic operation names were provider-qualified: %+v", native)
		}
	}
}

func TestS1ChannelBodyAliasesAndRepeatedProjectionKeepFrozenEvidence(t *testing.T) {
	source := strings.Replace(s1ChannelDirectoryBody, "  deliver:\n", "  deliver: &message\n", 1)
	source = strings.Replace(source, "events:\n", "  repeated: *message\nevents:\n", 1)
	body := s1CapturedChannelBody(t, "team-bot", source)
	first, err := packs.AdmitChannelBody(body)
	if err != nil {
		t.Fatal(err)
	}
	if first.Operations["deliver"].Tool != "team-bot.send_message" || first.Operations["repeated"].Tool != "team-bot.send_message" {
		t.Fatal("aliased operation lost exact local reference projection")
	}
	first.Operations["deliver"].Input["room"] = packs.ChannelMapping{From: "foreign.room"}
	first.Onboarding.ProviderCredentialRole = "foreign.token"
	copy := body.Bytes()
	copy[0] = 'X'
	second, err := packs.AdmitChannelBody(body)
	if err != nil || second.Operations["deliver"].Tool != "team-bot.send_message" || second.Operations["deliver"].Input["room"].From != "context.destination" || second.Onboarding.ProviderCredentialRole != "team-bot.bot_token" || !bytes.Equal(body.Bytes(), []byte(source)) {
		t.Fatalf("readback or repeated projection changed captured evidence: %+v %v", second, err)
	}
}

func TestS1ChannelBodyRequiresCapturedRoleAndRejectsAuthoredProvider(t *testing.T) {
	provider, err := packartifact.AdmitProviderMembers("team-bot", map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw]}, cannot: [execute code]}\n"),
		"trigger.yaml":  []byte("event_name: {literal: raw}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	trigger, _ := provider.Body(packmodel.BodyTrigger)
	if _, err := packs.AdmitChannelBody(trigger); err == nil {
		t.Fatal("trigger role became a channel")
	}
	for _, authored := range []string{"team-bot", "foreign", "null"} {
		_, err := packartifact.AdmitProviderMembers("team-bot", map[string][]byte{
			"manifest.yaml":  []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw]}, cannot: [execute code]}\n"),
			"trigger.yaml":   []byte("event_name: {literal: raw}\n"),
			"connector.yaml": []byte("tools: {}\n"),
			"channel.yaml":   []byte("provider: " + authored + "\n" + s1ChannelDirectoryBody),
		})
		if err == nil || !strings.Contains(err.Error(), "directory") {
			t.Fatalf("authored provider %q overrode captured channel identity: %v", authored, err)
		}
	}
}

func TestS1ChannelBodyLogicalReconstructionRetainsNamespace(t *testing.T) {
	members := map[string][]byte{
		"shop/agents.yaml":             []byte("{}\n"),
		"shop/schema.yaml":             []byte("stages: []\n"),
		"shop/team-bot/manifest.yaml":  []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n"),
		"shop/team-bot/channel.yaml":   []byte(s1ChannelDirectoryBody),
		"shop/team-bot/trigger.yaml":   []byte("event_name: {literal: raw}\n"),
		"shop/team-bot/connector.yaml": []byte("tools:\n  send_message:\n    category: provider_connector\n    handler_type: http\n    effect_class: non_idempotent_write\n    response_success: {kind: http_status_2xx}\n    http: {method: POST, url: 'https://example.test/send'}\n"),
	}
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*sourceartifact.AdmittedSourceArtifact{artifact, retained} {
		provider, err := packartifact.ProviderFromArtifact(source, "shop/team-bot")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := provider.Body(packmodel.BodyChannel)
		manifest, err := packs.AdmitChannelBody(body)
		if err != nil || manifest.Provider != "team-bot" || manifest.Operations["deliver"].Tool != "team-bot.send_message" || manifest.Events["text"].Event != "inbound.team-bot.text.received" {
			t.Fatalf("logical reconstruction lost channel identity: %+v %v", manifest, err)
		}
	}
	if _, err := packs.AdmitChannelBody(packartifact.ProviderBody{}); err == nil {
		t.Fatal("zero BODY became a channel")
	}
}
