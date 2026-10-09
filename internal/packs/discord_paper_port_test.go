package packs_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

// The descriptor is a paper port, not an installed Gateway adapter. Its typed
// normalized catalog and HTTP operation consume the existing channel compiler.
func discordPaperPort(t *testing.T) (packs.SatisfactionPlan, packs.TriggerPackDescriptor) {
	t.Helper()
	body, err := os.ReadFile("testdata/discord-paper-port/channel.yaml")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := packs.ParseChannelManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	channel := packs.LoadedChannelPack{
		Envelope: packs.Envelope{ID: "provider.discord.hitl_channel", Type: packs.TypeChannel,
			Version: "0.1.0", ManifestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(body)),
			Implements: []string{"swarm.hitl-channel/v2"}, Provenance: packs.Provenance{Source: "external"},
			Requires: packs.Requires{Packs: map[string]string{packs.TypeTrigger: "provider.discord", packs.TypeConnector: "provider.discord.connector"}}},
		Manifest: manifest, Source: packs.MustPackSource("test", "discord-paper-port"),
	}
	snowflake := manifest.OpaqueTypes["delivery_reference"]
	fields := map[string]packs.TriggerEventField{
		"text":       {Schema: mockStringSchema(1, 2000, ""), Required: true},
		"author_id":  {Schema: snowflake, Required: true},
		"channel_id": {Schema: snowflake, Required: true},
		"message_id": {Schema: snowflake, Required: true},
		"reply_id":   {Schema: snowflake},
		"scope": {Schema: contracts.MustToolInputSchema(contracts.ToolSchemaString,
			contracts.ToolSchemaEnum("direct", "shared")), Required: true},
		"entry": {Schema: mockObjectSchema(map[string]contracts.ToolInputSchema{
			"reference": mockStringSchema(1, 32, ""), "address": mockStringSchema(5, 32, ""),
		}, "reference")},
	}
	trigger := packs.TriggerPackDescriptor{
		Transport: packs.ChannelTransportSession,
		Identity:  packs.MustPackIdentity("provider.discord", "0.1.0", "sha256:"+strings.Repeat("e", 64), packs.TypeTrigger, packs.MustPackSource("test", "discord-paper-port")),
		Provider:  "discord", Generation: triggergeneration.FromCanonicalBytes([]byte("discord-paper-port-normalized-catalog")),
		Events: map[string]packs.TriggerEvent{"inbound.discord.text_message": {Name: "inbound.discord.text_message", Fields: fields}},
	}
	connector := packs.ConnectorPackDescriptor{
		Identity: packs.MustPackIdentity("provider.discord.connector", "0.1.0", "sha256:"+strings.Repeat("f", 64), packs.TypeConnector, packs.MustPackSource("test", "discord-paper-port")),
		Provider: "discord", Tools: map[string]contracts.ToolSchemaEntry{
			"discord.create_message": contracts.MustToolSchemaEntry(
				contracts.WithToolCategory(contracts.ToolCategoryProviderConnector.String()),
				contracts.WithToolHandler(contracts.ToolHandlerHTTP),
				contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
				contracts.WithToolCredentials("discord_bot_token"),
				contracts.WithToolSchemas(mockObjectSchema(map[string]contracts.ToolInputSchema{
					"channel_id": snowflake, "content": mockStringSchema(1, 2000, ""),
				}, "channel_id", "content"), mockObjectSchema(map[string]contracts.ToolInputSchema{"id": snowflake}, "id")),
				contracts.WithToolHTTP(contracts.HTTPToolSpec{Method: "POST", URL: "https://discord.com/api/v10/channels/{{input.channel_id}}/messages",
					Headers: map[string]string{"Authorization": "Bot {{credentials.discord_bot_token}}"},
					Body:    map[string]any{"content": "{{input.content}}", "allowed_mentions": map[string]any{"parse": []any{}}}}),
				contracts.WithToolResponseSuccess(contracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
				contracts.WithToolResponseMapping(map[string]any{"id": "{{response.body.id}}"}),
			),
		},
	}
	plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	return plan, trigger
}

func TestDiscordPaperPortFitsExistingNeutralInterface(t *testing.T) {
	plan, _ := discordPaperPort(t)
	vector := plan.Capabilities().Vector()
	if plan.Transport() != packs.ChannelTransportSession || plan.HasNativeInbox() ||
		!vector.CardRender || !vector.ReplyToReference || !vector.ActionsAsText || !vector.InboxListing ||
		vector.Edit || vector.ActionsAsButtons || vector.Acknowledgment {
		t.Fatal("paper port changed its declared session/text baseline")
	}
	profile, found := plan.OnboardingProfile()
	if !found || profile.IdentityCeremony() != packs.ChannelCeremonyAuthenticatedTextChallenge ||
		profile.ProviderCredential() != "discord_bot_token" || profile.SigningCredential() != "" {
		t.Fatal("Gateway readiness replaced the real credential or human claim")
	}
	bounds, err := plan.PresentationBounds()
	if err != nil || bounds.TextRunes != 2000 {
		t.Fatalf("Discord text bound = %+v, %v", bounds, err)
	}
	input, err := plan.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": "Inbox"}}, map[string]any{"destination": "123456789012345678"})
	if err != nil || input["channel_id"] != "123456789012345678" || input["content"] != "Inbox" || len(input) != 2 {
		t.Fatalf("session ingress cannot coexist with HTTP delivery: %v, %v", input, err)
	}
	for _, operation := range []string{"edit", "acknowledge_interaction", "install_inbox_entry", "read_default_inbox_launcher"} {
		if _, _, err := plan.ConnectorOperation(operation); err == nil {
			t.Fatalf("paper port fabricated optional operation %s", operation)
		}
	}
	var unavailable *operatorchannel.SessionProviderUnavailableError
	if err := plan.RequireExecutableProvider(); !errors.As(err, &unavailable) {
		t.Fatalf("uninstalled Gateway transport was executable: %v", err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(channelonboarding.Candidate{Provider: "discord", Plan: plan,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{PlanGeneration: generation},
		Target:     channelonboarding.CandidateTarget{AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("discord-paper-port-normalized-catalog"))}})
	if err != nil {
		t.Fatal(err)
	}
	var readback struct {
		Capabilities map[string]bool `json:"capabilities"`
	}
	if err := json.Unmarshal(raw, &readback); err != nil || len(readback.Capabilities) != 7 ||
		readback.Capabilities["edit"] || readback.Capabilities["actions_as_buttons"] ||
		readback.Capabilities["acknowledgment"] || !readback.Capabilities["inbox_listing"] {
		t.Fatalf("candidate readback omitted or fabricated vector fields: %s: %v", raw, err)
	}
}

func TestDiscordPaperPortNormalizedReplyAndReferenceTranscripts(t *testing.T) {
	plan, trigger := discordPaperPort(t)
	authorization := provideroutput.MustAuthorization("discord", "inbound.discord.text_message", trigger.Identity.ID(), trigger.Identity.Version(), trigger.Identity.ManifestHash(), trigger.Generation)
	// These are the expected adapter outputs specified in the paper transcript,
	// not a new implementation of Discord's raw-message interpreter.
	for _, row := range []struct {
		name, text, reply, scope string
		entry                    bool
	}{
		{"direct genuine reply", "Retire", "555", "direct", false},
		{"guild genuine reply", "Retire", "555", "shared", false},
		{"reference type absent uses Discord DEFAULT contract", "Retire", "555", "direct", false},
		{"forward snapshot has no reply authority", "Retire", "", "shared", false},
		{"crosspost reference has no reply authority", "Retire", "", "shared", false},
		{"thread starter reference has no reply authority", "Retire", "", "shared", false},
		{"context menu reference has no reply authority", "Retire", "", "shared", false},
		{"missing reference has no reply authority", "Retire", "", "direct", false},
		{"unknown referenced message with exact receipt ID", "Retire", "555", "direct", false},
		{"deleted referenced message has no reply authority", "Retire", "", "direct", false},
		{"unquoted words do not acquire a control", "Retire", "", "direct", false},
		{"receipt independent inbox", "/inbox", "", "direct", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			payload := map[string]any{"text": row.text, "author_id": "111", "channel_id": "222", "message_id": "333", "scope": row.scope}
			if row.reply != "" {
				payload["reply_id"] = row.reply
			}
			if row.entry {
				payload["entry"] = map[string]any{"reference": "inbox"}
			}
			fact, matched, err := plan.ProjectTextFact("inbound.discord.text_message", authorization, payload)
			if err != nil || !matched || fact.ReplyToReference != row.reply || fact.ConversationRef != "222" || fact.ExternalAccountRef != "111" {
				t.Fatalf("normalized transcript = %+v, %t, %v", fact, matched, err)
			}
			if row.entry != (fact.EntryReference == "inbox") {
				t.Fatal("receipt-independent inbox was changed")
			}
			action := operatorchannel.ActionFact{Kind: operatorchannel.ActionSourceReply, Interface: fact.Interface,
				ExternalAccountRef: fact.ExternalAccountRef, ConversationRef: fact.ConversationRef,
				ConversationScope: fact.ConversationScope, MessageReference: row.reply, TextSource: fact, Token: "stored-token"}
			if (action.Validate() == nil) != (row.reply != "" && !row.entry) {
				t.Fatal("reference-free text acquired reply evidence")
			}
		})
	}
	for _, content := range []any{nil, "", strings.Repeat("x", 2001)} {
		if _, _, err := plan.ProjectTextFact("inbound.discord.text_message", authorization,
			map[string]any{"text": content, "author_id": "111", "channel_id": "222", "message_id": "333", "scope": "shared"}); err == nil {
			t.Fatalf("missing privileged content or out-of-bounds content accepted: %T", content)
		}
	}
	if _, matched, err := plan.ProjectTextFact("inbound.discord.text_message", provideroutput.Authorization{}, map[string]any{}); err == nil && matched {
		t.Fatal("unverified Gateway data acquired provider-output authority")
	}
}
