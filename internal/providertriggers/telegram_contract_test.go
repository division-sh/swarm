package providertriggers

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

const telegramBotAPIUpdateContract = "Bot API 10.1 (2026-06-11) https://core.telegram.org/bots/api#update"

var telegramBotAPI101UpdateRoots = []string{
	"message",
	"edited_message",
	"channel_post",
	"edited_channel_post",
	"business_connection",
	"business_message",
	"edited_business_message",
	"deleted_business_messages",
	"guest_message",
	"message_reaction",
	"message_reaction_count",
	"inline_query",
	"chosen_inline_result",
	"callback_query",
	"shipping_query",
	"pre_checkout_query",
	"purchased_paid_media",
	"poll",
	"poll_answer",
	"my_chat_member",
	"chat_member",
	"chat_join_request",
	"chat_boost",
	"removed_chat_boost",
	"managed_bot",
}

func TestTelegramSelectedTextMessageContractUsesShippedPack(t *testing.T) {
	_, catalog, plan := telegramPlatformContract(t)
	delivery, err := plan.Accept(telegramContractRequest(t, map[string]any{
		"update_id": 101,
		"message": map[string]any{
			"message_id": 7,
			"from":       map[string]any{"id": 12345},
			"chat":       map[string]any{"id": -1001234567890, "type": "private"},
			"text":       "hello",
		},
	}))
	if err != nil {
		t.Fatalf("Accept selected Telegram text message: %v", err)
	}
	if len(delivery.Events) != 2 {
		t.Fatalf("events = %#v, want exactly raw plus text_message", delivery.Events)
	}
	if raw := delivery.Events[0]; raw.Kind != OutputKindRaw || raw.Name != "inbound.telegram" || !raw.Authorization.Empty() {
		t.Fatalf("raw event = %#v", raw)
	}
	normalized := delivery.Events[1]
	if normalized.Kind != OutputKindNormalized || normalized.Name != "inbound.telegram.text_message" {
		t.Fatalf("normalized event = %#v", normalized)
	}
	wantPayload := map[string]any{
		"external_account_reference": "12345", "conversation_reference": "-1001234567890",
		"provider_message_reference": json.Number("7"), "text": "hello",
	}
	if !reflect.DeepEqual(normalized.Payload, wantPayload) {
		t.Fatalf("normalized payload = %#v, want %#v", normalized.Payload, wantPayload)
	}
	if err := catalog.VerifyProviderOutputAuthorization(normalized.Authorization); err != nil {
		t.Fatalf("VerifyProviderOutputAuthorization: %v", err)
	}
}

func TestTelegramSelectedCallbackActionContractUsesShippedPack(t *testing.T) {
	_, _, plan := telegramPlatformContract(t)
	delivery, err := plan.Accept(telegramContractRequest(t, map[string]any{
		"update_id": 102,
		"callback_query": map[string]any{
			"id": "callback-1", "data": "approve_1", "from": map[string]any{"id": 12345},
			"message": map[string]any{"message_id": 8, "chat": map[string]any{"id": 67890, "type": "private"}},
		},
	}))
	if err != nil {
		t.Fatalf("Accept selected Telegram callback action: %v", err)
	}
	if len(delivery.Events) != 2 || delivery.Events[1].Name != "inbound.telegram.callback_action" {
		t.Fatalf("events = %#v, want raw plus callback_action", delivery.Events)
	}
	want := map[string]any{
		"token": "approve_1", "interaction_reference": "callback-1", "external_account_reference": "12345",
		"conversation_reference": "67890", "provider_message_reference": json.Number("8"),
	}
	if !reflect.DeepEqual(delivery.Events[1].Payload, want) {
		t.Fatalf("callback payload = %#v, want %#v", delivery.Events[1].Payload, want)
	}
}

func TestTelegramTextRejectsOutOfRangeProviderMessageReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "text provider message", "message.message_id", map[string]any{
		"update_id": 201,
		"message": map[string]any{
			"message_id": 2147483648, "from": map[string]any{"id": 12345},
			"chat": map[string]any{"id": 67890, "type": "private"}, "text": "hello",
		},
	}, "must be <= 2.147483647e+09")
}

func TestTelegramTextRejectsNegativeExternalAccountReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "text external account", "message.from.id", map[string]any{
		"update_id": 202,
		"message": map[string]any{
			"message_id": 7, "from": map[string]any{"id": -1},
			"chat": map[string]any{"id": 67890, "type": "private"}, "text": "hello",
		},
	}, "must match pattern")
}

func TestTelegramTextRejectsOverlongConversationReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "text conversation", "message.chat.id", map[string]any{
		"update_id": 203,
		"message": map[string]any{
			"message_id": 7, "from": map[string]any{"id": 12345},
			"chat": map[string]any{"id": 123456789012345678, "type": "private"}, "text": "hello",
		},
	}, "length must be <= 17")
}

func TestTelegramCallbackRejectsOutOfRangeProviderMessageReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "callback provider message", "callback_query.message.message_id", map[string]any{
		"update_id": 204,
		"callback_query": map[string]any{
			"id": "callback-1", "data": "approve_1", "from": map[string]any{"id": 12345},
			"message": map[string]any{"message_id": 2147483648, "chat": map[string]any{"id": 67890, "type": "private"}},
		},
	}, "must be <= 2.147483647e+09")
}

func TestTelegramCallbackRejectsNegativeExternalAccountReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "callback external account", "callback_query.from.id", map[string]any{
		"update_id": 205,
		"callback_query": map[string]any{
			"id": "callback-1", "data": "approve_1", "from": map[string]any{"id": -1},
			"message": map[string]any{"message_id": 8, "chat": map[string]any{"id": 67890, "type": "private"}},
		},
	}, "must match pattern")
}

func TestTelegramCallbackRejectsOverlongConversationReference(t *testing.T) {
	assertTelegramNormalizedIdentifierRejected(t, "callback conversation", "callback_query.message.chat.id", map[string]any{
		"update_id": 206,
		"callback_query": map[string]any{
			"id": "callback-1", "data": "approve_1", "from": map[string]any{"id": 12345},
			"message": map[string]any{"message_id": 8, "chat": map[string]any{"id": 123456789012345678, "type": "private"}},
		},
	}, "length must be <= 17")
}

func assertTelegramNormalizedIdentifierRejected(t *testing.T, subject, sourcePath string, payload map[string]any, want string) {
	t.Helper()
	_, _, plan := telegramPlatformContract(t)
	delivery, err := plan.Accept(telegramContractRequest(t, payload))
	if err == nil || !strings.Contains(err.Error(), sourcePath) || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s admission = %#v, error = %v; want source %q and %q", subject, delivery, err, sourcePath, want)
	}
	if len(delivery.Events) != 0 {
		t.Fatalf("%s emitted events after identifier rejection: %#v", subject, delivery.Events)
	}
}

func TestTelegramBotAPI101ResidualUpdateUnionRemainsRawOnly(t *testing.T) {
	_, _, plan := telegramPlatformContract(t)
	fixtures := []struct {
		name  string
		root  string
		value any
	}{
		{name: "message missing text", root: "message", value: map[string]any{"message_id": 1, "chat": map[string]any{"id": 42}, "photo": []any{map[string]any{"file_id": "photo"}}}},
		{name: "message missing chat id", root: "message", value: map[string]any{"message_id": 1, "text": "hello"}},
		{name: "message missing message id", root: "message", value: map[string]any{"chat": map[string]any{"id": 42}, "text": "hello"}},
		{name: "edited message", root: "edited_message", value: map[string]any{"message_id": 1}},
		{name: "channel post", root: "channel_post", value: map[string]any{"message_id": 1}},
		{name: "edited channel post", root: "edited_channel_post", value: map[string]any{"message_id": 1}},
		{name: "business connection", root: "business_connection", value: map[string]any{"id": "business"}},
		{name: "business message", root: "business_message", value: map[string]any{"message_id": 1}},
		{name: "edited business message", root: "edited_business_message", value: map[string]any{"message_id": 1}},
		{name: "deleted business messages", root: "deleted_business_messages", value: map[string]any{"business_connection_id": "business"}},
		{name: "guest message", root: "guest_message", value: map[string]any{"message_id": 1}},
		{name: "message reaction", root: "message_reaction", value: map[string]any{"date": 1}},
		{name: "message reaction count", root: "message_reaction_count", value: map[string]any{"date": 1}},
		{name: "inline query", root: "inline_query", value: map[string]any{"id": "inline"}},
		{name: "chosen inline result", root: "chosen_inline_result", value: map[string]any{"result_id": "result"}},
		{name: "callback message data", root: "callback_query", value: telegramCallbackFixture(map[string]any{"message": telegramCallbackMessage(), "data": "choice"})},
		{name: "callback message game", root: "callback_query", value: telegramCallbackFixture(map[string]any{"message": telegramCallbackMessage(), "game_short_name": "game"})},
		{name: "callback inline data", root: "callback_query", value: telegramCallbackFixture(map[string]any{"inline_message_id": "inline", "data": "choice"})},
		{name: "callback inline game", root: "callback_query", value: telegramCallbackFixture(map[string]any{"inline_message_id": "inline", "game_short_name": "game"})},
		{name: "shipping query", root: "shipping_query", value: map[string]any{"id": "shipping"}},
		{name: "pre-checkout query", root: "pre_checkout_query", value: map[string]any{"id": "checkout"}},
		{name: "purchased paid media", root: "purchased_paid_media", value: map[string]any{"payload": "purchase"}},
		{name: "poll", root: "poll", value: map[string]any{"id": "poll"}},
		{name: "poll answer", root: "poll_answer", value: map[string]any{"poll_id": "poll"}},
		{name: "my chat member", root: "my_chat_member", value: map[string]any{"date": 1}},
		{name: "chat member", root: "chat_member", value: map[string]any{"date": 1}},
		{name: "chat join request", root: "chat_join_request", value: map[string]any{"date": 1}},
		{name: "chat boost", root: "chat_boost", value: map[string]any{"boost": map[string]any{"boost_id": "boost"}}},
		{name: "removed chat boost", root: "removed_chat_boost", value: map[string]any{"boost_id": "boost"}},
		{name: "managed bot", root: "managed_bot", value: map[string]any{"bot": map[string]any{"id": 1}}},
	}

	covered := make(map[string]int, len(telegramBotAPI101UpdateRoots))
	for index, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			covered[fixture.root]++
			delivery, err := plan.Accept(telegramContractRequest(t, map[string]any{
				"update_id":  1000 + index,
				fixture.root: fixture.value,
			}))
			if err != nil {
				t.Fatalf("Accept %s fixture (%s): %v", fixture.root, telegramBotAPIUpdateContract, err)
			}
			if len(delivery.Events) != 1 || delivery.Events[0].Kind != OutputKindRaw || delivery.Events[0].Name != "inbound.telegram" || !delivery.Events[0].Authorization.Empty() {
				t.Fatalf("events = %#v, want exactly raw for %s under %s", delivery.Events, fixture.root, telegramBotAPIUpdateContract)
			}
		})
	}
	if len(telegramBotAPI101UpdateRoots) != 25 {
		t.Fatalf("recorded Update roots = %d, want 25 for %s", len(telegramBotAPI101UpdateRoots), telegramBotAPIUpdateContract)
	}
	for _, root := range telegramBotAPI101UpdateRoots {
		if covered[root] == 0 {
			t.Errorf("Update root %q has no disposition proof for %s", root, telegramBotAPIUpdateContract)
		}
	}
	if covered["callback_query"] != 4 {
		t.Errorf("callback_query variants covered = %d, want message/inline x data/game", covered["callback_query"])
	}
}

func TestTelegramInstalledAndEffectiveSubjectsCarryExactSelectedDescriptors(t *testing.T) {
	pack, catalog, plan := telegramPlatformContract(t)
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("InstalledCapabilitySubjects: %v", err)
	}
	if len(installed) != 1 {
		t.Fatalf("installed subjects = %#v, want Telegram only", installed)
	}
	effective, err := plan.EffectiveCapabilitySubject(EffectiveSubjectRequest{
		BundleHash: strings.Repeat("a", 64), Alias: "chat",
		SigningSecret: "webhook_signing.telegram", SourcePath: "package.yaml",
	})
	if err != nil {
		t.Fatalf("EffectiveCapabilitySubject: %v", err)
	}
	wantDescriptors := telegramSelectedTriggerDescriptors()
	for name, subject := range map[string]packs.Subject{"installed": installed[0], "effective": effective} {
		if !reflect.DeepEqual(subject.TriggerEvents, wantDescriptors) {
			t.Errorf("%s trigger descriptors = %#v, want %#v", name, subject.TriggerEvents, wantDescriptors)
		}
		if subject.Provider != "telegram" || subject.Provenance != packs.ProvenancePlatform {
			t.Errorf("%s subject provider/provenance = %q/%q", name, subject.Provider, subject.Provenance)
		}
	}
	if installed[0].ID != "provider.telegram" || installed[0].Source != "trigger_pack" || installed[0].Applicability != "installed" || installed[0].SourcePath != pack.SourcePath {
		t.Fatalf("installed subject identity = %#v", installed[0])
	}
	identity := effective.TriggerAdmission
	if effective.Source != "trigger_pack_binding" || effective.Applicability != "effective" || identity == nil || identity.Pack == nil ||
		identity.Pack.ID != pack.Envelope.ID || identity.Pack.Version != pack.Envelope.Version ||
		identity.Pack.ManifestHash != pack.Envelope.ManifestHash || identity.Pack.Provenance != packs.ProvenancePlatform {
		t.Fatalf("effective subject pack provenance = %#v", effective)
	}
}

func telegramPlatformContract(t *testing.T) (LoadedPack, *CatalogSnapshot, InboundAdmissionPlan) {
	t.Helper()
	dir := filepath.Join(testPlatformPackRoot(), "telegram")
	loaded, err := LoadPlatformPackDirs("0.7.0", dir)
	if err != nil {
		t.Fatalf("LoadPlatformPackDirs Telegram: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded Telegram packs = %d, want 1", len(loaded))
	}
	catalog, err := NewCatalogSnapshot(CatalogEntriesFromLoadedPacks(loaded...)...)
	if err != nil {
		t.Fatalf("NewCatalogSnapshot Telegram: %v", err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram",
	})
	if err != nil {
		t.Fatalf("CompileAdmission Telegram: %v", err)
	}
	return loaded[0], catalog, plan
}

func telegramContractRequest(t *testing.T, payload map[string]any) Request {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal Telegram fixture: %v", err)
	}
	var decoded map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode Telegram fixture: %v", err)
	}
	return telegramRequest("telegram-secret", body, decoded)
}

func telegramCallbackFixture(variant map[string]any) map[string]any {
	out := map[string]any{
		"id": "callback", "from": map[string]any{"id": 1, "is_bot": false, "first_name": "User"},
		"chat_instance": "chat-instance",
	}
	for key, value := range variant {
		out[key] = value
	}
	return out
}

func telegramCallbackMessage() map[string]any {
	return map[string]any{"message_id": 1, "chat": map[string]any{"id": 42}}
}

func telegramSelectedTriggerDescriptors() []packs.TriggerEventDescriptor {
	return []packs.TriggerEventDescriptor{
		{
			Event: "inbound.telegram", Kind: "raw",
			Fields: []packs.TriggerEventFieldDescriptor{
				{Name: "entity_id", Type: "text", Required: true},
				{Name: "event_type", Type: "text", Required: true},
				{Name: "headers", Type: "json", Required: true},
				{Name: "payload", Type: "json", Required: true},
				{Name: "provider", Type: "text", Required: true},
				{Name: "provider_delivery_id", Type: "text", Required: true},
				{Name: "provider_event_id", Type: "text", Required: true},
				{Name: "provider_event_type", Type: "text", Required: true},
				{Name: "received_at", Type: "text", Required: true},
			},
		},
		{
			Event: "inbound.telegram.callback_action", Kind: "normalized",
			Fields: []packs.TriggerEventFieldDescriptor{
				{Name: "conversation_reference", Type: "text", Required: true, CarryEligible: true},
				{Name: "external_account_reference", Type: "text", Required: true, CarryEligible: true},
				{Name: "interaction_reference", Type: "text", Required: true, CarryEligible: true},
				{Name: "provider_message_reference", Type: "integer", Required: true, CarryEligible: true},
				{Name: "token", Type: "text", Required: true, CarryEligible: true},
			},
		},
		{
			Event: "inbound.telegram.text_message", Kind: "normalized",
			Fields: []packs.TriggerEventFieldDescriptor{
				{Name: "conversation_reference", Type: "text", Required: true, CarryEligible: true},
				{Name: "external_account_reference", Type: "text", Required: true, CarryEligible: true},
				{Name: "provider_message_reference", Type: "integer", Required: true, CarryEligible: true},
				{Name: "text", Type: "text", Required: true, CarryEligible: true},
			},
		},
	}
}
