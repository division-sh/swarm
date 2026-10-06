package packs_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"gopkg.in/yaml.v3"
)

func TestChannelManifestAdmissionPresenceMatrix(t *testing.T) {
	body, err := os.ReadFile("../../packs/channels/telegram/channel.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		path               []string
		optional, emptyMap bool
	}{
		{[]string{"provider"}, false, false},
		{[]string{"transport"}, false, false},
		{[]string{"capabilities"}, false, false},
		{[]string{"opaque_types"}, false, false},
		{[]string{"operations"}, false, false},
		{[]string{"events"}, false, false},
		{[]string{"operations", "deliver"}, true, false},
		{[]string{"operations", "deliver", "tool"}, false, false},
		{[]string{"operations", "deliver", "input"}, true, true},
		{[]string{"operations", "deliver", "output"}, true, true},
		{[]string{"operations", "deliver", "input", "chat_id"}, true, false},
		{[]string{"operations", "deliver", "input", "reply_markup.inline_keyboard"}, true, false},
		{[]string{"operations", "deliver", "input", "reply_markup.inline_keyboard", "each"}, false, false},
		{[]string{"operations", "deliver", "input", "reply_markup.inline_keyboard", "item"}, false, false},
		{[]string{"events", "action"}, true, false},
		{[]string{"events", "action", "event"}, false, false},
		{[]string{"events", "action", "fields"}, false, true},
		{[]string{"events", "action", "fields", "token"}, true, false},
		{[]string{"registration"}, true, false},
		{[]string{"registration", "slot"}, false, false},
		{[]string{"registration", "slot", "namespace"}, false, false},
		{[]string{"registration", "slot", "identify"}, false, false},
		{[]string{"registration", "slot", "identify", "tool"}, false, false},
		{[]string{"registration", "slot", "identify", "output"}, true, true},
		{[]string{"registration", "credentials"}, false, false},
		{[]string{"registration", "credentials", "provider"}, false, false},
		{[]string{"registration", "credentials", "signing"}, false, false},
		{[]string{"registration", "apply"}, false, false},
		{[]string{"registration", "apply", "tool"}, false, false},
		{[]string{"registration", "apply", "input"}, true, true},
		{[]string{"registration", "readback"}, false, false},
		{[]string{"registration", "readback", "tool"}, false, false},
		{[]string{"registration", "readback", "output"}, true, true},
		{[]string{"onboarding"}, true, false},
		{[]string{"onboarding", "ceremony"}, false, false},
		{[]string{"onboarding", "provider_credential"}, false, false},
		{[]string{"onboarding", "confirmation"}, false, false},
		{[]string{"onboarding", "signing_credential"}, false, false},
		{[]string{"onboarding", "learned_destination"}, false, false},
		{[]string{"onboarding", "learned_destination", "destination"}, false, false},
	}
	for _, row := range rows {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
			t.Run(strings.Join(row.path, "/")+"/"+state, func(t *testing.T) {
				modified := channelPresenceBody(t, body, row.path, state)
				_, err := packs.ParseChannelManifest(modified)
				want := state == "valid" || state == "merge" || (state == "missing" && row.optional) || (state == "empty_map" && row.emptyMap)
				if (err == nil) != want {
					t.Fatalf("want admission=%v: %v", want, err)
				}
			})
		}
	}
	for _, row := range []struct {
		path                []string
		optional, emptyList bool
	}{
		{[]string{"native_inbox"}, true, false},
		{[]string{"native_inbox", "kind"}, false, false},
		{[]string{"native_inbox", "client_languages"}, false, true},
		{[]string{"native_inbox", "direct_launcher_read"}, false, false},
		{[]string{"native_inbox", "default_launcher_read"}, false, false},
		{[]string{"native_inbox", "commands_launcher"}, false, false},
		{[]string{"native_inbox", "inherited_launcher"}, false, false},
	} {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
			t.Run(strings.Join(row.path, "/")+"/"+state, func(t *testing.T) {
				modified := channelPresenceBody(t, body, row.path, state)
				_, err := packs.ParseChannelManifest(modified)
				want := state == "valid" || state == "merge" || (state == "missing" && row.optional) || (state == "empty_list" && row.emptyList)
				if (err == nil) != want {
					t.Fatalf("want native profile admission=%v: %v", want, err)
				}
			})
		}
	}
}

func TestChannelMappingAdmissionPresenceAndRetirement(t *testing.T) {
	base := "provider: probe\ntransport: webhook\n" + channelCapabilityFixture + "opaque_types: {reference: {type: string}}\noperations: {deliver: {tool: probe.deliver, input: {value: %s}}}\nevents: {observed: {event: probe.observed, fields: {value: event.value}}}\n"
	for _, mapping := range []string{"7", "true", "null", "''", "{from: payload.value}", "{from: '', each: input.rows, item: [{value: item.value}]}", "{from: null, each: input.rows, item: [{value: item.value}]}", "{each: '', item: [{value: item.value}]}", "{each: input.rows, item: []}", "{each: input.rows, item: [{value: {each: item.rows, item: [{value: item.value}]}}]}"} {
		_, err := packs.ParseChannelManifest([]byte(fmt.Sprintf(base, mapping)))
		if err == nil {
			t.Fatalf("invalid mapping admitted: %s", mapping)
		}
	}
	for _, mapping := range []string{"payload.value", "{each: input.rows, item: [{value: item.value}]}"} {
		if _, err := packs.ParseChannelManifest([]byte(fmt.Sprintf(base, mapping))); err != nil {
			t.Fatal(err)
		}
	}
	for _, inactive := range []string{"null", "''", "[]", "{}", "false", "health"} {
		body := fmt.Sprintf(base, "payload.value") + "onboarding: {ceremony: authenticated_text_challenge, provider_credential: key, signing_credential: signing, confirmation: deliver, connection_health: " + inactive + "}\n"
		if _, err := packs.ParseChannelManifest([]byte(body)); err == nil {
			t.Fatalf("inactive connection_health admitted: %s", inactive)
		}
	}
}

func channelPresenceBody(t testing.TB, body []byte, path []string, state string) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	parent := doc.Content[0]
	for _, name := range path[:len(path)-1] {
		var next *yaml.Node
		for i := 0; i < len(parent.Content); i += 2 {
			if parent.Content[i].Value == name {
				next = parent.Content[i+1]
				break
			}
		}
		if next == nil {
			t.Fatalf("missing fixture path %v", path)
		}
		parent = next
	}
	name := path[len(path)-1]
	index := -1
	for i := 0; i < len(parent.Content); i += 2 {
		if parent.Content[i].Value == name {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatalf("missing fixture field %v", path)
	}
	valid := parent.Content[index+1]
	switch state {
	case "missing":
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
	case "merge":
		key := parent.Content[index]
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
		parent.Content = append(parent.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!merge", Value: "<<"}, &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Anchor: "base", Content: []*yaml.Node{key, valid}})
	case "valid":
	default:
		literal := map[string]string{"null": "null", "empty_text": "''", "empty_list": "[]", "empty_map": "{}", "wrong_kind": "true"}[state]
		var replacement yaml.Node
		if err := yaml.Unmarshal([]byte(literal), &replacement); err != nil {
			t.Fatal(err)
		}
		parent.Content[index+1] = replacement.Content[0]
	}
	result, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
