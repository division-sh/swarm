package packs_test

import (
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"gopkg.in/yaml.v3"
)

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
