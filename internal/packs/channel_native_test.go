package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestChannelManifestRetainsNativeInboxProfileThroughStrictAdmission(t *testing.T) {
	body := []byte("transport: webhook\n" + channelCapabilityFixture + `provider: probe
opaque_types: {reference: {type: string}}
operations: {deliver: {tool: probe.deliver}}
events: {received: {event: probe.received, fields: {text: event.text}}}
native_inbox:
  kind: scoped_commands_v1
  client_languages: [en, fr]
  direct_launcher_read: read_chat_menu_button
  default_launcher_read: read_default_menu_button
  commands_launcher: commands
  inherited_launcher: default
`)
	manifest, err := packs.ParseChannelManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	profile := manifest.NativeInbox
	if profile == nil || profile.Kind != "scoped_commands_v1" || strings.Join(profile.ClientLanguages, ",") != "en,fr" ||
		profile.DirectLauncherRead != "read_chat_menu_button" || profile.DefaultLauncherRead != "read_default_menu_button" ||
		profile.CommandsLauncher != "commands" || profile.InheritedLauncher != "default" {
		t.Fatalf("strict manifest admission lost the native profile: %+v", profile)
	}
	for _, unknown := range []string{"unexpected: true\n", "  unexpected: true\n"} {
		if _, err := packs.ParseChannelManifest(append(append([]byte(nil), body...), []byte(unknown)...)); err == nil {
			t.Fatalf("strict manifest admission accepted an unknown field: %q", unknown)
		}
	}
}
