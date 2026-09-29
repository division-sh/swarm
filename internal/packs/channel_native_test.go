package packs_test

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestNativeInboxQualificationUsesEffectiveLocaleAndLauncher(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	registry, _, _, _ := loadTelegramChannelCompilerInputs(t)
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := plan.NativeInboxProfile()
	if err != nil {
		t.Fatal(err)
	}
	desired := json.RawMessage(`[{"command":"inbox_exact","description":"Open inbox"}]`)
	foreign := json.RawMessage(`[{"command":"foreign","description":"Foreign entry"}]`)
	for _, test := range []struct {
		name, language, chat, fallback string
		selected, commands             json.RawMessage
		shared, valid                  bool
	}{
		{"English selected", "en", "commands", "web_app", desired, foreign, false, true},
		{"French selected", "fr", "commands", "web_app", desired, foreign, false, true},
		{"French fallback", "fr", "default", "commands", json.RawMessage(`[]`), desired, false, true},
		{"provider launcher fallback", "fr", "default", "default", desired, foreign, false, true},
		{"higher precedence commands conflict", "fr", "commands", "commands", foreign, desired, false, false},
		{"chat launcher conflict", "en", "web_app", "commands", desired, desired, false, false},
		{"default launcher conflict", "en", "default", "web_app", desired, desired, false, false},
		{"shared launcher irrelevant", "fr", "web_app", "web_app", desired, foreign, true, true},
		{"missing declaration", "", "commands", "commands", desired, desired, false, false},
		{"unsupported declaration", "de", "commands", "commands", desired, desired, false, false},
		{"no locale inference", "FR", "commands", "commands", desired, desired, false, false},
		{"wrong label", "fr", "commands", "commands", json.RawMessage(`[{"command":"inbox_exact","description":"Boite de reception"}]`), desired, false, false},
		{"duplicate entry", "en", "commands", "commands", json.RawMessage(`[{"command":"inbox_exact","description":"Open inbox"},{"command":"inbox_exact","description":"Open inbox"}]`), desired, false, false},
		{"null readback", "fr", "commands", "commands", json.RawMessage(`null`), desired, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := profile.Qualify(test.language, packs.NativeInboxReadback{SelectedCommands: test.selected,
				FallbackCommands: test.commands, ChatLauncher: test.chat, DefaultLauncher: test.fallback, Shared: test.shared}, desired)
			if (err == nil) != test.valid {
				t.Fatalf("qualification valid=%v, want %v: %v", err == nil, test.valid, err)
			}
		})
	}
}
