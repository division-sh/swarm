package packmodel

import (
	"fmt"
	"strings"
	"testing"
)

const s1ManifestHeader = "version: 1.2.3\nplatform_version: '*'\n"
const s1ManifestCapabilities = "capabilities:\n  can:\n    emit_events: [inbound.team.message]\n    call_provider_actions: [team.send]\n    persist_dedupe_markers: true\n    lower_through_activity: true\n    journal_activity_attempts: true\n  cannot: [execute arbitrary code]\n"

func TestS1SharedManifestRefusesAuthoredProviderIdentity(t *testing.T) {
	for _, name := range []string{"team-bot", "team_bot", "github", "microsoft_graph", "1-provider", strings.Repeat("a", 100), "provider.telegram", "Team", "Telegram Bot", " team-bot", "team-bot ", ".", "../team", "aux", "con", strings.Repeat("a", 101)} {
		t.Run(name, func(t *testing.T) {
			body := fmt.Sprintf("name: %q\n", name) + s1ManifestHeader + s1ManifestCapabilities
			_, err := ParseManifestAt([]byte(body), "provider/manifest.yaml", DirectoryProvider)
			if err == nil || !strings.Contains(err.Error(), "the directory is the name") || !strings.Contains(err.Error(), "provider/manifest.yaml:1:") {
				t.Fatalf("authored name %q admitted or lost location: %v", name, err)
			}
		})
	}
}

func TestS1SharedManifestClosedFieldsAndTypedPresence(t *testing.T) {
	for _, kind := range []DirectoryKind{DirectoryFlow, DirectoryProvider} {
		base := s1ManifestHeader
		if kind == DirectoryProvider {
			base += s1ManifestCapabilities
		}
		for _, field := range []string{"name", "type", "id", "implements", "manifest_hash", "provenance", "tests", "requires", "requires.secrets", "requires.managed_credentials", "unknown"} {
			for _, value := range []string{"null", "[]", "{}", "false", "retired"} {
				t.Run(fmt.Sprintf("%s/%s/%s", kind, field, value), func(t *testing.T) {
					_, err := ParseManifestAt([]byte(base+field+": "+value+"\n"), "manifest.yaml", kind)
					if err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "manifest.yaml:") {
						t.Fatalf("retired field gained authority: %v", err)
					}
				})
			}
		}
		for _, field := range []string{"version", "platform_version"} {
			for _, value := range []string{"null", "[]", "{}", "false", "''"} {
				t.Run(fmt.Sprintf("%s/%s/%s", kind, field, value), func(t *testing.T) {
					lines := strings.Split(base, "\n")
					for i, line := range lines {
						if strings.HasPrefix(line, field+":") {
							lines[i] = field + ": " + value
						}
					}
					_, err := ParseManifestAt([]byte(strings.Join(lines, "\n")), "manifest.yaml", kind)
					if err == nil || !strings.Contains(err.Error(), field) {
						t.Fatalf("wrong-kind manifest field admitted: %v", err)
					}
				})
			}
		}
	}
	if _, err := ParseManifestAt([]byte(s1ManifestHeader+s1ManifestCapabilities), "manifest.yaml", DirectoryFlow); err == nil {
		t.Fatal("flow acquired provider capabilities")
	}
	if _, err := ParseManifestAt([]byte(s1ManifestHeader), "manifest.yaml", DirectoryProvider); err == nil {
		t.Fatal("provider omitted its capability declaration")
	}
	if _, err := ParseManifestAt([]byte(s1ManifestHeader), "manifest.yaml", DirectoryKind(0)); err == nil {
		t.Fatal("caller omitted the admitted directory kind")
	}
}

func TestS1SharedManifestRequirementAliasEvidence(t *testing.T) {
	for _, field := range []string{"secrets", "managed_credentials"} {
		for _, declaration := range []string{
			"requires:\n  " + field + ": [token]\n",
			"<<: {requires: {" + field + ": [token]}}\n",
		} {
			t.Run(field+declaration, func(t *testing.T) {
				_, err := ParseManifestAt([]byte(s1ManifestHeader+s1ManifestCapabilities+declaration), "provider/manifest.yaml", DirectoryProvider)
				if err == nil || !strings.Contains(err.Error(), "requires."+field+" is not a manifest key") || !strings.Contains(err.Error(), "provider/manifest.yaml:") || !strings.Contains(err.Error(), "credentials are declared on the action, trigger or channel that uses them") {
					t.Fatalf("retired summary lost exact teaching: %v", err)
				}
			})
		}
	}
	body := "capabilities: &field {can: {emit_events: [raw]}, cannot: [execute code]}\nversion: 1.2.3\nplatform_version: *field\n"
	_, err := ParseManifestAt([]byte(body), "provider/manifest.yaml", DirectoryProvider)
	if err == nil || !strings.Contains(err.Error(), "provider/manifest.yaml:3:") || !strings.Contains(err.Error(), "resolved at provider/manifest.yaml:1:") {
		t.Fatalf("alias introduction/resolution lost: %v", err)
	}
}

func TestS1SharedManifestAllRoleCapabilitiesAreDefensive(t *testing.T) {
	manifest, err := ParseManifestAt([]byte(s1ManifestHeader+s1ManifestCapabilities), "provider/manifest.yaml", DirectoryProvider)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := manifest.Capabilities()
	if !ok || len(first.Can.EmitEvents) != 1 || len(first.Can.CallProviderActions) != 1 || !first.Can.PersistDedupeMarkers || !first.Can.JournalActivityAttempts {
		t.Fatalf("role capability lost: %+v", first)
	}
	first.Can.EmitEvents[0] = "changed"
	first.Can.CallProviderActions[0] = "changed"
	first.Cannot[0] = "changed"
	second, _ := manifest.Capabilities()
	if second.Can.EmitEvents[0] != "inbound.team.message" || second.Can.CallProviderActions[0] != "team.send" || second.Cannot[0] != "execute arbitrary code" {
		t.Fatalf("mutable caller changed admitted capabilities: %+v", second)
	}
	for _, replacement := range []string{"emit_events: []", "emit_events: null", "emit_events: false"} {
		_, err := ParseManifestAt([]byte(s1ManifestHeader+strings.Replace(s1ManifestCapabilities, "emit_events: [inbound.team.message]", replacement, 1)), "manifest.yaml", DirectoryProvider)
		if err == nil {
			t.Fatalf("invalid capability admitted: %s", replacement)
		}
	}
}
