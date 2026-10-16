package packs

import (
	"strings"
	"testing"
)

func TestS1TriggerSubjectRetainsExactPortableProviderKey(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", "_team", "con", "Telegram", "telegram.bot", " acme", "acme ", strings.Repeat("a", 101)} {
		subject := Subject{ID: "test.trigger", Kind: SubjectProviderTrigger, Provider: key, Source: "trigger_pack", Applicability: "installed"}
		got, err := NormalizeSubjects([]Subject{subject})
		valid := key == "telegram-sales" || key == "telegram_sales"
		if !valid {
			if err == nil {
				t.Errorf("nonportable subject provider %q admitted: %+v", key, got)
			}
			if id, err := IngressSubjectID("bundle", ".", key); err == nil {
				t.Errorf("nonportable subject provider %q gained id %q", key, id)
			}
			continue
		}
		if err != nil || len(got) != 1 || got[0].Provider != key {
			t.Fatalf("portable subject key changed: %+v err=%v", got, err)
		}
		if id, err := IngressSubjectID("bundle", ".", key); err != nil || id != "ingress:bundle:.:"+key {
			t.Errorf("subject id lost exact key: %q err=%v", id, err)
		}
	}
}

func TestS1ConnectorSubjectRetainsExactPortableProviderKey(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot", "9-team", "_team", "con", "TEAM", "team.bot", " team", "team ", strings.Repeat("a", 101)} {
		subject := Subject{
			ID: key + ".send", Kind: SubjectProviderConnector, Provider: key, Source: "connector_pack", Applicability: "installed",
			Requirements: []Requirement{RequirementWithStatus(RequirementImport, key+".send", "package", "NOT_IMPORTED", "connector_pack_registry")},
		}
		got, err := NormalizeSubjects([]Subject{subject})
		valid := key == "team-bot" || key == "team_bot" || key == "9-team"
		if !valid {
			if err == nil {
				t.Errorf("invalid connector subject provider %q admitted: %+v", key, got)
			}
			continue
		}
		if err != nil || len(got) != 1 || got[0].Provider != key {
			t.Fatalf("connector subject key changed: %+v err=%v", got, err)
		}
	}
}

func TestS1ChannelSubjectsRetainExactPortableProviderKey(t *testing.T) {
	for _, base := range []Subject{
		{ID: "channel.pack", Kind: SubjectChannelPack, Source: "channel_pack", Applicability: "installed"},
		{ID: "channel.outbound", Kind: SubjectChannelOutbound, Source: "channel_binding", Applicability: "effective"},
	} {
		for _, key := range []string{"team-bot", "team_bot", "9-team", " team", "team ", "TEAM", "_team", "con", strings.Repeat("a", 101)} {
			base.Provider = key
			got, err := NormalizeSubjects([]Subject{base})
			valid := key == "team-bot" || key == "team_bot" || key == "9-team"
			if !valid {
				if err == nil {
					t.Errorf("channel subject %s repaired key %q: %+v", base.Kind, key, got)
				}
				continue
			}
			if err != nil || len(got) != 1 || got[0].Provider != key {
				t.Fatalf("channel subject %s changed exact key %q: %+v err=%v", base.Kind, key, got, err)
			}
		}
	}
}
