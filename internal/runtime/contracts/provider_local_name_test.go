package contracts

import (
	"strings"
	"testing"
)

func TestS1ProviderLocalNamesShareExactToolAndCredentialSyntax(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot", "9-team", strings.Repeat("a", 100)} {
		for _, local := range []string{"send_message", "bot-token", "7token"} {
			qualified, err := QualifyProviderLocalName(key, local)
			if err != nil || qualified != key+"."+local {
				t.Fatalf("local reference %q/%q=%q: %v", key, local, qualified, err)
			}
		}
	}
	for _, key := range []string{"_team", "con", " team", "team ", "TEAM", strings.Repeat("a", 101)} {
		if _, err := QualifyProviderLocalName(key, "send_message"); err == nil {
			t.Errorf("invalid provider key %q admitted", key)
		}
	}
	for _, local := range []string{"", "team-bot.send_message", "foreign.send_message", "child/send_message", " send_message", "send_message ", "bot token", "bot\n_token", "*"} {
		if _, err := QualifyProviderLocalName("team-bot", local); err == nil {
			t.Errorf("non-local or inexact reference %q admitted", local)
		}
	}
}
