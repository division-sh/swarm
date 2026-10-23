package eventidentity

import (
	"strings"
	"testing"
)

func TestS1ProviderEventProjectionRetainsExactLocalTokens(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot", "9-team", strings.Repeat("a", 100)} {
		for _, local := range []string{"raw", "text.received", "message_v2", "7"} {
			name, err := QualifyProviderEvent(key, local)
			if err != nil || name != "inbound."+key+"."+local || !IsCanonicalName(name) {
				t.Fatalf("exact projection %q/%q=%q: %v", key, local, name, err)
			}
		}
	}
	for _, key := range []string{"_team", "con", " team", "team ", "TEAM", strings.Repeat("a", 101)} {
		if _, err := QualifyProviderEvent(key, "raw"); err == nil {
			t.Errorf("invalid provider key %q repaired", key)
		}
	}
	for _, local := range []string{"", "inbound.team.raw", "inbound.foreign.raw", "child/raw", "text.*", "raw-hyphen", "Text", "text. received", "text.\u00a0received", "text.\u2003received", "text.\treceived", "text..received", "raw "} {
		if _, err := QualifyProviderEvent("team-bot", local); err == nil {
			t.Errorf("non-local or inexact event %q admitted", local)
		}
	}
}
