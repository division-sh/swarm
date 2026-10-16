package eventidentity

import (
	"strings"
	"testing"

	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
)

func TestS1ProviderDirectoryKeyPreservesEventIdentity(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", "1-store", "team-bot-2"} {
		t.Run(key, func(t *testing.T) {
			if err := runtimepaths.ValidateFlowSegment(key); err != nil {
				t.Fatalf("control is not a valid provider directory: %v", err)
			}
			for _, local := range []string{"message", "raw.pull_request"} {
				name := "inbound." + key + "." + local
				for _, reference := range []string{name, "child/" + name} {
					if !IsValidName(reference) || !IsCanonicalName(reference) || !IsCanonicalDeclaration(reference) {
						t.Errorf("valid directory key cannot be referenced exactly: %q", reference)
					}
					if Normalize(reference) != reference || LeafName(reference) != name {
						t.Errorf("provider directory key was normalized: %q", reference)
					}
				}
				for _, pattern := range []string{"inbound." + key + ".*", "**/inbound." + key + ".*"} {
					if !IsCanonicalPattern(pattern) || !MatchPattern(pattern, name) {
						t.Errorf("fixed provider-key pattern rejected or mismatched: %q -> %q", pattern, name)
					}
				}
			}
			if MatchPattern("inbound."+key+".*", "inbound."+strings.ReplaceAll(key, "-", "_")+".message") && strings.Contains(key, "-") {
				t.Fatal("hyphen and underscore provider keys were merged")
			}
		})
	}
}

func TestS1ProviderKeyDoesNotBroadenOrdinaryEventTokens(t *testing.T) {
	for _, name := range []string{
		"business.team-bot.message", "inbound.telegram.text-message", "inbound.team-bot.text-message",
		"inbound.-team.message", "inbound.TEAM-BOT.message", "inbound.team-bot .message",
		"inbound. team-bot.message", "inbound.provider.team-bot.message", "inbound.team-bot",
		"inbound." + strings.Repeat("a", 99) + "-b.message",
	} {
		if IsCanonicalName(name) || IsCanonicalDeclaration(name) {
			t.Errorf("invalid provider or ordinary event token accepted: %q", name)
		}
	}
	for _, pattern := range []string{"business.team-bot.*", "inbound.team-bot.text-*", "*.team-bot.message", "inbound.-team.*", "inbound.TEAM-BOT.*"} {
		if IsCanonicalPattern(pattern) || IsCanonicalDeclaration(pattern) {
			t.Errorf("provider exception leaked to ordinary wildcard tokens: %q", pattern)
		}
	}
}
