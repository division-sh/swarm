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

func TestS1EveryConcreteProviderKeyUsesPortableDirectoryOwner(t *testing.T) {
	type boundaryCase struct {
		label, key string
		valid      bool
	}
	cases := []boundaryCase{
		{"plain", "team", true},
		{"underscore", "team_bot", true},
		{"hyphen", "team-bot", true},
		{"plain_100", strings.Repeat("a", 100), true},
		{"plain_101", strings.Repeat("a", 101), false},
		{"hyphen_100", strings.Repeat("a", 98) + "-b", true},
		{"hyphen_101", strings.Repeat("a", 99) + "-b", false},
		{"leading_underscore", "_team", false},
		{"leading_space", " team", false},
		{"trailing_space", "team ", false},
		{"leading_tab", "\tteam", false},
		{"trailing_tab", "team\t", false},
		{"newline", "team\n", false},
		{"empty", "", false},
	}
	for _, reserved := range []string{"con", "prn", "aux", "nul", "com1", "com9", "lpt1", "lpt9"} {
		cases = append(cases, boundaryCase{"reserved_" + reserved, reserved, false})
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			if got := runtimepaths.ValidateFlowSegment(tc.key) == nil; got != tc.valid {
				t.Fatalf("directory-owner control=%v want %v for %q", got, tc.valid, tc.key)
			}
			for _, prefix := range []string{"", "child/", "child/deep/"} {
				name := prefix + "inbound." + tc.key + ".message"
				if IsValidName(name) != tc.valid || IsCanonicalName(name) != tc.valid || IsCanonicalDeclaration(name) != tc.valid {
					t.Errorf("concrete reference disagrees with directory owner: %q want valid=%v", name, tc.valid)
				}
				pattern := prefix + "inbound." + tc.key + ".*"
				if IsCanonicalPattern(pattern) != tc.valid || IsCanonicalDeclaration(pattern) != tc.valid {
					t.Errorf("fixed-key pattern disagrees with directory owner: %q want valid=%v", pattern, tc.valid)
				}
			}
			pattern := "**/inbound." + tc.key + ".*"
			if IsCanonicalPattern(pattern) != tc.valid {
				t.Errorf("recursive fixed-key pattern disagrees with directory owner: %q", pattern)
			}
		})
	}
	for _, pattern := range []string{"inbound.*.message", "inbound.*.*", "**/inbound.*.message"} {
		if !IsCanonicalPattern(pattern) {
			t.Errorf("whole-token provider wildcard refused: %q", pattern)
		}
	}
	if IsCanonicalPattern("inbound.**.message") || IsCanonicalName("inbound.*.message") {
		t.Fatal("provider wildcard became concrete or partial globstar became valid")
	}
}
