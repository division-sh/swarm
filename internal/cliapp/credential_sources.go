package cliapp

import (
	"sort"
	"strconv"
	"strings"
	"unicode"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

// CredentialSourceLines projects only already-observed, value-free sources.
func CredentialSourceLines(facts []runtimecredentials.SourceFact) []string {
	seen := make(map[runtimecredentials.SourceFact]bool, len(facts))
	var lines []string
	for _, fact := range facts {
		if fact.Key == "" || fact.Reference == "" || seen[fact] {
			continue
		}
		seen[fact] = true
		lines = append(lines, "credential "+credentialSourceText(fact.Key)+" · source "+credentialSourceText(fact.Reference))
	}
	sort.Strings(lines)
	return lines
}

func credentialSourceText(text string) string {
	if strings.ContainsFunc(text, func(r rune) bool { return unicode.IsControl(r) || r == '\u2028' || r == '\u2029' }) {
		return strconv.QuoteToASCII(text)
	}
	return text
}
