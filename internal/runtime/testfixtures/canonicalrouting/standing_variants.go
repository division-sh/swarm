package canonicalrouting

import (
	"strings"
	"testing"
)

// WithoutStandingIngressPins derives the non-standing sibling from the
// canonical standing fixture without creating a second routing authority.
func WithoutStandingIngressPins(t testing.TB, schema string) string {
	t.Helper()
	standingPins := `pins:
  inputs:
    events:
      - {name: telegram_update, event: inbound.telegram, source: external}
  outputs: {events: []}
`
	nonStandingPins := `pins:
  inputs:
    events: []
  outputs: {events: []}
`
	result := strings.Replace(schema, standingPins, nonStandingPins, 1)
	if result == schema {
		t.Fatal("standing ingress event pins not found")
	}
	return result
}
