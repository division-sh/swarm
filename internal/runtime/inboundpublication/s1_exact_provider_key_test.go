package inboundpublication

import (
	"strings"
	"testing"
)

func TestS1ReceiptProviderKeyIsExactBeforeAndAfterNormalization(t *testing.T) {
	base := evidenceProofRequest(t)
	ids := map[string]bool{}
	for _, key := range []string{"telegram-sales", "telegram_sales", strings.Repeat("a", 100)} {
		request := base
		request.Provider = key
		var err error
		request.PublicationID, request.MarkerEventID, err = DeterministicIDs(request.Identity())
		if err != nil {
			t.Fatal(err)
		}
		if ids[request.PublicationID] {
			t.Fatal("distinct keys share receipt identity")
		}
		ids[request.PublicationID] = true
		if err := request.Normalized().Validate(); err != nil || request.Normalized().Provider != key {
			t.Fatalf("valid exact key %q refused or changed: %v", key, err)
		}
	}
	for _, key := range []string{"", "_team", "con", "lpt9", "github ", " github", "GitHub", "github.bot", strings.Repeat("a", 101)} {
		request := base
		request.Provider = key
		if normalized := request.Normalized(); normalized.Provider != key {
			t.Errorf("normalization repaired provider %q into %q", key, normalized.Provider)
		}
		if err := request.Identity().Validate(); err == nil {
			t.Errorf("receipt identity admitted nonportable provider %q", key)
		}
		if err := request.Normalized().Validate(); err == nil {
			t.Errorf("normalized request admitted provider %q", key)
		}
	}
}
