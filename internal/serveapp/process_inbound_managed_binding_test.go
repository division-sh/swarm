package serveapp

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
)

func TestInboundPublicationRetainsDistinctManagedSigningKeyBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := prepareChannelAnchorPublicJourney(t, backend, "bot-token", false, 0, nil)
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			targets, err := h.runtime.PlanStandingTargets()
			if err != nil || len(targets) != 1 {
				t.Fatalf("exact declared ingress targets=%d err=%v", len(targets), err)
			}
			signingRole := loadSupportedTelegramRegistration(t).SigningCredential()
			managedKey := ""
			for _, admission := range h.registration.CredentialAdmissions {
				if admission.Role == signingRole {
					managedKey = admission.StoreKey
				}
			}
			if managedKey == "" || targets[0].SigningSecret == "" || managedKey == targets[0].SigningSecret {
				t.Fatal("fixture did not establish distinct declared and managed signing keys")
			}
			callback, signing, _ := h.provider.Registration()
			address, err := url.Parse(callback)
			if err != nil {
				t.Fatal(err)
			}
			address.RawQuery = ""
			absent := address.String()
			query := address.Query()
			query.Set("swarm_callback_generation", "foreign")
			address.RawQuery = query.Encode()
			body := `{"update_id":88901,"message":{"message_id":88901,"from":{"id":9001},"chat":{"id":9001,"type":"private"},"text":"ordinary distinct-key input"}}`
			for _, rejected := range []struct {
				name, callback string
			}{
				{"absent", absent},
				{"foreign", address.String()},
			} {
				t.Run(rejected.name, func(t *testing.T) {
					status, _ := postProviderAliasUpdate(t, rejected.callback, "", signing, body)
					if status != http.StatusNotFound {
						t.Fatalf("invalid managed binding reached publication: status=%d", status)
					}
				})
			}
			status, bodyBytes := postProviderAliasUpdate(t, callback, "", signing, body)
			var publication standingTelegramPublication
			if status != http.StatusAccepted || json.Unmarshal(bodyBytes, &publication) != nil || publication.RunID == "" || len(publication.EventIDs) != 2 {
				t.Fatalf("distinct managed key publication: status=%d body=%s", status, bodyBytes)
			}
		})
	}
}
