package serveapp

import (
	"net/http"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
)

// Keep the accepted-publication/immediate-shutdown path from the A9 control;
// neither delivery quiescence nor a sleep may stand in for the shutdown join.
func TestAcceptedAliasPublicationJoinedShutdownBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, map[string]servedparity.Backend{"sqlite": servedparity.BackendDefaultSQLite, "postgres": servedparity.BackendExplicitPostgres}[backend], true)
			writeChannelOnboardingLiveToolCredentials(t, h.credentialPath)
			h.opts.AbandonActiveRuns = false
			registerChannelFixtureAccount(t, h.opts.ConfigPath, "joined-shutdown-bot-token", "420079")
			h.start(t)
			requireServedLandingRun(t, servedTestProcessRuntime(t, h.process), ".")
			callback, secret, _ := h.provider.Registration()
			status, receipt := postProviderAliasUpdate(t, callback, "", secret, `{"update_id":9801,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
			if status != http.StatusAccepted {
				t.Fatalf("authenticated publication: status=%d receipt=%s", status, receipt)
			}
			h.stop(t)
		})
	}
}
